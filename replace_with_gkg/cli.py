#!/usr/bin/env python
# -*- coding: utf-8 -*-
"""CLI to replace value with suggestion from the Google Knowledge Graph."""


import csv
import json
import sys
import time
from pathlib import Path
from typing import List, Mapping, Sequence, Union

import click
import click_pathlib
import inflect
from loguru import logger
from prompt_toolkit.token import Token
from PyInquirer import prompt, style_from_dict
from yaspin import yaspin
from yaspin.spinners import Spinners

from replace_with_gkg import Replacer

PROMPT_STYLE = style_from_dict({
    Token.Question: 'bold',
    Token.Answer: '#ansidarkgreen',
})

CIRCUIT_BREAKER_LIMIT = 500

nlp = inflect.engine()


def _create_openrefine_file(
    openrefine_output_file: Path,
    csv_file: Path,
    replacements: Mapping[str, str],
    column: str,
):
    openrefine_file_path = openrefine_output_file
    if not openrefine_file_path:
        openrefine_file_path = Path(
            csv_file.parent,
            '{0}_openrefine.json'.format(csv_file.stem),
        )
        logger.info(
            'No OpenRefine file path provided, using {0}',
            openrefine_file_path,
        )

    with open(openrefine_file_path, 'w') as output_file:
        json.dump(
            [
                {
                    'op': 'core/mass-edit',
                    'engineConfig': {
                        'facets': [],
                        'mode': 'row-based',
                    },
                    'columnName': column,
                    'expression': 'value',
                    'edits': [
                        {
                            'from': [from_val],
                            'fromBlank': False,
                            'fromError': False,
                            'to': to_val,
                        },
                    ],
                    'description': 'Mass edit cells in column {0}'.format(
                        column,
                    ),
                }
                for from_val, to_val in replacements.items()
            ],
            output_file,
            indent=2,
        )


def _create_processed_values_output_file(
    processed_values: Sequence[str],
    processed_values_output_file: Path,
    csv_file: Path,
):
    processed_values_output_file_path = processed_values_output_file
    if not processed_values_output_file_path:
        processed_values_output_file_path = Path(
            csv_file.parent,
            '{0}_processed.json'.format(csv_file.stem),
        )
        logger.info(
            'No processed values file path provided, using {0}',
            processed_values_output_file_path,
        )
    with open(processed_values_output_file_path, 'w') as output_file:
        json.dump(processed_values, output_file, indent=2)


def _create_output_file(
    output_file_path: Path,
    csv_file: Path,
    headers: List[str],
    rows: List[Mapping[str, Union[str, int, float]]],
    replacements: Mapping[str, str],
    column: str,
):
    if not output_file_path:
        output_file_path = Path(
            csv_file.parent,
            '{0}_out{1}'.format(csv_file.stem, csv_file.suffix),
        )
    logger.info('Writing file with new values to {0}', output_file_path)

    with open(output_file_path, 'w', newline='') as output_csv_file:
        csv_writer = csv.DictWriter(output_csv_file, fieldnames=headers)
        csv_writer.writeheader()
        for row in rows:
            if row.get(column) in replacements:
                row[column] = replacements[row[column]]
            csv_writer.writerow(row)


def _read_unique_values_from_csv(csv_file: Path, column: str):
    logger.info(
        'Reading unique values of column "{0}" from CSV file and caching it',
        column,
    )
    unique_values = set()
    rows = []
    headers = []
    with yaspin(
        Spinners.bouncingBar,
        text='Processing CSV file',
        color='yellow',
    ):
        with open(csv_file) as csv_file_content:
            csv_reader = csv.DictReader(csv_file_content)
            headers = csv_reader.fieldnames
            for row in csv_reader:
                unique_values.add(row.get(column))
                rows.append(row)
    logger.info('Found {0} unique values', len(unique_values))
    return unique_values, rows, headers


def _read_ignore_values_file(ignore_values_file: Path):
    ignore_values = set()
    if ignore_values_file:
        with open(ignore_values_file) as ignore_values_file_content:
            ignore_values = set(json.load(ignore_values_file_content))
    return ignore_values


def _prompt_user(position, total, unique_value, suggestion):
    return prompt(
        [{
            'type': 'confirm',
            'message': '[{0}/{1}] Replace "{2}" with "{3}"?'.format(
                position,
                total,
                unique_value,
                suggestion,
            ),
            'name': 'should_replace',
            'default': False,
            'qmark': '',
        }], style=PROMPT_STYLE,
    )


def _process_suggestions(  # noqa: WPS231
    replacer, unique_values, ignore_values,
):
    counter = 0
    processed = []
    replacements = {}
    circuit_breaker = 0

    with yaspin(
        Spinners.bouncingBar,
        text='Checking values against Google Knowledge Graph...',
        color='yellow',
    ) as spinner:
        for position, unique_value in enumerate(sorted(unique_values)):
            if not unique_value:
                continue
            if unique_value in ignore_values:
                processed.append(unique_value)
                continue
            if circuit_breaker == CIRCUIT_BREAKER_LIMIT:
                # Google seems to rate limit req/s to ~1000 (undocumented).
                # Adding a 1min sleep when reaching 500 consecutive requests.
                logger.info('Hit circuit breaker; sleeping 1min...')
                time.sleep(60)
                logger.info('Resetting circuit breaker; continuing...')
                circuit_breaker = 0
            circuit_breaker += 1
            try:
                suggestion = replacer.suggest(unique_value)
            except Exception as error:
                logger.error(error)
                break
            suggestion_matches_input = nlp.compare_nouns(
                suggestion.lower(), unique_value.lower(),
            )
            if suggestion and not suggestion_matches_input:
                spinner.hide()
                counter += 1
                # Resetting the circuit_breaker since human interaction reduces
                # req/s.
                circuit_breaker = 0
                answers = _prompt_user(
                    position, len(unique_values), unique_value, suggestion,
                )
                if not answers:
                    # User hit ctrl-c to abort
                    break
                if answers.get('should_replace'):
                    replacements[unique_value] = suggestion
                spinner.show()
            processed.append(unique_value)
    logger.info('Offered {0} suggestions', counter)
    logger.info('Collected {0} value replacement pairs', len(replacements))
    return processed, replacements


@click.group()
@click.option(
    '-k',
    '--gkg-api-key',
    'gkg_api_key',
)
@click.pass_context
def cli(ctx, gkg_api_key):
    """Replace values with suggestions from Google Knowledge Graph."""
    ctx.ensure_object(dict)
    ctx.obj['GKG_API_KEY'] = gkg_api_key


@cli.command()
@click.argument('request')
@click.pass_context
def suggest(ctx, request: str):
    """Replace values with suggestions from GKG for single input."""
    replacer = Replacer(ctx.obj.get('GKG_API_KEY'))
    suggestion = replacer.suggest(request)
    if suggestion == request:
        logger.info(
            'Result from Google Knowledge Graph equals input: "{0}"', request,
        )
    elif suggestion:
        logger.info('Result from Google Knowledge Graph: "{0}"', suggestion)
    else:
        logger.info(
            'No results in the Google Knowledge Graph for: "{0}"', request,
        )


@cli.command()
@click.argument('column')
@click.argument(
    'csv_file',
    type=click_pathlib.Path(exists=True),
)
@click.option(
    '-i',
    '--in-place',
    'in_place',
    is_flag=True,
    default=False,
    help='Replace CSV file values in-place (default: false)',
)
@click.option(
    '-o',
    '--output-file',
    'output_file',
    type=click_pathlib.Path(),
    help='Output CSV file path {0}'.format(
        '(ignore when using --in-place; default stem(<CSV_FILE>)_out.csv)',
    ),
)
@click.option(
    '-s',
    '--save-openrefine',
    'save_openrefine',
    is_flag=True,
    default=False,
    help='Save replacements as OpenRefine Operation History file {0}'.format(
        '(default: false)',
    ),
)
@click.option(
    '-f',
    '--openrefine-output-file',
    'openrefine_output_file',
    type=click_pathlib.Path(),
    help='OpenRefine Operation History file path {0}'.format(
        '(default stem(<CSV_FILE>)_openrefine.json)',
    ),
)
@click.option(
    '-c',
    '--save-processed-values',
    'save_processed_values',
    is_flag=True,
    default=False,
    help='Store processed values in file (default: false)',
)
@click.option(
    '-r',
    '--processed-values-output-file',
    'processed_values_output_file',
    type=click_pathlib.Path(),
    help='Processed values file path {0}'.format(
        '(default stem(<CSV_FILE>)_processed.json)',
    ),
)
@click.option(
    '-g',
    '--ignore-values-file',
    'ignore_values_file',
    type=click_pathlib.Path(),
    help='Load values to be ignored from file',
)
@click.option(
    '-d',
    '--dry-run',
    'dry_run',
    is_flag=True,
    default=False,
    help='Skip replacing and saving CSV file values (default: false)',
)
@click.pass_context
def process_file(
    ctx,
    column: str,
    csv_file: Path,
    in_place: bool,
    output_file: Path,
    save_openrefine: bool,
    openrefine_output_file: Path,
    save_processed_values: bool,
    processed_values_output_file: Path,
    ignore_values_file: Path,
    dry_run: bool,
):
    """Replace values with suggestions from GKG for given CSV file."""
    replacer = Replacer(ctx.obj.get('GKG_API_KEY'))

    unique_values, rows, headers = _read_unique_values_from_csv(
        csv_file, column,
    )

    ignore_values = _read_ignore_values_file(ignore_values_file)

    processed_values, replacements = _process_suggestions(
        replacer, unique_values, ignore_values,
    )

    if save_processed_values:
        _create_processed_values_output_file(
            processed_values, processed_values_output_file, csv_file,
        )

    if save_openrefine:
        _create_openrefine_file(
            openrefine_output_file, csv_file, replacements, column,
        )

    if dry_run:
        sys.exit(0)

    output_file_path = csv_file if in_place else output_file
    _create_output_file(
        output_file_path, csv_file, headers, rows, replacements, column,
    )


if __name__ == '__main__':
    cli(obj={})
