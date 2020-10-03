# Copyright (C) 2020 VIB/BEG/UGent - Tim Diels <tim@diels.me>
#
# This file is part of varbio.
#
# varbio is free software: you can redistribute it and/or modify
# it under the terms of the GNU Lesser General Public License as published by
# the Free Software Foundation, either version 3 of the License, or
# (at your option) any later version.
#
# varbio is distributed in the hope that it will be useful,
# but WITHOUT ANY WARRANTY; without even the implied warranty of
# MERCHANTABILITY or FITNESS FOR A PARTICULAR PURPOSE.  See the
# GNU Lesser General Public License for more details.
#
# You should have received a copy of the GNU Lesser General Public License
# along with varbio.  If not, see <http://www.gnu.org/licenses/>.

from textwrap import dedent
import csv
import io
import logging

from varbio._util import open_text, UserError, join_lines


def parse_csv(path):
    '''
    Robustly parse csv

    Parameters
    ----------
    path : ~pathlib.Path

    Yields
    ------
    CSV rows, header included as lists. All values are str with outer
    whitespace stripped.
    '''
    # Remove empty lines up front, otherwise the sniffer fails to detect
    # the right/any delimiter sometimes.
    with open_text(path) as f:
        line_numbers = []
        lines = []
        for line_number, line in _read_non_empty_lines(f):
            line_numbers.append(line_number)
            lines.append(line)
        text = '\n'.join(lines)

    if not lines:
        raise UserError(join_lines(
            '''
            csv file is empty (except for maybe some whitespace). It must
            contain at least a header line'
            '''
        ))

    dialect = _detect_dialect(text, lines)
    logging.info(dedent(f'''\
        Detected csv dialect of {path}:
        delimiter {dialect.delimiter!r}
        quotechar {dialect.quotechar!r}
        doublequote {dialect.doublequote!r}
        quoting {dialect.quoting!r}
        escapechar {dialect.escapechar!r}'''
    ))

    yield from _parse(text, lines, line_numbers, dialect)

def _detect_dialect(text, lines):
    # Possible delimiters have to be specified, otherwise it can pick any char
    # as delimiter, e.g. 'e'.
    delimiters = ';,\t| '
    sniffer = csv.Sniffer()
    try:
        return sniffer.sniff(text, delimiters)
    except csv.Error as ex:
        logging.warning(join_lines(
            f'''
            Failed to autodetect csv format based on whole file, retrying with
            just the first 2 (non-empty) lines. Autodetect error: {ex}
            '''
        ))
        try:
            return sniffer.sniff('\n'.join(lines[:2]), delimiters)
        except csv.Error as ex:
            msg = (
                f'''
                Failed to autodetect csv format, there is a syntax error in at
                least the first 2 non-empty lines of the file. Ensure both rows
                have the same amount of columns; try using ';' as column
                separator/delimiter. Autodetect error: {ex}
                '''
            )
            raise UserError(msg) from ex

def _parse(text, lines, line_numbers, dialect):
    def get_line():
        # line_num is not the same as using enumerate if a csv row can span
        # multiple lines; which isn't the case with our inputs though, but no
        # reason not to use line_num (1-based)
        return lines[reader.line_num - 1]

    def get_line_number():
        return line_numbers[reader.line_num - 1]

    f = io.StringIO(text)
    reader = csv.reader(f, dialect)
    col_count = None
    for row in reader:
        if not col_count:
            col_count = len(row)

        if len(row) != col_count:
            raise UserError(
                f'Line {get_line_number()} (1-based) has {len(row)} columns, '
                f'expected {col_count}. Line:\n{get_line()}'
            )

        row = [value.strip() for value in row]

        for col, value in enumerate(row, start=1):
            if not value:
                raise UserError(
                    f'Line {get_line_number()}, column {col} (1-based) is empty (or '
                    f'is whitespace); it must have a value. Line:\n{get_line()}'
                )

        yield row

def _read_non_empty_lines(f):
    for line_number, line in enumerate(f.readlines(), start=1):
        if not line.strip():
            continue
        yield line_number, line.rstrip('\n')
