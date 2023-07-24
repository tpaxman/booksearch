from typing import Callable
import re
import pathlib
import argparse
import pandas as pd

def main():
    parser = argparse.ArgumentParser()
    parser.add_argument('input_filename', help='dot-separated file')
    parser.add_argument('--sep', '-s', default='.', help='delimiter to use')
    parser.add_argument('--column_names', '-c', nargs='+', help='names for CSV output columns')
    parser.add_argument('--output_filename', '-o', help='csv file')

    args = parser.parse_args()
    input_filename = args.input_filename
    sep = args.sep
    column_names = args.column_names
    output_filename = args.output_filename

    with open(input_filename) as f:
        lines = f.read().strip().split('\n')

    if not column_names:
        assumed_num_columns = max(x.count(sep) for x in lines)
        column_names = range(assumed_num_columns + 1)

    if not output_filename:
        output_filename = pathlib.Path(input_filename).with_suffix('.csv')

    _parse_line = _generate_line_parser(sep=sep, num_columns_expected=len(column_names))

    df = pd.DataFrame(map(_parse_line, lines), columns=column_names)
    df.to_csv(output_filename, index=False)


def _generate_line_parser(sep: str, num_columns_expected: int) -> Callable:
    def _parse_line(line: str) -> list:
        num_dots_expected = num_columns_expected - 1
        num_dots_actual = line.count(sep)
        num_dots_missing = num_dots_expected - num_dots_actual
        line_augmented = line + (sep * num_dots_missing)
        line_parts = re.split(r'\s*' + re.escape(sep) + r'\s*', line_augmented)
        return line_parts
    return _parse_line


if __name__ == '__main__':
    main()
