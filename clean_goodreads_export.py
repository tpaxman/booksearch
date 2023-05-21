import argparse
import pandas as pd
import tools.goodreads as goodreads

def main():
    parser = argparse.ArgumentParser()
    parser.add_argument('input_csv')
    parser.add_argument('output_csv')
    args = parser.parse_args()
    input_csv = args.input_csv
    output_csv = args.output_csv

    goodreads_raw = pd.read_csv(input_csv)
    goodreads_clean = goodreads.clean_library_export(goodreads_raw)

    goodreads_clean.to_csv(output_csv, index=False)


if __name__ == '__main__':
    main()
