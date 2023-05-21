import argparse
import pandas as pd
import tools.goodreads as goodreads

def main():
    parser = argparse.ArgumentParser()
    parser.add_argument('input_csv')
    parser.add_argument('output_csv')
    parser.add_argument('--shelf', '-s', help='shelf to filter to')
    args = parser.parse_args()
    input_csv = args.input_csv
    output_csv = args.output_csv
    shelf = args.shelf

    goodreads_raw = pd.read_csv(input_csv)
    goodreads_clean = goodreads.clean_library_export(goodreads_raw)
    
    if shelf:
        shelf_dummies = goodreads.get_shelf_dummies(goodreads_clean)
        to_read = shelf_dummies.loc[lambda t: t['to-read'], ['goodreads_id']]
        goodreads_clean = goodreads_clean.merge(to_read)

    goodreads_clean.to_csv(output_csv, index=False)


if __name__ == '__main__':
    main()
