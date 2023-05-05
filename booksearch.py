import argparse
import run_abebooks as abe
import run_annas_archive as ann

def main():

    parser = argparse.ArgumentParser()
    parser.add_argument('--title', '-t')
    parser.add_argument('--author', '-a')
    #parser.add_argument('--sources', '-s', nargs='*')
    args = parser.parse_args()

    print("\nANNA'S ARCHIVE")
    ann.main_display(query=args.title + ' ' + args.author)
    print("\nABEBOOKS")
    abe.generate_main_display(edmonton_only=False)(title=args.title, author=args.author)
    print("\nABEBOOKS (EDMONTON)")
    abe.generate_main_display(edmonton_only=True)(title=args.title, author=args.author)


if __name__ == '__main__':
    main()
