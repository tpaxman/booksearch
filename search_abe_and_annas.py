import argparse
import tools.abebooks as abe
import tools.annas_archive as ann

def main():

    parser = argparse.ArgumentParser()
    parser.add_argument('--title', '-t')
    parser.add_argument('--author', '-a')
    parser.add_argument('--sources', '-s', nargs='*')
    args = parser.parse_args()

    ann.main_display(query=args.title + ' ' + args.author)
    abe.generate_main_display(edmonton_only=False)(title=args.title, author=args.author)
    abe.generate_main_display(edmonton_only=True)(title=args.title, author=args.author)


if __name__ == '__main__':
    main()
