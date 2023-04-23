from argparse import ArgumentParser, Namespace


def getparser():
    parser = ArgumentParser()
    parser.add_argument('-p', '--properties', required=True, help='Read the configurations to process file ingestion', type=str)

    args: Namespace = parser.parse_args()
    return args
