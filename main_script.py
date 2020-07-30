import argparse
from p_acquisition import m_acquisition as mac
from p_wrangling import m_wrangling as wra
from p_analysis import m_analysis as anl


def argument_parser():
    parser = argparse.ArgumentParser(description='specify input file and country...')
    parser.add_argument("-p", "--path", help='specify raw_data_project_m1 route...', required=True, type=str)
    parser.add_argument("-c", "--country", default='all', help='specify country...', required=False, type=str)
    args = parser.parse_args()

    return args


def main(args):
    print('starting pipeline...')
    data = mac.acquire(args.path)
    data = wra.wrangling(data)
    table_country = anl.percent_country(data, args.country)
    print(table_country)
    print('pipeline finished...')


if __name__ == '__main__':
    arguments = argument_parser()
    main(arguments)
