import argparse
import os

from ConfigParser import SafeConfigParser


def main():
    parser = argparse.ArgumentParser()
    parser.add_argument(
        '-c', '--config', type=str, required=True,
        help='Path to the config ini file')
    parser.add_argument(
        '-s', '--section', type=str, required=True,
        help='Section')
    parser.add_argument(
        '-n', '--name', type=str, required=True,
        help='Name')
    parser.add_argument(
        '-v', '--value', type=str, required=True,
        help='Value')

    args = parser.parse_args()
    config_file = os.path.expanduser(args.config)

    ini_parser = SafeConfigParser()
    ini_parser.read(config_file)
    ini_parser.set(args.section, args.name, args.value)

    with open(config_file, 'wb') as f:
        ini_parser.write(f)


if __name__ == '__main__':
    main()
