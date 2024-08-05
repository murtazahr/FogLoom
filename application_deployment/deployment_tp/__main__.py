import sys
import argparse
import logging
from sawtooth_sdk.processor.core import TransactionProcessor
from handler import DockerDeploymentTransactionHandler
from sawtooth_sdk.processor.log import init_console_logging


def create_console_handler(verbose_level):
    clog = logging.StreamHandler()
    formatter = logging.Formatter(
        '%(asctime)s [%(levelname)s] %(name)s: %(message)s')
    clog.setFormatter(formatter)

    if verbose_level == 0:
        clog.setLevel(logging.WARN)
    elif verbose_level == 1:
        clog.setLevel(logging.INFO)
    else:
        clog.setLevel(logging.DEBUG)

    return clog


def parse_args(args):
    parser = argparse.ArgumentParser(
        formatter_class=argparse.RawTextHelpFormatter)

    parser.add_argument(
        '-C', '--connect',
        default='tcp://localhost:4004',
        help='Endpoint for the validator connection')

    parser.add_argument('-v', '--verbose',
                        action='count',
                        default=0,
                        help='Increase output sent to stderr')

    return parser.parse_args(args)


def main(args=None):
    if args is None:
        args = sys.argv[1:]
    opts = parse_args(args)
    processor = None
    try:
        # Set up logging
        verbose_level = opts.verbose
        init_console_logging(verbose_level=verbose_level)

        processor = TransactionProcessor(url=opts.connect)
        handler = DockerDeploymentTransactionHandler()
        processor.add_handler(handler)
        processor.start()
    except KeyboardInterrupt:
        pass
    except Exception as e:
        print("Error: {}".format(e), file=sys.stderr)
    finally:
        if processor is not None:
            processor.stop()


if __name__ == "__main__":
    main()
