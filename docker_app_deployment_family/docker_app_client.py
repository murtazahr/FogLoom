#!/usr/bin/env python3

import argparse
import hashlib
import logging
import os
import sys
import traceback

import cbor

from colorlog import ColoredFormatter
from sawtooth_signing import create_context
from sawtooth_signing import CryptoFactory
from sawtooth_signing.secp256k1 import Secp256k1PrivateKey

from sawtooth_sdk.protobuf.transaction_pb2 import TransactionHeader
from sawtooth_sdk.protobuf.transaction_pb2 import Transaction
from sawtooth_sdk.protobuf.batch_pb2 import BatchHeader
from sawtooth_sdk.protobuf.batch_pb2 import Batch

from sawtooth_sdk.protobuf.batch_pb2 import BatchList
import urllib.request
import urllib.error

FAMILY_NAME = "docker_app"
FAMILY_VERSION = "1.0"
NAMESPACE = hashlib.sha512(FAMILY_NAME.encode('utf-8')).hexdigest()[:6]


def _get_private_keyfile(key_name):
    if os.path.isfile(key_name):
        return key_name
    home = os.path.expanduser("~")
    key_dir = os.path.join(home, ".sawtooth", "keys")
    return os.path.join(key_dir, key_name + ".priv")


def _get_public_keyfile(key_name):
    home = os.path.expanduser("~")
    key_dir = os.path.join(home, ".sawtooth", "keys")
    return os.path.join(key_dir, key_name + ".pub")


def _get_keyfile(key_name):
    private_keyfile = _get_private_keyfile(key_name)
    if os.path.exists(private_keyfile):
        return private_keyfile
    public_keyfile = _get_public_keyfile(key_name)
    if os.path.exists(public_keyfile):
        return public_keyfile
    raise Exception("No key file found for {}".format(key_name))


def _read_private_key(keyfile):
    if keyfile is None:
        home = os.path.expanduser("~")
        key_dir = os.path.join(home, ".sawtooth", "keys")
        keyfile = os.path.join(key_dir, "root.priv")
    else:
        keyfile = os.path.abspath(keyfile)

    try:
        with open(keyfile, "r") as key_file:
            private_key_str = key_file.read().strip()
        return Secp256k1PrivateKey.from_hex(private_key_str)
    except FileNotFoundError:
        print(f"No such file or directory: '{keyfile}'")
        print("Please make sure the private key file exists.")
        sys.exit(1)


def _create_batch(signer, transactions):
    transaction_signatures = [t.header_signature for t in transactions]

    header = BatchHeader(
        signer_public_key=signer.get_public_key().as_hex(),
        transaction_ids=transaction_signatures
    ).SerializeToString()

    signature = signer.sign(header)

    batch = Batch(
        header=header,
        transactions=transactions,
        header_signature=signature)

    return batch


def _create_docker_app_transaction(signer, app_name, app_version, app_data):
    payload = cbor.dumps({
        'name': app_name,
        'version': app_version,
        'data': app_data,
    })

    # Construct the address
    address = NAMESPACE + hashlib.sha512(app_name.encode()).hexdigest()[-64:]

    header = TransactionHeader(
        signer_public_key=signer.get_public_key().as_hex(),
        family_name=FAMILY_NAME,
        family_version=FAMILY_VERSION,
        inputs=[address],
        outputs=[address],
        dependencies=[],
        payload_sha512=hashlib.sha512(payload).hexdigest(),
        batcher_public_key=signer.get_public_key().as_hex(),
        nonce=hashlib.sha512(os.urandom(32)).hexdigest()
    ).SerializeToString()

    signature = signer.sign(header)

    transaction = Transaction(
        header=header,
        payload=payload,
        header_signature=signature
    )

    return transaction


def _send_docker_app_txn(url, keyfile, app_name, app_version, app_data):
    context = create_context('secp256k1')
    private_key = _read_private_key(keyfile)
    signer = CryptoFactory(context).new_signer(private_key)

    transaction = _create_docker_app_transaction(signer, app_name, app_version, app_data)
    batch = _create_batch(signer, [transaction])
    batch_list = BatchList(batches=[batch])

    batch_bytes = batch_list.SerializeToString()

    try:
        request = urllib.request.Request(
            url + '/batches',
            batch_bytes,
            method='POST',
            headers={'Content-Type': 'application/octet-stream'})
        response = urllib.request.urlopen(request)

        return response.read().decode('utf-8')

    except urllib.error.HTTPError as e:
        return e.read().decode('utf-8')


def create_console_handler(verbose_level):
    clog = logging.StreamHandler()
    formatter = ColoredFormatter(
        "%(log_color)s[%(asctime)s %(levelname)-8s%(module)s]%(reset)s "
        "%(white)s%(message)s",
        datefmt="%H:%M:%S",
        reset=True,
        log_colors={
            'DEBUG': 'cyan',
            'INFO': 'green',
            'WARNING': 'yellow',
            'ERROR': 'red',
            'CRITICAL': 'red',
        })

    clog.setFormatter(formatter)

    if verbose_level == 0:
        clog.setLevel(logging.WARN)
    elif verbose_level == 1:
        clog.setLevel(logging.INFO)
    else:
        clog.setLevel(logging.DEBUG)

    return clog


def setup_loggers(verbose_level):
    logger = logging.getLogger()
    logger.setLevel(logging.DEBUG)
    logger.addHandler(create_console_handler(verbose_level))


def create_parent_parser(prog_name):
    parent_parser = argparse.ArgumentParser(prog=prog_name, add_help=False)
    parent_parser.add_argument(
        '-v', '--verbose',
        action='count',
        help='enable more verbose output')

    parent_parser.add_argument(
        '-V', '--version',
        action='version',
        version='%(prog)s (Hyperledger Sawtooth) version ' + FAMILY_VERSION,
        help='display version information')

    parent_parser.add_argument(
        '--url',
        type=str,
        help='specify URL of REST API')

    parent_parser.add_argument(
        '--keyfile',
        type=str,
        help="identify file containing user's private key")

    return parent_parser


def create_parser(prog_name):
    parent_parser = create_parent_parser(prog_name)

    parser = argparse.ArgumentParser(
        description='Provides subcommands to manage your docker apps',
        parents=[parent_parser])

    subparsers = parser.add_subparsers(title='subcommands', dest='command')
    subparsers.required = True

    create_parser = subparsers.add_parser(
        'create',
        help='Creates a new docker app',
        parents=[parent_parser])
    create_parser.add_argument(
        'name',
        type=str,
        help='name of the docker app')
    create_parser.add_argument(
        'version',
        type=str,
        help='version of the docker app')
    create_parser.add_argument(
        'data',
        type=str,
        help='base64 encoded docker app data')

    return parser


def do_create(args):
    name = args.name
    version = args.version
    data = args.data
    url = args.url
    keyfile = args.keyfile

    response = _send_docker_app_txn(url, keyfile, name, version, data)
    print("Response: {}".format(response))


def main(prog_name=os.path.basename(sys.argv[0]), args=None):
    if args is None:
        args = sys.argv[1:]
    parser = create_parser(prog_name)
    args = parser.parse_args(args)

    if args.verbose is None:
        verbose_level = 0
    else:
        verbose_level = args.verbose
    setup_loggers(verbose_level=verbose_level)

    if args.command == 'create':
        do_create(args)
    else:
        raise Exception("Invalid command: {}".format(args.command))


def main_wrapper():
    try:
        main()
    except KeyboardInterrupt:
        pass
    except SystemExit as e:
        raise e
    except BaseException as e:
        traceback.print_exc(file=sys.stderr)
        sys.exit(1)


if __name__ == '__main__':
    main_wrapper()
