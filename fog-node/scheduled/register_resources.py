import hashlib
import json
import logging
import os
import ssl
import sys
import tempfile
import time
import psutil
import requests
import asyncio
from ibmcloudant.cloudant_v1 import CloudantV1
from ibm_cloud_sdk_core.authenticators import BasicAuthenticator
from ibm_cloud_sdk_core.api_exception import ApiException
from coredis import RedisCluster
from coredis.exceptions import RedisError
from sawtooth_sdk.messaging.stream import Stream
from sawtooth_sdk.protobuf.batch_pb2 import BatchHeader, Batch, BatchList
from sawtooth_sdk.protobuf.transaction_pb2 import TransactionHeader, Transaction
from sawtooth_signing import create_context, CryptoFactory, secp256k1

logger = logging.getLogger(__name__)

FAMILY_NAME = 'peer-registry'
FAMILY_VERSION = '1.0'
NAMESPACE = hashlib.sha512(FAMILY_NAME.encode()).hexdigest()[:6]

# Path to the private key file
PRIVATE_KEY_FILE = os.getenv('SAWTOOTH_PRIVATE_KEY', '/root/.sawtooth/keys/root.priv')

# Cloudant configuration
CLOUDANT_URL = f"https://{os.getenv('COUCHDB_HOST', 'couch-db-0:6984')}"
CLOUDANT_USERNAME = os.getenv('COUCHDB_USER')
CLOUDANT_PASSWORD = os.getenv('COUCHDB_PASSWORD')
CLOUDANT_DB = 'resource_registry'
CLOUDANT_SSL_CA = os.getenv('COUCHDB_SSL_CA')
CLOUDANT_SSL_CERT = os.getenv('COUCHDB_SSL_CERT')
CLOUDANT_SSL_KEY = os.getenv('COUCHDB_SSL_KEY')

# Redis configuration
REDIS_HOST = os.getenv('REDIS_HOST', 'redis-cluster')
REDIS_PORT = int(os.getenv('REDIS_PORT', '6379'))
REDIS_PASSWORD = os.getenv('REDIS_PASSWORD')
REDIS_SSL_CERT = os.getenv('REDIS_SSL_CERT')
REDIS_SSL_KEY = os.getenv('REDIS_SSL_KEY')
REDIS_SSL_CA = os.getenv('REDIS_SSL_CA')

UPDATE_INTERVAL = int(os.getenv('RESOURCE_UPDATE_INTERVAL', 300))
BLOCKCHAIN_BATCH_SIZE = int(os.getenv('RESOURCE_UPDATE_BATCH_SIZE', 5))


def get_resource_data():
    try:
        # CPU information
        cpu_count = psutil.cpu_count()
        cpu_percent = psutil.cpu_percent(interval=1)

        # Memory information
        memory = psutil.virtual_memory()
        memory_total = memory.total
        memory_used = memory.used
        memory_percent = memory.percent

        # Disk information
        disk = psutil.disk_usage('/')
        disk_total = disk.total
        disk_used = disk.used
        disk_percent = disk.percent

        return {
            'cpu': {
                'total': cpu_count,
                'used_percent': cpu_percent
            },
            'memory': {
                'total': memory_total,
                'used': memory_used,
                'used_percent': memory_percent
            },
            'disk': {
                'total': disk_total,
                'used': disk_used,
                'used_percent': disk_percent
            }
        }
    except Exception as e:
        logger.error(f"Error getting resource data: {str(e)}")
        return None


def load_private_key(key_file):
    try:
        with open(key_file, 'r') as key_reader:
            private_key_str = key_reader.read().strip()
            return secp256k1.Secp256k1PrivateKey.from_hex(private_key_str)
    except IOError as e:
        raise IOError(f"Failed to load private key from {key_file}: {str(e)}") from e


def connect_to_cloudant():
    logger.info("Starting Cloudant initialization")
    temp_files = []
    try:
        authenticator = BasicAuthenticator(CLOUDANT_USERNAME, CLOUDANT_PASSWORD)
        client = CloudantV1(authenticator=authenticator)
        client.set_service_url(CLOUDANT_URL)

        ssl_verify = True
        cert_files = None

        if CLOUDANT_SSL_CA:
            ca_file = tempfile.NamedTemporaryFile(delete=False, mode='w+', suffix='.crt')
            ca_file.write(CLOUDANT_SSL_CA)
            ca_file.flush()
            temp_files.append(ca_file.name)
            ssl_verify = ca_file.name
        else:
            logger.warning("CLOUDANT_SSL_CA is empty or not set")
            ssl_verify = False

        if CLOUDANT_SSL_CERT and CLOUDANT_SSL_KEY:
            cert_file = tempfile.NamedTemporaryFile(delete=False, mode='w+', suffix='.crt')
            key_file = tempfile.NamedTemporaryFile(delete=False, mode='w+', suffix='.key')
            cert_file.write(CLOUDANT_SSL_CERT)
            key_file.write(CLOUDANT_SSL_KEY)
            cert_file.flush()
            key_file.flush()
            temp_files.extend([cert_file.name, key_file.name])
            cert_files = (cert_file.name, key_file.name)
        else:
            logger.warning("CLOUDANT_SSL_CERT or CLOUDANT_SSL_KEY is empty or not set")

        # Set the SSL configuration for the client
        client.set_http_config({
            'verify': ssl_verify,
            'cert': cert_files
        })

        # Test the connection
        try:
            db_info = client.get_database_information(db=CLOUDANT_DB).get_result()
            logger.info(f"Successfully connected to database '{CLOUDANT_DB}'.")
            return client
        except ApiException as e:
            logger.error(f"Failed to connect to Cloudant database: {str(e)}")
            raise

    except Exception as e:
        logger.error(f"Failed to initialize Cloudant connection: {str(e)}")
        raise
    finally:
        for file_path in temp_files:
            try:
                os.unlink(file_path)
                logger.debug(f"Temporary file deleted: {file_path}")
            except Exception as e:
                logger.warning(f"Failed to delete temporary file {file_path}: {str(e)}")


async def connect_to_redis():
    logger.info("Starting Redis initialization")
    temp_files = []
    try:
        ssl_context = ssl.create_default_context(ssl.Purpose.SERVER_AUTH)
        ssl_context.check_hostname = False
        ssl_context.verify_mode = ssl.CERT_NONE
        ssl_context.minimum_version = ssl.TLSVersion.TLSv1_2
        ssl_context.maximum_version = ssl.TLSVersion.TLSv1_3

        if REDIS_SSL_CA:
            ca_file = tempfile.NamedTemporaryFile(delete=False, mode='w+', suffix='.crt')
            ca_file.write(REDIS_SSL_CA)
            ca_file.flush()
            temp_files.append(ca_file.name)
            ssl_context.load_verify_locations(cafile=ca_file.name)
        else:
            logger.warning("REDIS_SSL_CA is empty or not set")

        if REDIS_SSL_CERT and REDIS_SSL_KEY:
            cert_file = tempfile.NamedTemporaryFile(delete=False, mode='w+', suffix='.crt')
            key_file = tempfile.NamedTemporaryFile(delete=False, mode='w+', suffix='.key')
            cert_file.write(REDIS_SSL_CERT)
            key_file.write(REDIS_SSL_KEY)
            cert_file.flush()
            key_file.flush()
            temp_files.extend([cert_file.name, key_file.name])
            ssl_context.load_cert_chain(
                certfile=cert_file.name,
                keyfile=key_file.name
            )
        else:
            logger.warning("REDIS_SSL_CERT or REDIS_SSL_KEY is empty or not set")

        logger.info(f"Attempting to connect to Redis cluster at {REDIS_HOST}:{REDIS_PORT}")
        redis = await RedisCluster(
            host=REDIS_HOST,
            port=REDIS_PORT,
            password=REDIS_PASSWORD,
            ssl=True,
            ssl_context=ssl_context,
            decode_responses=True
        )
        logger.info("Connected to Redis cluster successfully")
        return redis

    except Exception as e:
        logger.error(f"Failed to connect to Redis: {str(e)}")
        raise
    finally:
        for file_path in temp_files:
            try:
                os.unlink(file_path)
                logger.debug(f"Temporary file deleted: {file_path}")
            except Exception as e:
                logger.warning(f"Failed to delete temporary file {file_path}: {str(e)}")


async def update_redis(redis_cluster, node_id, resource_data):
    try:
        key = f"resources_{node_id}"
        # Measure the time taken for the Redis write operation
        start_time = time.time()
        await redis_cluster.set(key, json.dumps(resource_data))
        end_time = time.time()

        write_time = end_time - start_time
        logger.info(f"Updated resource data for node {node_id} in Redis. Write time: {write_time:.4f} seconds")

        logger.info(f"Updated Redis with latest resource data for node {node_id}")
    except RedisError as e:
        logger.error(f"Error updating Redis for node {node_id}: {str(e)}")


def store_resource_data(client, node_id, resource_data):
    try:
        doc_id = node_id
        timestamp = int(time.time())
        data_hash = hashlib.sha256(json.dumps(resource_data, sort_keys=True).encode()).hexdigest()

        # Try to get the existing document
        try:
            doc = client.get_document(db=CLOUDANT_DB, doc_id=doc_id).get_result()
        except ApiException:
            # Document doesn't exist, create a new one
            doc = {
                '_id': doc_id,
                'resource_data_list': [],
                'latest_update': {}
            }

        # Update the document
        if 'resource_data_list' not in doc:
            doc['resource_data_list'] = []
        doc['resource_data_list'].append({
            'timestamp': timestamp,
            'data': resource_data,
            'data_hash': data_hash
        })
        doc['latest_update'] = {
            'timestamp': timestamp,
            'data_hash': data_hash
        }

        # Measure the time taken for the Cloudant write operation
        start_time = time.time()
        if '_rev' in doc:
            response = client.put_document(db=CLOUDANT_DB, doc_id=doc_id, document=doc).get_result()
        else:
            response = client.post_document(db=CLOUDANT_DB, document=doc).get_result()
        end_time = time.time()

        write_time = end_time - start_time
        logger.info(f"Appended resource data for node {node_id} in Cloudant. Write time: {write_time:.4f} seconds")

        return {'timestamp': timestamp, 'data_hash': data_hash, 'write_time': write_time}
    except Exception as e:
        logger.error(f"Error storing resource data for node {node_id} in Cloudant: {str(e)}")
        return None


def create_transaction(node_id, updates, signer):
    payload = json.dumps({
        'node_id': node_id,
        'updates': updates
    }).encode()

    header = TransactionHeader(
        family_name=FAMILY_NAME,
        family_version=FAMILY_VERSION,
        inputs=[NAMESPACE],
        outputs=[NAMESPACE],
        signer_public_key=signer.get_public_key().as_hex(),
        batcher_public_key=signer.get_public_key().as_hex(),
        dependencies=[],
        payload_sha512=hashlib.sha512(payload).hexdigest(),
        nonce=hex(int(time.time()))
    ).SerializeToString()

    signature = signer.sign(header)

    transaction = Transaction(
        header=header,
        payload=payload,
        header_signature=signature
    )

    logger.info(f"Transaction created with signature: {signature}")
    return transaction


def create_batch(transactions, signer):
    logger.info(f"Creating batch for transactions: {transactions}")
    batch_header = BatchHeader(
        signer_public_key=signer.get_public_key().as_hex(),
        transaction_ids=[t.header_signature for t in transactions],
    ).SerializeToString()

    signature = signer.sign(batch_header)

    batch = Batch(
        header=batch_header,
        transactions=transactions,
        header_signature=signature,
    )

    logger.info(f"Batch created with signature: {signature}")
    return batch


def submit_batch(batch):
    logger.info("Submitting batch to validator")
    stream = Stream(url=os.getenv('VALIDATOR_URL', 'tcp://validator:4004'))

    batch_list = BatchList(batches=[batch])
    future = stream.send(
        message_type='CLIENT_BATCH_SUBMIT_REQUEST',
        content=batch_list.SerializeToString()
    )

    result = future.result()
    logger.info(f"Submitted batch to validator: {result}")


async def main():
    logger.info("Starting Resource Registration Client")
    node_id = os.getenv('NODE_ID', 'unrecognized_node')

    try:
        private_key = load_private_key(PRIVATE_KEY_FILE)
    except IOError as e:
        logger.error(str(e))
        sys.exit(1)

    context = create_context('secp256k1')
    signer = CryptoFactory(context).new_signer(private_key)

    client = connect_to_cloudant()
    if not client:
        logger.error("Couldn't connect to Cloudant. Exiting.")
        sys.exit(1)

    redis_cluster = await connect_to_redis()
    if not redis_cluster:
        logger.error("Couldn't connect to Redis cluster. Exiting.")
        sys.exit(1)

    updates = []
    while True:
        try:
            resource_data = get_resource_data()
            if resource_data:
                logger.debug(f"Resource data: {json.dumps(resource_data, indent=2)}")
                update_info = store_resource_data(client, node_id, resource_data)
                if update_info:
                    updates.append(update_info)

                    # Update Redis with the latest resource data
                    await update_redis(redis_cluster, node_id, resource_data)

                    if len(updates) >= BLOCKCHAIN_BATCH_SIZE:
                        transaction = create_transaction(node_id, updates, signer)
                        batch = create_batch([transaction], signer)
                        submit_batch(batch)
                        logger.info(f"Logged {len(updates)} resource updates in blockchain for node {node_id}")
                        updates = []
            else:
                logger.warning("Failed to get resource data")
        except Exception as e:
            logger.error(f"Error in main loop: {str(e)}")
        await asyncio.sleep(UPDATE_INTERVAL)


if __name__ == '__main__':
    logging.basicConfig(level=logging.DEBUG)
    asyncio.run(main())
