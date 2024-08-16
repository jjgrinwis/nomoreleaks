import os
import time
import logging
from c8 import C8Client
from c8.exceptions import (
    InsertKVError,
    DocumentGetError,
    DocumentRevisionError,
    CollectionTruncateError
)

# Logging setup
logging.basicConfig(level=logging.INFO)
logger = logging.getLogger(__name__)

# Ensure that environment variables are set
api_key = os.environ.get('C8_API_KEY')
fabric = os.environ.get('C8_FABRIC')

if not api_key or not fabric:
    raise EnvironmentError('C8_API_KEY and C8_FABRIC must be set as environment variables.')

# File with hashes, this should come from somewhere 
sha256_list = 'hashes_sha256.txt'

# Our collection which holds all documents
COLLECTION_NAME = 'NoMoreLeaks'

def main():
    kv_list = []
    entry = {}

    # Load SHA-256 hashes from file
    try:
        with open(sha256_list) as file:
            for line in file:
                entry['_key'] = line.rstrip()
                entry['value'] = None
                kv_list.append(entry.copy())
    except FileNotFoundError:
        logger.error(f'File {sha256_list} not found.')
        return

    # Connect to Macrometa
    try:
        macrometa_client = C8Client(protocol='https', host='play.paas.macrometa.io', port=443, apikey=api_key, geofabric=fabric)
    except Exception as e:
        logger.error(f'Failed to connect to Macrometa: {e}')
        return

    # Retrieve metadata
    try:
        meta_data = macrometa_client.get_document(COLLECTION_NAME, 'metadata') or {}
    except (DocumentGetError, DocumentRevisionError) as e:
        logger.error(f'Failed to get document: {e}')
        meta_data = {}

    # Truncate the collection
    try:
        macrometa_client.collection(COLLECTION_NAME).truncate()
    except CollectionTruncateError:
        logger.error(f'Truncation of collection "{COLLECTION_NAME}" failed')

    # Update the metadata key, feel free to add extra fields
    meta_data['active_version'] = 'v45'
    meta_data['version_updated'] = int(time.time())

    # Add metadata info to kv_list, the list we're going to write to our collection
    kv_list.append({'_key': 'metadata', 'value': meta_data})

    # Insert the key-value pairs into the collection
    try:
        macrometa_client.insert_key_value_pair(COLLECTION_NAME, kv_list)
    except InsertKVError:
        logger.error('Insertion of key-value pairs failed')

if __name__ == "__main__":
    main()
