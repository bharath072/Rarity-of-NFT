import requests
import json
from eth_utils import to_checksum_address
from web3 import Web3, HTTPProvider
from sqlalchemy import create_engine, MetaData, Table, insert
import ipfshttpclient
from moralis import evm_api

# Initialize Web3
w3 = Web3(HTTPProvider('https://mainnet.infura.io/v3/0122fa228a70405396ba354d417a66a3'))

if w3:
    print("Connection successful to Web3.")
else:
    print("Not connected to Web3.")

# Etherscan API key
etherscan_api_key = "C869CN3JSWUWAJH5A736DB6BVUC7UMJYKT"

def fetch_etherscan_metadata(contract_address):
    url = "https://api.etherscan.io/api"
    params = {
        "module": "contract",
        "action": "getsourcecode",
        "address": contract_address,
        "apikey": etherscan_api_key
    }
    response = requests.get(url, params=params)

    if response.status_code == 200:
        data = response.json()
        result = data.get("result")

        if result:
            metadata = {
                "ContractName": result[0]["ContractName"],
                "CompilerVersion": result[0]["CompilerVersion"],
                "OptimizationUsed": result[0]["OptimizationUsed"],
                "Runs": result[0]["Runs"],
                "LicenseType": result[0]["LicenseType"],
                "EVMVersion": result[0]["EVMVersion"]
            }
            print(json.dumps(metadata, indent=4))
            return metadata
        else:
            print("Metadata not found for collection:", contract_address)
            return None
    else:
        print("Failed to fetch metadata from Etherscan")
        return None

# Example contract address
contract_address = "0x60E4d786628Fea6478F785A6d7e704777c86a7c6"

# Convert contract address to checksum address
checksum_contract_address = to_checksum_address(contract_address)
with open('config.json') as f:
    config = json.load(f)
db_params = config['database']
engine = create_engine(
    f"postgresql://{db_params['user']}:{db_params['password']}@{db_params['host']}:{db_params['port']}/{db_params['database']}"
)


c_data = fetch_etherscan_metadata(contract_address)

# Initialize Web3 contract
checksum_contract_address = to_checksum_address(contract_address)
etherscan_api_url = f'https://api.etherscan.io/api?module=contract&action=getabi&address={checksum_contract_address}'
response = requests.get(etherscan_api_url)
data = response.json()
contract_abi = data.get('result')

if not contract_abi:
    exit("Failed to fetch ABI from Etherscan")

# Insert data into the 'collection' table
with engine.connect() as connection:
    collection_table = Table('collection', MetaData(bind=engine), autoload=True)
    values_to_insert = [
        {'collection_id': contract_address, 'contract_name': c_data['ContractName']}
    ]
    insert_statement1 = insert(collection_table).values(values_to_insert)
    connection.execute(insert_statement1)
    print("The collection ID added successfully to the 'collection' table.")

# Initialize Web3 contract
contract_abi = json.loads(contract_abi)
nft_contract = w3.eth.contract(abi=contract_abi, address=checksum_contract_address)
def get_token_ids(collection_id):
    api_key = "EWqWGLCnqtkZlf8x27j2Ji5axSAUtqpZt2LgthG02ZSThkJSl9URAXFcyAybLLqY"
    params = {
        "chain": "eth",
        "format": "decimal",
        "address": collection_id
    }

    token_ids = []

    # Initial request without cursor
    result = evm_api.nft.get_nft_owners(api_key=api_key, params=params)
    token_ids += [item['token_id'] for item in result.get('result', [])]

    # Check if there are more pages with cursors
    while 'cursor' in result and result['cursor'] is not None:
        cursor = result['cursor']
        params['cursor'] = cursor  # Add cursor to the parameters for the next request

        # Make the next request with the cursor
        result = evm_api.nft.get_nft_owners(api_key=api_key, params=params)
        token_ids += [item['token_id'] for item in result.get('result', [])]
        token_ids.sort()
    # Print all retrieved token IDs
    return token_ids

# Function to get token metadata
def get_token_metadata(token_id):
    try:
        metadata_uri = nft_contract.functions.tokenURI(int(token_id)).call()

        # Check if the URI starts with 'ipfs://'
        if metadata_uri.startswith('ipfs://'):
            ipfs_hash = metadata_uri[len('ipfs://'):]

            # Connect to the local IPFS daemon
            client = ipfshttpclient.connect()

            # Fetch the data from IPFS
            metadata_response = client.cat(ipfs_hash)
            metadata = json.loads(metadata_response.decode('utf-8-sig'))

            # Close the IPFS client
            client.close()

            return metadata
        else:
            # If not using IPFS, use the existing method to fetch from a regular URI
            metadata_response = requests.get(metadata_uri)
            metadata = json.loads(metadata_response.content.decode('utf-8-sig'))

            return metadata
    except Exception as e:
        print(f"Error getting Collection metadata: {e}")
        return None


with engine.connect() as connection:
    asset_table = Table('asset_table', MetaData(bind=engine), autoload=True)
    trait_table = Table('trait_table', MetaData(bind=engine), autoload=True)

    # Get token IDs for the collection
    tokens = get_token_ids(contract_address)
    for token_id in tokens:
        metadata = get_token_metadata(token_id)

        if metadata is not None and 'attributes' in metadata:
            # Insert data into the 'asset_table'
            values_to_insert_asset = [
                {'collection_id': contract_address, 'contract_name': c_data['ContractName'],
                 'token_id': token_id, 'metadata': metadata['attributes']}
            ]
            insert_statement_asset = insert(asset_table).values(values_to_insert_asset)
            connection.execute(insert_statement_asset)

            # Insert data into the 'trait_table'
            trait_data = [{'token_id': token_id,
                           'trait_type': trait['trait_type'],
                           'tarit_value': trait['value'],
                           'collection_id': contract_address,
                           'contract_name': c_data['ContractName']}
                          for trait in metadata['attributes']]
            insert_statement_trait = insert(trait_table).values(trait_data)
            connection.execute(insert_statement_trait)

            print(f"Data added to the 'asset_table' and 'trait_table' for token ID {token_id}.")
        else:
            print(f"Skipping metadata for token ID {token_id} as it is None or does not contain 'attributes'.")

