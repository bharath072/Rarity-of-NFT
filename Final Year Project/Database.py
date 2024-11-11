import json
from sqlalchemy import create_engine, Column, Integer, String, DateTime, Boolean, MetaData, Table, JSON, Float
from collections import Counter
# Loading the database parameters from config.json file
with open('config.json') as f:
    config = json.load(f)
# Database connection parameters
db_params = config['database']
# Creating database engine
engine = create_engine(
    f"postgresql://{db_params['user']}:{db_params['password']}@{db_params['host']}:{db_params['port']}/{db_params['database']}"
)
# Define metadata
metadata = MetaData()
"""
The metadata object essentially acts as a container to keep track of all the table definitions and related information.
"""
# Define the Collection table
collection_table = Table(
    'collection',
    metadata,
    Column('id', Integer, primary_key=True),
    Column('collection_id', String),
    Column('contract_name', String)
)

# Define the Asset Metadata table
asset_metadata_table = Table(
    'asset_table',
    metadata,
    Column('id', Integer, primary_key=True),
    Column('collection_id', String),
    Column('contract_name', String),
    Column('token_id', Integer),
    Column('metadata', JSON)
)

# Define trait table 
trait_table = Table(
    'trait_table',
    metadata,
    Column('id', Integer, primary_key=True),
    Column('collection_id', String),
    Column('contract_name', String),
    Column('token_id', Integer),
    Column('trait_type', String),
    Column('tarit_value', String)
)


rarity_table = Table(
    'rarity_table', 
    metadata, 
    Column('id', Integer, primary_key=True),  
    Column('trait_type', String),              
    Column('trait_value', String),             
    Column('rarity_percentage', Float),        
    Column('rarity_score', Float)        
)

# Create tables
metadata.create_all(engine)
