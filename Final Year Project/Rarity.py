from sqlalchemy import select, func, create_engine, MetaData, Table
import json
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

# Load tables
trait_table = Table('trait_table', metadata, autoload=True, autoload_with=engine)
rarity_table = Table('rarity_table', metadata, autoload=True, autoload_with=engine)

# Create a connection
with engine.connect() as connection:
    # Fetch all token IDs
    token_ids = connection.execute(select([trait_table.c.token_id]).distinct()).fetchall()

    # Initialize list to store token IDs with rarity sum
    token_rarity_sums = []

    # Calculate overall rarity sum for each token_id
    for token_id in token_ids:
        token_id = token_id['token_id']

        # Fetch unique traits for the token_id
        token_traits_query = select([trait_table.c.trait_type, trait_table.c.tarit_value]).distinct().where(
            trait_table.c.token_id == token_id
        )
        token_traits = connection.execute(token_traits_query).fetchall()

        # Calculate overall rarity sum for the token_id
        rarity_sum = 0
        for trait in token_traits:
            trait_rarity_query = select([func.sum(rarity_table.c.rarity_percentage)]).where(
                (rarity_table.c.trait_type == trait['trait_type']) & (rarity_table.c.trait_value == trait['tarit_value'])
            )
            trait_rarity_sum = connection.execute(trait_rarity_query).scalar()
            if trait_rarity_sum is not None:
                rarity_sum += trait_rarity_sum

        # Multiply the rarity sum by 100
        rarity_sum *= 100

        # Append token_id and rarity sum to the list
        token_rarity_sums.append({'token_id': token_id, 'rarity_sum': rarity_sum})

    # Sort the list of token IDs based on rarity sum
    
    token_rarity_sums.sort(key=lambda x: x['rarity_sum'])
    filtered_token_rarity_sums = [token for token in token_rarity_sums if token['rarity_sum'] != 0]
    # Extract top 20 token IDs with lowest rarity sum
    top_20_rarest_tokens = filtered_token_rarity_sums[:20]

    # Print the top 20 rarest tokens
    print("Top 20 Rarest NFT Tokens:")
    for index, token in enumerate(top_20_rarest_tokens, start=1):
        if token['rarity_sum'] != 0:
                
                print(f"{index}. Token ID: {token['token_id']}, Rarity Score: {token['rarity_sum']}")