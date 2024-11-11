from sqlalchemy import select, func, create_engine, MetaData, Table,Column,String,Float

import json

# Load database configuration from config.json
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

# Fetch unique trait types and values
with engine.connect() as connection:
    trait_table = Table('trait_table', MetaData(bind=engine), autoload=True)
    
    # Get distinct trait_type and trait_value combinations
    distinct_traits_query = select([trait_table.c.trait_type, trait_table.c.tarit_value]).distinct()
    distinct_traits = connection.execute(distinct_traits_query).fetchall()

    # Calculate rarity percentage for each unique trait
    rarity_data = {}
    for trait in distinct_traits:
        trait_type, trait_value = trait
        total_items = connection.execute(func.count().select().where(trait_table.c.trait_type == trait_type)).scalar()
        items_with_trait = connection.execute(func.count().select().where(
            (trait_table.c.trait_type == trait_type) & (trait_table.c.tarit_value == trait_value)
        )).scalar()

        rarity_percentage = items_with_trait / total_items if total_items > 0 else 0
        rarity_data[(trait_type, trait_value)] = rarity_percentage

    # Compute rarity score based on inverse of rarity percentage
    rarity_scores = {trait: 1 / rarity_percentage if rarity_percentage > 0 else float('inf') for trait, rarity_percentage in rarity_data.items()}

    # Print the rarity percentages and corresponding rarity scores
    print("Rarity Percentages and Rarity Scores:")
    for trait, rarity_percentage in rarity_data.items():
        rarity_score = rarity_scores[trait]
        print(f"Trait: {trait}, Rarity Percentage: {rarity_percentage}, Rarity Score: {rarity_score}")

rarity_table = Table('rarity_table', metadata, autoload=True, autoload_with=engine)
# Create the table in the database
metadata.create_all(engine)
with engine.connect() as connection:
    rarity_data_to_insert = [
        {'trait_type': trait_type, 'trait_value': trait_value, 'rarity_percentage': rarity_percentage, 'rarity_score': rarity_score}
        for (trait_type, trait_value), rarity_percentage in rarity_data.items()
        for (trait_type, trait_value), rarity_score in rarity_scores.items()
    ]
    connection.execute(rarity_table.insert(), rarity_data_to_insert)

print("Rarity data has been stored in the rarity_table.")