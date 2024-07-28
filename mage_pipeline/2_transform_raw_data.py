import pandas as pd
if 'transformer' not in globals():
    from mage_ai.data_preparation.decorators import transformer
if 'test' not in globals():
    from mage_ai.data_preparation.decorators import test


@transformer
def transform(df, *args, **kwargs):
    """
    Template code for a transformer block.

    Add more parameters to this function if this block has multiple parent blocks.
    There should be one parameter for each output variable from each parent block.

    Args:
        data: The output from the upstream parent block
        args: The output from any additional upstream blocks (if applicable)

    Returns:
        Anything (e.g. data frame, dictionary, array, int, str, etc.)
    """
    # Drop these columns
    columns_to_drop = [
    'original_location', 'country', 'region', 'province', 'city', 'top_tags', 'price_level',
    'special_diets', 'default_language', 'popularity_detailed', 'working_shifts_per_week',
    'reviews_count_in_default_language', 'excellent', 'very_good', 'average', 'poor', 'terrible'
    ]

    df = df.drop(columns=columns_to_drop)

    # Drop duplicates if there are any
    df = df.drop_duplicates()

    # Rename the column 'restaurant_link'
    df = df.rename(columns={'restaurant_link': 'tripadvisor_link'})

    # Sort the DataFrame by 'restaurant_name' alphabetically 
    df = df.sort_values(by='restaurant_name', ascending=True)
    
    # Replace null values in location columns
    df['latitude'].fillna(0, inplace=True)
    df['longitude'].fillna(0, inplace=True)

    # Remove everything after the first comma in the address column (keep only street)
    df['address'] = df['address'].apply(lambda x: x.split(',')[0])

    # Convert 'claimed' column to boolean datatype
    df['claimed'] = df['claimed'].apply(lambda x: True if x == 'Claimed' else False)
    
    # Convert 'awards' column strings to lists of strings, keeping NaN values
    df['awards'] = df['awards'].apply(
    lambda x: [award.strip() for award in x.split(',')] if pd.notna(x) and isinstance(x, str) else x
    )

    # Convert 'meals' column strings to lists of strings, keeping NaN values
    df['meals'] = df['meals'].apply(lambda x: [meal.strip() for meal in x.split(',')] if pd.notna(x) else x)

    # Convert 'cuisines' column strings to lists of strings, keeping NaN values
    df['cuisines'] = df['cuisines'].apply(lambda x: [cuisine.strip() for cuisine in x.split(',')] if pd.notna(x) else x)

    # Convert 'features' column strings to lists of strings, keeping NaN values
    df['features'] = df['features'].apply(lambda x: [feature.strip() for feature in x.split(',')] if pd.notna(x) else x)

    # Convert 'keywords' column strings to lists of strings, keeping NaN values
    df['keywords'] = df['keywords'].apply(lambda x: [keyword.strip() for keyword in x.split(',')] if pd.notna(x) else x)

    # Extract the rank as an integer from 'popularity_generic', preserving NaN values
    df['popularity_rnk_all_restaurants'] = df['popularity_generic'].str.extract(r'#(\d+)').astype(float)  # Extract as float to handle NaNs
    df['popularity_rnk_all_restaurants'] = df['popularity_rnk_all_restaurants'].astype('Int64')  # Convert to Int64 to preserve NaNs
    # Drop the old 'popularity_generic' column
    df = df.drop(columns=['popularity_generic'])

    # Extract 'eur_price_range_from' and 'eur_price_range_to'
    df[['eur_price_range_from', 'eur_price_range_to']] = df['price_range'].str.extract(r'€([\d,]+)-€([\d,]+)')
    # Remove commas and convert the extracted columns to float, keeping NaN values intact
    df['eur_price_range_from'] = df['eur_price_range_from'].str.replace(',', '').astype(float)
    df['eur_price_range_to'] = df['eur_price_range_to'].str.replace(',', '').astype(float)
    # Convert to integers while keeping NaN values
    df['eur_price_range_from'] = df['eur_price_range_from'].astype('Int64')
    df['eur_price_range_to'] = df['eur_price_range_to'].astype('Int64')
    # Drop the original 'price_range' column
    df.drop(columns=['price_range'], inplace=True)

    # Convert 'open_days_per_week', 'open_hours_per_week' and 'total_reviews_count' to integers while preserving NaN values
    df['open_days_per_week'] = df['open_days_per_week'].apply(lambda x: int(x) if pd.notna(x) else pd.NA)
    df['open_hours_per_week'] = df['open_hours_per_week'].apply(lambda x: int(x) if pd.notna(x) else pd.NA)
    df['total_reviews_count'] = df['total_reviews_count'].apply(lambda x: int(x) if pd.notna(x) else pd.NA)

    # Convert 'Y'/'N' to boolean values
    df['vegetarian_friendly'] = df['vegetarian_friendly'].map({'Y': True, 'N': False})
    df['vegan_options'] = df['vegan_options'].map({'Y': True, 'N': False})
    df['gluten_free'] = df['gluten_free'].map({'Y': True, 'N': False})

    # Reset index and set it as a column named 'restaurant_id'
    df.reset_index(drop=True, inplace=True) 
    df['restaurant_id'] = df.index + 1

    # Reorder the columns
    order = [
    'restaurant_id', 'restaurant_name', 'address', 'longitude', 'latitude', 'claimed',
    'avg_rating', 'total_reviews_count', 'popularity_rnk_all_restaurants', 'awards',
    'eur_price_range_from', 'eur_price_range_to', 'meals', 'cuisines', 'features',
    'vegetarian_friendly', 'vegan_options', 'gluten_free', 'open_days_per_week',
    'open_hours_per_week', 'original_open_hours', 'food', 'service', 'value',
    'atmosphere', 'tripadvisor_link', 'keywords'
    ]
    df = df.reindex(columns=order)

    # Create restaurant_dim table
    restaurant_dim_columns = [
    'restaurant_id', 'restaurant_name', 'claimed', 'awards', 'tripadvisor_link',
    'keywords', 'open_days_per_week', 'open_hours_per_week', 'original_open_hours'
    ]
    restaurant_dim = df[restaurant_dim_columns]

    # Create location_dim table
    location_dim_columns = ['restaurant_id', 'address', 'longitude', 'latitude']
    location_dim = df[location_dim_columns].drop_duplicates().reset_index(drop=True)
    location_dim['location_id'] = range(1, len(location_dim) + 1)
    location_dim = location_dim[['location_id', 'restaurant_id', 'address', 'longitude', 'latitude']]

    # Create price_range_dim table
    price_range_dim_columns = ['restaurant_id', 'eur_price_range_from', 'eur_price_range_to']
    price_range_dim = df[price_range_dim_columns].drop_duplicates().reset_index(drop=True)
    price_range_dim['price_range_id'] = range(1, len(price_range_dim) + 1)
    price_range_dim = price_range_dim[['price_range_id', 'restaurant_id', 'eur_price_range_from', 'eur_price_range_to']]

    # Convert list-like columns to tuples (to handle error from drop_duplicates())
    df['meals'] = df['meals'].apply(lambda x: tuple(x) if isinstance(x, list) else x)
    df['cuisines'] = df['cuisines'].apply(lambda x: tuple(x) if isinstance(x, list) else x)
    df['features'] = df['features'].apply(lambda x: tuple(x) if isinstance(x, list) else x)

    # Create attributes_dim table
    attributes_dim_columns = ['restaurant_id', 'meals', 'cuisines', 'features']
    attributes_dim = df[attributes_dim_columns].drop_duplicates().reset_index(drop=True)
    attributes_dim['attributes_id'] = range(1, len(attributes_dim) + 1)

    # Convert tuple columns back to lists and reorder
    attributes_dim['meals'] = attributes_dim['meals'].apply(lambda x: list(x) if isinstance(x, tuple) else x)
    attributes_dim['cuisines'] = attributes_dim['cuisines'].apply(lambda x: list(x) if isinstance(x, tuple) else x)
    attributes_dim['features'] = attributes_dim['features'].apply(lambda x: list(x) if isinstance(x, tuple) else x)
    attributes_dim = attributes_dim[['attributes_id', 'restaurant_id', 'meals', 'cuisines', 'features']]

    # Create dietary_dim table
    dietary_dim_columns = ['restaurant_id', 'vegetarian_friendly', 'vegan_options', 'gluten_free']
    dietary_dim = df[dietary_dim_columns].drop_duplicates().reset_index(drop=True)
    dietary_dim['dietary_id'] = range(1, len(dietary_dim) + 1)
    dietary_dim = dietary_dim[['dietary_id', 'restaurant_id', 'vegetarian_friendly', 'vegan_options', 'gluten_free']]

    # Create restaurant_reviews_fact table
    restaurant_reviews_fact = df[['restaurant_id', 'avg_rating', 'total_reviews_count', 'popularity_rnk_all_restaurants',
                                'food', 'service', 'value', 'atmosphere']]

    # Merge with other tables to get FKs
    restaurant_reviews_fact = restaurant_reviews_fact.merge(location_dim[['restaurant_id', 'location_id']],
                                                            on='restaurant_id', how='left')

    restaurant_reviews_fact = restaurant_reviews_fact.merge(price_range_dim[['restaurant_id', 'price_range_id']],
                                                            on='restaurant_id', how='left')

    restaurant_reviews_fact = restaurant_reviews_fact.merge(dietary_dim[['restaurant_id', 'dietary_id']],
                                                            on='restaurant_id', how='left')

    restaurant_reviews_fact = restaurant_reviews_fact.merge(attributes_dim[['restaurant_id', 'attributes_id']],
                                                            on='restaurant_id', how='left')

    # Assign review_id as a unique identifier for each row (PK)
    restaurant_reviews_fact['review_id'] = range(1, len(restaurant_reviews_fact) + 1)

    # Reorder the columns
    restaurant_reviews_fact = restaurant_reviews_fact[['review_id', 'restaurant_id', 'location_id', 'price_range_id', 
                                                    'attributes_id', 'dietary_id', 'avg_rating', 'total_reviews_count', 
                                                    'popularity_rnk_all_restaurants', 'food', 'service', 'value', 
                                                    'atmosphere']]

    # Ensure no duplicate rows exist in the fact table
    restaurant_reviews_fact = restaurant_reviews_fact.drop_duplicates().reset_index(drop=True)

    
    return {"restaurant_dim":restaurant_dim.to_dict(orient="dict"),
    "location_dim":location_dim.to_dict(orient="dict"),
    "price_range_dim":price_range_dim.to_dict(orient="dict"),
    "attributes_dim":attributes_dim.to_dict(orient="dict"),
    "dietary_dim":dietary_dim.to_dict(orient="dict"),
    "restaurant_reviews_fact":restaurant_reviews_fact.to_dict(orient="dict")
    }


@test
def test_output(output, *args) -> None:
    """
    Template code for testing the output of the block.
    """
    assert output is not None, 'The output is undefined'