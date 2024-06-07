import pandas as pd

if 'transformer' not in globals():
    from mage_ai.data_preparation.decorators import transformer

if 'test' not in globals():
    from mage_ai.data_preparation.decorators import test

@transformer
def transform(df, *args, **kwargs):
    # creating the transformation logic
    # converting columns to datetime
    df['tpep_pickup_datetime'] = pd.to_datetime(df['tpep_pickup_datetime'])
    df['tpep_dropoff_datetime'] = pd.to_datetime(df['tpep_dropoff_datetime'])

    # extracting components from the pickup and dropoff date and time
    datetime_dimension = df[['tpep_pickup_datetime', 'tpep_dropoff_datetime']].drop_duplicates().reset_index(drop=True)
    datetime_dimension['pick_hour'] = datetime_dimension['tpep_pickup_datetime'].dt.hour
    datetime_dimension['drop_hour'] = datetime_dimension['tpep_dropoff_datetime'].dt.hour
    datetime_dimension['pick_day'] = datetime_dimension['tpep_pickup_datetime'].dt.day
    datetime_dimension['drop_day'] = datetime_dimension['tpep_dropoff_datetime'].dt.day
    datetime_dimension['pick_month'] = datetime_dimension['tpep_pickup_datetime'].dt.month
    datetime_dimension['drop_month'] = datetime_dimension['tpep_dropoff_datetime'].dt.month
    datetime_dimension['pick_year'] = datetime_dimension['tpep_pickup_datetime'].dt.year
    datetime_dimension['drop_year'] = datetime_dimension['tpep_dropoff_datetime'].dt.year
    datetime_dimension['pick_weekday'] = datetime_dimension['tpep_pickup_datetime'].dt.weekday
    datetime_dimension['drop_weekday'] = datetime_dimension['tpep_dropoff_datetime'].dt.weekday
    datetime_dimension['datetime_id'] = datetime_dimension.index

    datetime_dimension = datetime_dimension[[
    'datetime_id', 'tpep_pickup_datetime','pick_hour', 'pick_day', 'pick_month', 'pick_year', 'pick_weekday',
    'tpep_dropoff_datetime', 'drop_hour', 'drop_day', 'drop_month', 'drop_year', 'drop_weekday'
    ]]

    # extracting components from the passenger details
    passenger_dimension = df[['passenger_count']].drop_duplicates().reset_index(drop=True)
    passenger_dimension['passenger_id'] = passenger_dimension.index

    passenger_dimension = passenger_dimension[[
        'passenger_id', 'passenger_count'
    ]]

    # extracting components from the trip distance
    trip_distance_dimension = df[['trip_distance']].drop_duplicates().reset_index(drop=True)
    trip_distance_dimension['trip_distance_id'] = trip_distance_dimension.index

    trip_distance_dimension = trip_distance_dimension[[
        'trip_distance_id', 'trip_distance'
    ]]

    # declaring an array to store the rate codes and extracting components from it
    rate_codes = {
    1:'Standard rate',
    2:'JFK',
    3:'Newark',
    4:'Nassau or Westchester',
    5:'Negotiated fare',
    6:'Group ride',
    }

    rate_code_dimension = df[['RatecodeID']].drop_duplicates().reset_index(drop=True)
    rate_code_dimension['rate_code_name'] = rate_code_dimension['RatecodeID'].map(rate_codes)
    rate_code_dimension['rate_code_id'] = rate_code_dimension.index

    rate_code_dimension = rate_code_dimension[[
        'rate_code_id', 'RatecodeID', 'rate_code_name'
    ]]

    # extracting components from the pickup location details
    pickup_location_dimension = df[['pickup_longitude', 'pickup_latitude']].drop_duplicates().reset_index(drop=True)
    pickup_location_dimension['pickup_location_id'] = pickup_location_dimension.index

    pickup_location_dimension = pickup_location_dimension[[
        'pickup_location_id', 'pickup_latitude', 'pickup_longitude'
    ]]

    # extracting details from the dropoff location details
    dropoff_location_dimension = df[['dropoff_longitude', 'dropoff_latitude']].drop_duplicates().reset_index(drop=True)
    dropoff_location_dimension['dropoff_location_id'] = dropoff_location_dimension.index

    dropoff_location_dimension = dropoff_location_dimension[[
        'dropoff_location_id', 'dropoff_latitude', 'dropoff_longitude'
    ]]

    # storing the payment options in an array and extracting the components
    payment_types = {
    1:'Credit card',
    2:'Cash',
    3:'No charge',
    4:'Dispute',
    5:'Unknown',
    6:'Voided trip',
    }

    payment_type_dimension = df[['payment_type']].drop_duplicates().reset_index(drop=True)
    payment_type_dimension['payment_type_name'] = payment_type_dimension['payment_type'].map(payment_types)
    payment_type_dimension['payment_type_id'] = payment_type_dimension.index

    payment_type_dimension = payment_type_dimension[[
        'payment_type_id', 'payment_type', 'payment_type_name'
    ]]

    # creating the fact table using outer join to keep all the records
    fact_table = df.merge(passenger_dimension, left_on='trip_id', right_on='passenger_id', how='left')\
        .merge(trip_distance_dimension, left_on='trip_id', right_on='trip_distance_id', how='left')\
        .merge(rate_code_dimension, left_on='trip_id', right_on='rate_code_id', how='left')\
        .merge(pickup_location_dimension, left_on='trip_id', right_on='pickup_location_id', how='left')\
        .merge(dropoff_location_dimension, left_on='trip_id', right_on='dropoff_location_id', how='left')\
        .merge(datetime_dimension, left_on='trip_id', right_on='datetime_id', how='left')\
        .merge(payment_type_dimension, left_on='trip_id', right_on='payment_type_id', how='left')\
        [['trip_id', 'VendorID', 'datetime_id', 'passenger_id',
        'trip_distance_id', 'rate_code_id', 'store_and_fwd_flag','pickup_location_id', 'dropoff_location_id',
        'payment_type_id', 'fare_amount', 'extra', 'mta_tax', 'tip_amount', 'tolls_amount', 'improvement_surcharge',
        'total_amount'
        ]]
    
    # sepcifying the values to be returned in the form of a dictionary
    return {
        "datetime_dimension":datetime_dimension.to_dict(orient='dict'),
        'passenger_dimension':passenger_dimension.to_dict(orient='dict'),
        'trip_distance_dimension':trip_distance_dimension.to_dict(orient='dict'),
        'rate_code_dimension':rate_code_dimension.to_dict(orient='dict'),
        'pikcup_location_dimension':pickup_location_dimension.to_dict(orient='dict'),
        'dropoff_location_dimension':dropoff_location_dimension.to_dict(orient='dict'),
        'payment_type_dimension':payment_type_dimension.to_dict(orient='dict')
    }

@test
def test_output(output, *args) -> None:
    '''
    Format for testing the output of the code block
    '''
    assert output is not None, 'The output of the function is not defined.'