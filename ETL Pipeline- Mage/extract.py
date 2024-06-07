import io
import requests
import pandas as pd

if 'data_loader' not in globals():
    from mage_ai.data_preparation.decorators import data_loader

if 'test' not in globals():
    from mage_ai.data_preparation.decorators import test

@data_loader
def load_api_data(*args, **kwargs):
    """
    Load data from an API
    """
    url = 'https://storage.googleapis.com/ubervision-etl/dataset.csv'
    response = requests.get(url)
    return pd.read_csv(io.StringIO(response.text), sep=',')

@test
def testing_output(output, *args) -> None:
    """
    Test the output of the function
    """
    assert output is not None, 'The output of the function is not defined.'