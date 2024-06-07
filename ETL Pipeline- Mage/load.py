from mage_ai.data_preparation.repo_manager import get_repo_path
from mage_ai.io.bigquery import BigQuery
from mage_ai.io.config import ConfigFileLoader
import pandas as pd
from pandas import DataFrame
from os import path

if 'data_exporter' not in globals():
    from mage_ai.data_preparation.decorators import data_exporter

@data_exporter
def export_to_big_query(data, **kwargs) -> None:
    """
    This is the template to export the data to the BigQuery warehouse.
    """

    config_path = path.join(get_repo_path(), 'io_config.yaml')
    config_profile = 'default'

    for key, val in data.items():
        table_id = 'ubervision-etl.{}'.format(key)
        BigQuery.with_config(ConfigFileLoader(config_path, config_profile)).export(
            DataFrame(val),
            table_id,
            if_exists= 'replace' # the table is replaced if the name already exists
        )
        