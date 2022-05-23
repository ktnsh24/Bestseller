import pandas as pd
import logging
import os

from configuration import read_config
from datareader import getDF
from download import download_s3_data, move_object
from transform import transform_date, column_lowercase, json_dumps, flatten_list
from delete import delete_folder
from connections import execute_sql, execute_model

LOG_LEVEL = os.environ.get("LOG_LEVEL", "INFO")
logger = logging.getLogger(__name__)

config = read_config()
review_setting = config['data'].get('review_data', {})
metadata_setting = config['data'].get('meta_data', {})
dimensional_setting = config['data'].get('dimensional_model', {})


def _setup_logging() -> None:
    """
    Setting up logging level and format.
    :return: None
    """
    if logger.handlers:
        for handler in logger.handlers:
            logger.removeHandler(handler)

    logging.basicConfig(
        level=LOG_LEVEL,
        format="%(asctime)s - %(name)s - %(levelname)s - %(message)s",
        force=True
    )


def write_data_to_database(data: pd.DataFrame, settings) -> bool:
    """
    high level function to write a data frame to a database.

    :param data: Pandas Dataframe
    :param settings: Settings object containing source properties.
    :return: Success bool
    """
    table_path = settings.get('create_table_query_path', {})
    table_name = settings.get('table_name', {})
    mode = settings.get('mode', {})
    database_schema = settings.get('database_schema', {})

    try:
        execute_sql(data,table_path,mode,table_name,database_schema)
        return True, 'No Error'
    except Exception as E:
        logger.error(E)
        return False, E


def read_write_review_data() -> None:
    """
    high level function to read, transform, and write review data
    :return: None
    """
    review_df = getDF(review_setting.get('path', {}))
    logger.info('transforming review data file')
    review_df = column_lowercase(review_df)
    review_df = transform_date(review_df, review_setting.get('columns_to_transform_date', {}))
    logger.info('writing review data file to db')
    write_data_to_database(review_df, review_setting)
    return


def read_write_metadata() -> None:
    """
    high level function to read, transform, and write metadata
    :return: None
    """
    logger.info(f'Reading meta data file')
    product_df = getDF(metadata_setting.get('path', {}))
    logger.info('transforming meta data file')
    product_df = column_lowercase(product_df)
    product_df = json_dumps(product_df, metadata_setting.get('columns_to_json', {}))
    product_df = flatten_list(product_df, metadata_setting.get('columns_to_flat', {}))
    logger.info('writing meta data file to db')
    write_data_to_database(product_df, metadata_setting)
    return


def create_dimensional_model() -> bool:
    """
    high level function to execute sql queries of dimensional model
    :return: True if success, else False
    """
    table_path = dimensional_setting
    logger.info(f'crating dimensional table')
    try:
        for table in table_path:
            execute_model(table)
        return True, 'No Error'
    except Exception as E:
        logger.error(E)
        return False, E


def main():
    _setup_logging()
    """
    Open and read the file as a single buffer
    download_s3_data: The function will download the data in from the s3 bucket into sources folder(specified in the config.yaml file)
    read_write_review_data: The function will read, transform, and load the review data to postgres sql db
    read_write_metadata: The function will read, transform, and load the review data to postgres sql db
    create_dimensional_model: The function will create the dimensional model (star schema)
    delete_folder: The function will delete the sources folder once the file are loaded
    move_object: The function will archived the data in the s3 folder
    """
    download_s3_data()
    read_write_review_data()
    read_write_metadata()
    create_dimensional_model()
    delete_folder()
    move_object()
    return


# Press the green button in the gutter to run the script.
if __name__ == '__main__':
    main()


