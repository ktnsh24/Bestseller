import logging
import pandas as pd
import json
import sys

from configuration import read_config

logger = logging.getLogger(__name__)
config = read_config()


def transform_date(dataset: pd.DataFrame, columns: list) -> pd.DataFrame:
    """
    function to change column to datetime datatype.
    :param dataset: Pandas Dataframe
    :param columns: list of columns
    :return: Pandas dataframe
    """
    for column in columns:
        try:
            dataset[column] = pd.to_datetime(dataset[column], unit='s')
        except Exception as E:
            logger.error(E)
            logger.error(f"column {column} data type is failed to convert to datetime")
            sys.exit()
    return dataset


def column_lowercase(dataset: pd.DataFrame) -> pd.DataFrame:
    """
    function to convert columns name into lowercase.
    :param dataset: Pandas Dataframe
    :return: Pandas dataframe
    """
    dataset.columns = map(str.lower, dataset.columns)
    return dataset


def json_dumps(dataset: pd.DataFrame, columns: list) -> pd.DataFrame:
    """
    function to convert dict into json.
    :param dataset: Pandas Dataframe
    :param columns: List of columns .
    :return: Pandas dataframe
    """
    for column in columns:
        try:
            dataset[column] = dataset[column].apply(json.dumps)
        except Exception:
            logger.error(f"column {column} can not be converted to json")
            pass
    return dataset


def flatten_list(dataset: pd.DataFrame, columns: list) -> pd.DataFrame:
    """
    function to convert list from nested list.
    :param dataset: Pandas Dataframe
    :param columns: List of columns .
    :return: Pandas dataframe
    """
    for column in columns:
        try:
            dataset[column] = dataset[column].apply(lambda x: [item for sublist in x for item in sublist])
        except Exception as E:
            # logger.error(E)
            logger.error(f"column {column} can not be flatten to list")
            sys.exit()
    return dataset

