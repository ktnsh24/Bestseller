import logging
import pandas as pd
import gzip
import glob
from typing import Dict

logger = logging.getLogger(__name__)


def read_json_file(path) -> pd.DataFrame:
    try:
        logger.info("Trying to read JSON file")
        json_df = pd.read_json(path, orient='records', lines=True)
        return json_df
    except Exception as E:
        logger.error("Failed to read JSON file")
        raise E


def parse(path) -> Dict:
    """
    function to open the gzip file.
    :param path: Path of the files to unzip
    :return: dtypes dictionary
    """
    files = glob.glob(path)
    for file in files:
        logger.info(f"Reading {file} file")
        g = gzip.open(file, 'rb')
        for l in g:
            yield eval(l)
    logger.info("file is read successfully")


def getDF(path) -> pd.DataFrame:
    """
    function to read the unzipped file.
    :param path: Path of the files to unzip
    :return: pandas dataframe
    """

    i = 0
    df = {}
    try:
        logger.info("Trying to read zip file")
        for d in parse(path):
            df[i] = d
            if i == 100:
                break
            i += 1
        return pd.DataFrame.from_dict(df, orient='index')
    except Exception as E:
        logger.error("Failed to read zip file")
        raise E

