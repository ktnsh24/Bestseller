import pandas as pd
import logging
import sqlalchemy
from sqlalchemy import create_engine

from configuration import read_config

logger = logging.getLogger(__name__)
config = read_config()


def create_db_connection() -> sqlalchemy.engine.base.Engine:
    """
    Create a database connection using sqlalchemy's create_engine.
    Username, Password, db and Host needs to be set using environment parameters.
    :return: Engine object
    """

    try:
        engine = create_engine(
            f"postgresql://{config['PGUSER']}:{config['PGPASSWORD']}@{config['PGHOST']}"
            f":{config['PORT']}/{config['DATABASE']}")
        logger.info("Connection successfully established")
        return engine
    except Exception as E:
        logger.error("Connection with database could not be established")
        raise E


def execute_sql(dataset: pd.DataFrame, table_path: str, mode: str, table_name: str, database_schema: str) -> None:
    """
    Execute a sql statement from the a file
    and return the results as dictionary
    :param dataset: data to write
    :param table_path:  file path that needs to be executed
    :param mode: append or replace
    :param table_name: name of the table
    :param database_schema: name of the schema
    :param dtype: dictionary
    """
    engine = create_db_connection()
    try:
        if mode == 'append':
            result = check_if_table_exists(table_name, database_schema)
            for row in result:
                if row[0] == 0:
                    fd = open(table_path, 'r')
                    escaped_sql = sqlalchemy.text(fd.read())
                    logger.info(f"sql file is {table_name}")
                    engine.execute(escaped_sql)
                else:
                    logger.info(f"Table {table_name} already exists")
            write_to_db(engine, dataset, table_name, database_schema)
    except Exception as E:
        logger.error(E)
        raise E


def write_to_db(engine: sqlalchemy.engine.base.Engine, dataset: pd.DataFrame,
                table_name: str, database_schema: str) -> None:
    """
    write the final dataframe to database table
    :param engine: Postgres sql engine
    :param dataset: pandas dataframe
    :param table_name: name of the table
    :param database_schema: name of the schema
    :return: None
    """
    try:
        logger.info(f'starting to write data in the table {table_name} in postgres')
        dataset.to_sql(table_name,
                        engine,
                        method='multi',
                        schema=database_schema,
                        chunksize=1000,
                        if_exists='append',
                        index=False)
        logger.info(f'finished writing data in the table {table_name} in postgres')
    except Exception as e:
        logger.exception(e)
        logger.error("failed to write data in SQLServer")
        raise


def check_if_table_exists(table_name: str, database_schema: str) -> list:
    """
    :param table_name: name of the table
    :param database_schema: name of the schema
    :return: list
    """
    engine = create_db_connection()
    result = engine.execute(
        f"""
                        SELECT COUNT(*)
                        FROM information_schema.tables
                        WHERE table_name = '{table_name}' AND
                        table_schema = '{database_schema}'
                        """
    )
    return result


def execute_model(filename: str) -> None:
    """
    Execute a sql statement from the a file
    and return the results as dictionary
    :param filename: name of the file that needs to be executed
    :return: None
    """
    engine = create_db_connection()
    fd = open(filename, 'r')
    escaped_sql = sqlalchemy.text(fd.read())
    logger.info(f"sql model is {filename}")

    with engine.connect() as connection:
        connection.execute(escaped_sql)