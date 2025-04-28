import os
import logging
import pandas as pd
from sqlalchemy import create_engine
from sqlalchemy.engine import URL
import nuhs_lst.utils.log_utils as lu
import nuhs_lst.utils.exception_utils as eu

# Initialize logger
logger = logging.getLogger(__name__)


class DBManager:
    """Handles database connections and queries for MSSQL.

    This class manages the connection to the MSSQL database and provides methods to execute SQL queries.
    It also includes methods for handling exceptions based on configured settings.
    """

    def __init__(self, args):
        """Initializes DBManager with connection parameters.

        Args:
            args (dict): Dictionary containing database connection information such as user,
                password, host, database name, and port.
        """
        connection_url = URL.create(
            "mssql+pymssql",
            username=args["DB_USER"],
            password=args["DB_PASSWORD"],
            host=args["DB_URL"],
            database=args["DB_NAME"],
            port=args["DB_PORT"],
        )

        self.engine = create_engine(connection_url, pool_size=4, max_overflow=10)

        # Settings to throw errors strictly or with leniency
        self.strict_with_exceptions = (
            args.get("DB_STRICT_WITH_EXCEPTIONS", "false").lower() == "true"
        )

        logger.debug("I AM IN DB!!!")
        logger.info(
            f'DB is {"STRICT " + lu.get_angry_sign() if self.strict_with_exceptions else "CHILL "}WITH EXCEPTIONS'
        )

    def execute(self, query, data=None, verbose=True):
        """Executes a SQL query on the database.

        Args:
            query (str): The SQL query to execute.
            data (dict or None): The data to bind to the query, if applicable.
            verbose (bool): If True, logs the query and data.

        Returns:
            None if successful, or handles exception if an error occurs.
        """
        if verbose:
            logger.debug(query)
            logger.debug(data)

        try:
            with self.engine.connect() as conn:
                logger.debug("Executing query")
                cursor = conn.execute(query, data) if data else conn.execute(query)
                logger.debug(f"Number of rows affected: {cursor.rowcount}")
        except Exception as e:
            return self._handle_exception(query, data, e)

    def execute_to_dataframe(self, query, params=None, verbose=True):
        """Executes a query and returns the result as a pandas DataFrame.

        Args:
            query (str): The SQL query to execute.
            params (dict or None): The parameters to bind to the query, if applicable.
            verbose (bool): If True, logs the query and parameters.

        Returns:
            pd.DataFrame or None: The resulting DataFrame, or None if an error occurred.
        """
        try:
            with self.engine.connect() as conn:
                if verbose:
                    logger.debug(query)
                    logger.debug(params)
                cursor = conn.execute(query, params) if params else conn.execute(query)
                df = pd.DataFrame(cursor.fetchall())
                if df.empty:
                    return None
                df.columns = cursor.keys()
                logger.debug(f"Resulting DataFrame shape: {df.shape}")
                return df
        except Exception as e:
            return self._handle_exception(query, params, e)
        return None

    def _handle_exception(self, query, data, e):
        """Handles exceptions that occur during query execution.

        Logs the exception and exits the program if configured to do so.

        Args:
            query (str): The SQL query that caused the error.
            data (dict or None): The data passed with the query.
            e (Exception): The exception that was raised.

        Returns:
            None
        """
        logger.debug(query)
        logger.debug(data)
        logger.error(f"Exception occurred: {eu.get_exception_string(e)}")

        if self.strict_with_exceptions:
            logger.error(f"DB Error. Exiting program \n {lu.get_explosion_cloud()}")
            import sys

            sys.exit(1)

        return None

    def execute_count_query(self, query, params=None):
        """Executes a count query and returns the result.

        Args:
            query (str): The SQL query to execute.
            params (dict or None): The parameters to bind to the query, if applicable.

        Returns:
            int: The result of the count query.
        """
        with self.engine.connect() as conn:
            logger.debug(query)
            logger.debug(params)
            cursor = (
                conn.execute(query, params).fetchone()[0]
                if params
                else conn.execute(query).fetchone()[0]
            )
            return cursor
