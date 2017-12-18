import logging
import pandas as pd
import sqlalchemy
from abc import ABC, abstractmethod


class BaseDBConnection(ABC):
    """
    Base class for Database connections
    """

    def __init__(self, url, **kwargs):
        """
        Initialize the connection using sqlalchemy url.
        :param url: sqlchemy url for DB engine creation.
        :param kwargs: sqlachemy kwargs
        """
        self.engine = sqlalchemy.create_engine(url, **kwargs)

    def get_pandas_df(self, sql_to_run, lowercase_colnames=False):
        """
        Runs the sql select statement provided and returns a pandas dataframe.
        :param sql_to_run: sql statement to run
        :param lowercase_colnames: Bool represent if dataframe column names have to be lowercase. Default False
        :return:
        """
        logging.info(sql_to_run)
        df = pd.read_sql_query(sql_to_run, self.engine)
        if lowercase_colnames:
            df.columns = [x.lower() for x in df.columns]
        return df

    def execute_sql(self, sql_to_run, throw_exception=True, force_single_query=False, auto_commit=True):
        """
        Executes SQL statement.
        :param sql_to_run: SQL statement to execute
        :param throw_exception: Force exception on failure. Default True
        :param force_single_query: Execute as a single statement even if
                there are multiple SQL statments separated by `;`.
        :param auto_commit: Automatically commits statement after execution
        :return:
        """
        # Split on semicolons assuming multiple sql statements.
        with self.engine.connect() as conn:
            if not force_single_query:
                for sub_query in sql_to_run.split(';'):
                    if not sub_query == '':
                        self.__execute_single_sql(sub_query, throw_exception, conn, auto_commit)

            # Don't split. In case you are inserting sql into a table, for example.
            else:
                self.__execute_single_sql(sql_to_run, throw_exception, conn, auto_commit)

    def __execute_single_sql(self, sql_to_run, throw_exception, connection, auto_commit):
        try:
            logging.info(sql_to_run)
            connection.execute(sqlalchemy.text(sql_to_run))
            if auto_commit is True:
                connection.execute("COMMIT;")
        except Exception as e:
            try:
                connection.execute('rollback;')
            except Exception:
                pass
            if throw_exception:
                raise ValueError("Unable to execute: {}\nBecause: {}".format(sql_to_run, e))

    def close(self):
        pass

    @abstractmethod
    def get_table_column_definition(self, db_object_name):
        """
        Database object description.
        :param db_object_name: table or view name
        """
        return



