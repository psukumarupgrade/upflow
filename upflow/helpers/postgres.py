from smart_open import smart_open
import six
from contextlib import closing
from airflow.hooks.postgres_hook import PostgresHook as AirflowPostgresHook
import logging
from airflow.models import BaseOperator
from airflow.utils.decorators import apply_defaults
from upflow.helpers.file import remove_access_key_from_uri


class PostgresHook(AirflowPostgresHook):
    """
    Interact with Postgres.
    You can specify ssl parameters in the extra field of your connection
    as ``{"sslmode": "require", "sslcert": "/path/to/cert.pem", etc}``.
    """
    conn_name_attr = 'postgres_conn_id'
    default_conn_name = 'postgres_default'
    supports_autocommit = True

    def __init__(self, *args, **kwargs):
        super(PostgresHook, self).__init__(*args, **kwargs)

    @classmethod
    def escape_quote(cls, s):
        return s.replace('\'', '\'\'')

    def bulk_load(self, uri, table_name, columns=(), load_options=None):
        """
        :param uri: Path of the file.
                    For s3 files: s3://{aws_key}:{aws_secret_key}@{path}

        :param table_name:
        :param columns:
        :param load_options: load_options is a dictionary that represents the postgres copy options
                             Refer to https://www.postgresql.org/docs/9.1/static/sql-copy.html for details
                                {
                                    FORMAT format_name,
                                    OIDS [ boolean ],
                                    DELIMITER 'delimiter_character',
                                    NULL 'null_string',
                                    HEADER [ boolean ],
                                    QUOTE 'quote_character',
                                    ESCAPE 'escape_character',
                                    FORCE_QUOTE { ( column [, ...] ) | * },
                                    FORCE_NOT_NULL ( column [, ...] ),
                                    ENCODING 'encoding_name'
                                }
        :return:
        """
        table_name = table_name.upper()
        with smart_open(uri) as fh:
            self._copy_from(fh, table_name, columns, load_options)

    def bulk_dump(self, uri, source, load_options=None):
        """
        :param uri: Path of the file to load into
                    For s3 files: s3://{aws_key}:{aws_secret_key}@{path}

        :param source: source table or sql
        :param load_options: load_options is a dictionary that represents the postgres copy options
                             Refer to https://www.postgresql.org/docs/9.1/static/sql-copy.html for details
                                {
                                    FORMAT format_name,
                                    OIDS [ boolean ],
                                    DELIMITER 'delimiter_character',
                                    NULL 'null_string',
                                    HEADER [ boolean ],
                                    QUOTE 'quote_character',
                                    ESCAPE 'escape_character',
                                    FORCE_QUOTE { ( column [, ...] ) | * },
                                    FORCE_NOT_NULL ( column [, ...] ),
                                    ENCODING 'encoding_name'
                                }
        :return: None
        """
        with smart_open(uri, 'wb') as fh:
            self._copy_to(source, fh, load_options)

    def get_pandas_df(self, sql, lowercase_columns=False, log_sql=True, parameters=None):
        """
        Runs the sql select statement provided and returns a pandas dataframe.
        :param sql: sql statement to run
        :param lowercase_columns: Bool represent if dataframe column names have to be lowercase. Default False
        :param log_sql: logs the sql to run if True. Default is True
        :param parameters: The parameters to render the SQL query with.
        :return:
        """
        if log_sql:
            logging.info(sql)
        df = super(PostgresHook, self).get_pandas_df(sql=sql, parameters=parameters)
        if lowercase_columns:
            df.columns = [x.lower() for x in df.columns]
        return df

    def get_table_column_definition(self, db_object_name, column_list=None, skip_column_list=None):
        """
        Database object description.
        :param db_object_name: table or view name
        :param column_list: list of column names to include in the result
        :param skip_column_list: list of column names to filter out
        :return: pandas dataframe view object description
        """

        database = None
        schema = None
        table = None

        db_object_name_obj = db_object_name.split(".")
        if len(db_object_name_obj) == 3:
            database = db_object_name_obj[0].lower()
            schema = db_object_name_obj[1].lower()
            table = db_object_name_obj[2].lower()
        if len(db_object_name_obj) == 2:
            schema = db_object_name_obj[0].lower()
            table = db_object_name_obj[1].lower()
        if len(db_object_name_obj) == 1:
            table = db_object_name_obj[0].lower()

        base_sql = """
            select 
                upper(column_name) as name, 
                upper(data_type) as type, 
                'COLUMN' as kind, 
                (case when lower(is_nullable) = 'no' 
                        THEN 'N' 
                      when lower(is_nullable) = 'yes' 
                        then 'Y' else null 
                  end) as "null?", 
                column_default as "default", 
                null as "primary key", 
                null as "unique key", 
                null as check, 
                null as expression
            from information_schema.columns where  
        """
        # remove new lines and tabs from string
        base_sql = ' '.join(base_sql.split())
        if table:
            base_sql += " table_name = '{}'".format(table)
        if schema:
            base_sql += " and table_schema = '{}'".format(schema)
        if database:
            base_sql += " and table_catalog = '{}'".format(database)

        df = self.get_pandas_df(base_sql, lowercase_columns=True)
        skip_column_list = [col.lower() for col in skip_column_list or []]
        if not column_list:
            return df[~df['name'].isin(skip_column_list)]
        else:
            column_list = [col.upper() for col in column_list]
            return df[df['name'].isin(column_list)][~df['name'].isin(skip_column_list)]

    def _copy_to(self, source, dest, flags):
        """
        Export a table or query into a file.
        Will open a separate raw connection to the database for loading and close it.
        The copy operation will be committed and the raw connection closes.
        :param source: table or sql
        :param dest: Destination file handler(not file path), in write mode
        :param flags: keyword options passed through to COPY.
                        Refer to http://www.postgresql.org/docs/9.6/static/sql-copy.html


        Examples: ::
            table = "mytable"
            with open('/tmp/file.tsv', 'w') as fp:
                copy_to(table, fp, conn, format='csv')
        """
        if " " in source:
            source = "(" + source + ")"
        formatted_flags = '({})'.format(self.format_flags(flags)) if flags else ''
        copy_sql = 'COPY {0} TO STDOUT {1}'.format(source, formatted_flags)
        with closing(self.get_conn()) as conn:
            with closing(conn.cursor()) as cursor:
                cursor.copy_expert(copy_sql, dest)

    def _copy_from(self, source, dest, columns=(), flags=None):
        """Import a table from a file. For flags, see the PostgreSQL documentation
        at http://www.postgresql.org/docs/9.5/static/sql-copy.html.

        :param source: Source file pointer, in read mode
        :param dest: relation name such as table, view or sql statement
        :param self.engine: SQLAlchemy engine, connection, or raw_connection
        :param columns: Optional tuple of columns
        :param **flags: Options passed through to COPY

         Examples: ::
            with open('/path/to/file.tsv') as fp:
                copy_from(fp, MyTable, conn)

            with open('/path/to/file.csv') as fp:
                copy_from(fp, MyModel, engine, format='csv')

        If an existing connection is passed to `self.engine`, it is the caller's
        responsibility to commit and close.

        The `columns` flag can be set to a tuple of strings to specify the column
        order. Passing `header` alone will not handle out of order columns, it simply tells
        postgres to ignore the first line of `source`.
        """
        formatted_columns = '({})'.format(','.join(columns)) if columns else ''
        formatted_flags = '({})'.format(self.format_flags(flags)) if flags else ''
        copy_sql = 'COPY {} {} FROM STDIN {}'.format(
            dest,
            formatted_columns,
            formatted_flags,
        )
        with closing(self.get_conn()) as conn:
            with closing(conn.cursor()) as cursor:
                cursor.copy_expert(copy_sql, dest)

    def format_flags(self, flags):
        return ', '.join(
            '{} {}'.format(key.upper(), self.format_flag(value))
            for key, value in flags.items()
        )

    def format_flag(self, value):
        return (
            six.text_type(value).upper()
            if isinstance(value, bool)
            else repr(value)
        )

    @staticmethod
    def _serialize_cell(cell, conn):
        """
        Postgresql will adapt all arguments to the execute() method internally,
        hence we return cell without any conversion.
        See http://initd.org/psycopg/docs/advanced.html#adapting-new-types for
        more information.
        :param cell: The cell to insert into the table
        :type cell: object
        :param conn: The database connection
        :type conn: connection object
        :return: The cell
        :rtype: object
        """
        return cell


class PostgresRunSQLOperator(BaseOperator):
    """
    Executes sql code in a specific Postgres database

    :param postgres_conn_id: reference to a specific postgres database
    :type postgres_conn_id: string
    :param sql: the sql code to be executed
    :type sql: Can receive a str representing a sql statement,
        a list of str (sql statements), or reference to a template file.
        Template reference are recognized by str ending in '.sql'
    :param database: name of database which overwrite defined one in connection
    :type database: string
    """

    template_fields = ('sql',)
    template_ext = ('.sql',)
    ui_color = '#ededed'

    @apply_defaults
    def __init__(
            self, sql,
            postgres_conn_id=None,
            autocommit=False,
            parameters=None,
            database=None,
            *args, **kwargs):
        super(PostgresRunSQLOperator, self).__init__(*args, **kwargs)
        self.sql = sql
        self.postgres_conn_id = postgres_conn_id
        self.autocommit = autocommit
        self.parameters = parameters
        self.database = database

    def execute(self, context):
        logging.info('Executing: %s', self.sql)
        hook = PostgresHook(postgres_conn_id=self.postgres_conn_id,
                            schema=self.database)
        hook.run(self.sql, self.autocommit, parameters=self.parameters)


class PostgresUnloaderOperator(BaseOperator):
    """
    Executes sql code in a specific Postgres database

    :param postgres_conn_id: reference to a specific postgres database
    :type postgres_conn_id: string
    :param uri: Path of the file to load into
                For s3 files: s3://{aws_key}:{aws_secret_key}@{path}
    :type uri: string
    :param source: source table or sql
    :type source: string
    :param load_options: load_options is a dictionary that represents the postgres copy options
    :type load_options:  dictionary of keys and values.
                         Refer to https://www.postgresql.org/docs/9.1/static/sql-copy.html for details
                        {
                            FORMAT format_name,
                            OIDS [ boolean ],
                            DELIMITER 'delimiter_character',
                            NULL 'null_string',
                            HEADER [ boolean ],
                            QUOTE 'quote_character',
                            ESCAPE 'escape_character',
                            FORCE_QUOTE { ( column [, ...] ) | * },
                            FORCE_NOT_NULL ( column [, ...] ),
                            ENCODING 'encoding_name'
                        }
    :param database: name of database which overwrite defined one in connection
    :type database: string
    """

    ui_color = '#ededed'

    @apply_defaults
    def __init__(
            self, uri,
            source,
            load_options=None,
            postgres_conn_id=None,
            autocommit=True,
            database=None,
            *args, **kwargs):
        super(PostgresUnloaderOperator, self).__init__(*args, **kwargs)
        self.uri = uri
        self.source = source
        self.load_options = load_options
        self.postgres_conn_id = postgres_conn_id
        self.autocommit = autocommit
        self.database = database

    def execute(self, context):
        logging.info('Unloading data from source: %s', self.source)
        hook = PostgresHook(postgres_conn_id=self.postgres_conn_id,
                            schema=self.database)
        hook.bulk_dump(uri=self.uri, source=self.source, load_options=self.load_options)
        logging.info('Loaded data into target: %s', self.uri)
        logging.info('Loaded data into target: %s', remove_access_key_from_uri(self.uri))

