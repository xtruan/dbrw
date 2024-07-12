import logging
import datetime

import psycopg2 as pg
from psycopg2.pool import ThreadedConnectionPool
import pandas as pd

# from preparing_cursor import PreparingCursor

logger = logging.getLogger(__name__)

SEQUENCE_COL_NAME = 'seq'

def get_connection_kwargs():
    """Get keyword arguments for psycopg2 connection.
    """
    connection_kwargs = {
        "connect_timeout": 300,
        "keepalives": 1,
        "keepalives_idle": 60,
        "keepalives_interval": 10,
        "keepalives_count": 5
    }
    return connection_kwargs

def create_connection_pool(dsn, min_conn=1, max_conn=10):
    """Create connection pool.
    """
    kwargs = get_connection_kwargs()
    return ThreadedConnectionPool(min_conn, max_conn, dsn, **kwargs)

def create_connection(dsn):
    """Create connection.
    """
    kwargs = get_connection_kwargs()
    return pg.connect(dsn, **kwargs)

def escape_id(identifier):
        # semicolon for breaking up statements, period for scoping, equals sign, inline comment
        naively_cleaned_identifier = identifier.replace(";", "").replace(".", "").replace("=", "").replace("--", "")
        # start with double quote
        escaped = '"'
        for char in naively_cleaned_identifier:
            if char == '"':
                # double the double quotes to escape
                escaped = escaped + char
            escaped = escaped + char
        # end with double quote
        return escaped + '"'   

def escape_li(literal):
    escaped = ""
    for char in literal:
        if char == "'":
            # double the single quotes to escape
            escaped = escaped + char
        escaped = escaped + char
    return escaped

def double_quote(identifier):
    return '"{}"'.format(identifier)

def single_quote(literal):
    return "'{}'".format(literal)

class DbUtilities:
    """Database utility class for interacting with PostgreSQL database.
    """

    def __init__(self, dsn, db_conn=None):
        """Initialize DbUtilities.
        """

        self.__dsn: str = dsn
        """str: database driver connection string."""

        self.__db_conn = db_conn
        """str: database driver connection."""

        self.__is_internal_db_conn: bool = False
        """bool: if db_conn was created internally it should be closed with the close function"""
    
    def get_connection(self):
        if self.__db_conn is None:
            self.__db_conn = create_connection(self.__dsn)
            self.__is_internal_db_conn = True
        return self.__db_conn
    
    def close_connection(self):
        if self.__db_conn is not None and self.__is_internal_db_conn:
            self.__db_conn.close()
    
    def get_dsn(self):
        return self.__dsn

    def execute_list(self, queries=[], params=None):
        """Execute list of SQL queries in order.

                Args:
                    queries (list): SQL query
                    params (list): list of parameters for query
        """
        with self.get_connection() as cnn:
                # cur = cnn.cursor(cursor_factory=PreparingCursor)
                cur = cnn.cursor()
                count = 0
                for query in queries:
                    sql = str(query)
                    cur.execute(sql, params)
                    count += 1
                cnn.commit()
                cur.close()
                logger.info('executed ' + str(count) + ' queries')

    def execute_modify(self, query, params=None):
        return self.execute(query, params, as_dataframe=False, return_results=False)

    def execute(self, query, params=None, as_dataframe=False, return_results=True):
        """Execute SQL query.

                Args:
                    query (str): SQL query
                    params (list): list of parameters for query
                    as_dataframe (bool): if True, return as Pandas dataframe, if False as list of dict

                Returns:
                    list of dict: object containing query results
        """
        if return_results == False:
            as_dataframe = False

        if as_dataframe:
            data = pd.DataFrame()
        else: 
            data = []

        try:
            with self.get_connection() as cnn:
                # cur = cnn.cursor(cursor_factory=PreparingCursor)
                cur = cnn.cursor()
                sql = str(query)

                if as_dataframe:
                    data = pd.read_sql(sql, cnn, coerce_float=True, params=params)
                else:
                    cur.execute(sql, params)
                    if return_results:
                        desc = cur.description
                        column_names = [col[0] for col in desc]
                        data = [dict(zip(column_names, row)) for row in cur.fetchall()]
                    else:
                        cnn.commit()
                
                cur.close()
                logger.info('executed: ' + ' '.join(query.split()))
        except Exception:
            logger.error('failed to execute: ' + ' '.join(query.split()))

        if return_results:
            return data
        else:
            return None

    def drop_schema(self, schema_name):
        drop_sql = "DROP SCHEMA IF EXISTS " + escape_id(schema_name) + " CASCADE;"
        self.execute_modify(drop_sql)

    def drop_table(self, schema_name, table_name):
        drop_sql = "DROP TABLE IF EXISTS " + escape_id(schema_name) + "." + escape_id(table_name) + ";"
        self.execute_modify(drop_sql)

    def drop_view(self, schema_name, view_name):
        drop_sql = "DROP VIEW IF EXISTS " + escape_id(schema_name) + "." + escape_id(view_name) + ";"
        self.execute_modify(drop_sql)

    def create_schema(self, schema_name, overwrite_existing=False):
        if overwrite_existing:
            self.drop_schema(schema_name)
        create_sql = "CREATE SCHEMA IF NOT EXISTS " + escape_id(schema_name) + ";"
        return self.execute_modify(create_sql)

    def build_table_insert_statement(self, table_name, table_data_row):
        return self.build_table_insert_statement(None, table_name, table_data_row)
    
    def build_table_insert_statement(self, schema_name, table_name, table_data_row):
        insert_sql = "INSERT INTO "
        if (schema_name is not None):
            insert_sql += escape_id(schema_name) + "."
        insert_sql += escape_id(table_name)

        columns_list = []
        for col_name in table_data_row:
            columns_list.append(escape_id(col_name))
        columns_sql = " (" + ','.join(columns_list) + ")"
        values_list = ['%s'] * len(columns_list)
        values_sql = " VALUES (" + ','.join(values_list) + ");"

        return insert_sql + columns_sql + values_sql

    def create_table_from_values(self, schema_name, table_name, table_data_row, overwite_existing=False, create_sequence_col=True):
        table_definition = {}
        for col_name in table_data_row:
            col_value = table_data_row[col_name]
            if isinstance(col_value, datetime.datetime):
                table_definition[col_name] = 'timestamptz'
            if isinstance(col_value, bool):
                table_definition[col_name] = 'bool'
            elif isinstance(col_value, int):
                table_definition[col_name] = 'bigint'
            elif isinstance(col_value, float):
                table_definition[col_name] = 'double precision'
            elif isinstance(col_value, bytes):
                table_definition[col_name] = 'bytea'
            #elif isinstance(self.try_cast(col_value, float), float):
            #    table_definition[col_name] = 'float8'
            else:
                table_definition[col_name] = 'text'
                
        self.create_table(schema_name, table_name, table_definition, overwite_existing, create_sequence_col)

    def try_cast(self, val, to_type):
        try:
            return to_type(val)
        except (ValueError, TypeError):
            logger.info('could not cast ' + str(val) + " to " + str(to_type))
            return val

    def create_table(self, schema_name, table_name, table_definition, overwite_existing=False, create_sequence_col=True, primary_key_col=SEQUENCE_COL_NAME):
        # check if the provided schema name matches one that actually exists in the database
        if schema_name not in self.get_all_schema_names():
            self.create_schema(schema_name, overwrite_existing=False)
        if overwite_existing:
            self.drop_table(schema_name, table_name)
        create_sql = "CREATE TABLE IF NOT EXISTS " + escape_id(schema_name) + "." + escape_id(table_name) + " ("
        # create a bigserial to track the sequence
        if create_sequence_col:
            create_sql += escape_id(SEQUENCE_COL_NAME) + " bigserial,"
        primary_key_col_is_valid = False
        for col_name in table_definition:
            # check if the provided primary key column is valid
            if col_name == primary_key_col:
                primary_key_col_is_valid = True
            col_type = table_definition[col_name]
            create_sql += escape_id(col_name) + " " + col_type + ","
        create_sql = create_sql[:-1] + ");"
        self.execute_modify(create_sql)
        # add primary key to sequence column if we created it
        if primary_key_col_is_valid:
            self.add_primary_key_to_table(schema_name, table_name, primary_key_col)
        elif create_sequence_col:
            self.add_primary_key_to_table(schema_name, table_name, SEQUENCE_COL_NAME)

    def create_view(self, schema_name, view_name, view_sql, overwite_existing=False):
        # check if the provided schema name matches one that actually exists in the database
        if schema_name not in self.get_all_schema_names():
            self.create_schema(schema_name, overwrite_existing=False)
        if overwite_existing:
            self.drop_view(schema_name, view_name)
        create_sql = "CREATE OR REPLACE VIEW " + escape_id(schema_name) + "." + escape_id(view_name) + " AS "
        create_sql = create_sql + view_sql + ";"
        self.execute_modify(create_sql)

    def create_index_on_table(self, schema_name, table_name, column_name):
        # check if the provided schema name matches one that actually exists in the database
        if schema_name not in self.get_all_schema_names():
            return False
        # check if the provided table name matches one that actually exists in the schema
        if table_name not in self.get_all_tables_in_schema(schema_name):
            return False 
        # check if the provided column name matches one that actually exists in the table
        if column_name not in self.get_all_columns_in_table(schema_name, table_name):
            return False

        index_name = escape_id(table_name + "_" + column_name + "_idx")
        create_sql = "CREATE INDEX " + index_name + " ON " + escape_id(schema_name) + "." + escape_id(table_name) + " (" + escape_id(column_name) + ");"
        self.execute_modify(create_sql)

    def add_primary_key_to_table(self, schema_name, table_name, column_name):
        # check if the provided schema name matches one that actually exists in the database
        if schema_name not in self.get_all_schema_names():
            return False
        # check if the provided table name matches one that actually exists in the schema
        if table_name not in self.get_all_tables_in_schema(schema_name):
            return False 
        # check if the provided column name matches one that actually exists in the table
        if column_name not in self.get_all_columns_in_table(schema_name, table_name):
            return False

        alter_sql = "ALTER TABLE " + escape_id(schema_name) + "." + escape_id(table_name) + " ADD PRIMARY KEY (" + escape_id(column_name) + ");"
        self.execute_modify(alter_sql)

    def get_table_data(self, schema_name, table_name, where_clause=None, sort_column=None, sort_ascending=True, limit=None, offset=None, as_dataframe=False):
        if sort_column is not None:
            return self.get_sorted_table_data(schema_name, table_name, [sort_column], [sort_ascending], where_clause, limit, offset, as_dataframe)
        else:
            return self.get_sorted_table_data(schema_name, table_name, [], [], where_clause, limit, offset, as_dataframe)

    def get_sorted_table_data(self, schema_name, table_name, sort_columns, sort_ascendings, where_clause=None, limit=None, offset=None, as_dataframe=False):
        where_sql = ""
        if where_clause != None:
            # semicolon for breaking up statements, inline comment
            naively_cleaned_where_clause = where_clause.replace(";", "").replace("--", "")
            where_sql = "WHERE " + naively_cleaned_where_clause
        
        sort_sql = ""
        if len(sort_columns) > 0 and len(sort_columns) == len(sort_ascendings):
            sort_sql = "ORDER BY "
            sort_items = []
            for i in range(len(sort_columns)):
                sort_item = escape_id(sort_columns[i]) + " "
                if sort_ascendings[i] == True:
                    sort_item = sort_item + "ASC"
                else:
                    sort_item = sort_item + "DESC"
                sort_items.append(sort_item)
            sort_sql = sort_sql + ",".join(sort_items) + " "
        
        limit_sql = ""
        if limit != None:
            limit_sql = "LIMIT " + str(int(limit)) + " "

        offset_sql = ""
        if offset != None:
            offset_sql = "OFFSET " + str(int(offset)) + " "

        return self.execute(
            "SELECT * FROM " + escape_id(schema_name) + "." + escape_id(table_name) + " " 
            + where_sql + " " + sort_sql + " " + limit_sql + " " + offset_sql + ";",
            None, as_dataframe
        )

    def get_table_row_count(self, schema_name, table_name, where_clause=None):
        where_sql = ""
        if where_clause != None:
            # semicolon for breaking up statements, inline comment
            naively_cleaned_where_clause = where_clause.replace(";", "").replace("--", "")
            where_sql = "WHERE " + naively_cleaned_where_clause
        return self.flatten_data((self.execute(
            "SELECT COUNT(1) AS row_count FROM " + escape_id(schema_name) + "." + escape_id(table_name) + " "
            + where_sql + ";",
            None, False)), ["row_count"]
        )[0]

    def get_all_schema_names(self):
        return self.flatten_data(self.execute(
            "SELECT schema_name FROM information_schema.schemata \
            ORDER BY schema_name;"), ["schema_name"]
        )
    
    def get_all_tables_in_schema(self, schema_name, include_views=True):
        view_sql = ""
        if include_views == False:
            view_sql = "AND table_type='BASE TABLE' "

        return self.flatten_data(self.execute(
            "SELECT table_name FROM information_schema.tables \
            WHERE table_schema=(%s) "
            + view_sql +
            "ORDER BY table_name;", [schema_name], False), ["table_name"]
        )

    def get_all_columns_in_table(self, schema_name, table_name):
        return self.flatten_data(self.execute(
            "SELECT column_name FROM information_schema.columns \
            WHERE table_schema=(%s) \
            AND table_name=(%s) \
            ORDER BY column_name;", [schema_name, table_name], False), ["column_name"])

    def does_relation_exist(self, schema_name, table_name):
        relation = escape_id(schema_name) + '.' + escape_id(table_name)
        data = self.execute("SELECT to_regclass((%s)) IS NOT NULL AS exists", [relation], False)
        return data != []

    def flatten_data(self, data, fields):
        flattened_data = []
        for item in data:
            for field in fields:
                flattened_data.append(item.get(field))
        return flattened_data
