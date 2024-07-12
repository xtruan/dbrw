import logging

from dbrw import DbUtilities

logger = logging.getLogger(__name__)

CACHE_MAX_ROWS = 10000
"""int: maximum number of rows allowed in the cache"""

class DbReader:
    """DbReader provides a way to safely iterate over arbitrary sized database tables without memory issues.
    """

    def __init__(self, db, schema_name, table_name, where_clause=None, sort_column=None, sort_ascending=True, as_dataframe=False):
        """Initialization of DbReader.
        """

        self.__db: DbUtilities = db
        """DbUtilities: database utility object for interacting with the database"""
        self.__schema_name: str = schema_name
        """str: schema or dataset to interact with"""
        self.__table_name: str = table_name
        """str: table or view to interact with"""
        self.__sort_column: str = sort_column
        """str: column to sort by"""
        self.__sort_ascending: bool = sort_ascending
        """bool: True to sort ASCending, False to sort DESCending"""
        self.__where_clause: str = where_clause
        """str: where clause to filter rows"""
        self.__as_dataframe: bool = as_dataframe
        """bool: True to return as pandas dataframe, False to return as dict"""

    def __iter__(self):
        """Initialization of iterator.

            Returns:
                DbReader: instance of DbReader
        """

        # fill inital cache from DB

        self.__row_count = self.__db.get_table_row_count(self.__schema_name, self.__table_name, self.__where_clause)
        """int: total number of rows in table"""
        self.__cache = self.__db.get_table_data(self.__schema_name, self.__table_name, self.__where_clause, self.__sort_column, self.__sort_ascending, CACHE_MAX_ROWS, 0, self.__as_dataframe)
        """list of dict: cache of database rows"""
        self.__cache_min = 0
        """int: cache lower bound"""
        self.__cache_max = len(self.__cache) - 1
        """int: cache upper bound"""
        self.__data_pos = 0
        """int: current data position"""

        return self

    def __next__(self):
        """Get next value of iterator.

            Returns:
                dict: current database row key/value pairs
        """

        # check if we're at the end
        if self.__data_pos >= self.__row_count:
            raise StopIteration

        row = {}
        if self.__data_pos >= self.__cache_min and self.__data_pos <= self.__cache_max:
            # if current row is cached, just return it
            if self.__as_dataframe:
                row = self.__cache.iloc[self.__data_pos - self.__cache_min]
            else:
                row = self.__cache[self.__data_pos - self.__cache_min]
        else:
            # if current row is not cached, need to cache next set of rows
            self.__cache = self.__db.get_table_data(self.__schema_name, self.__table_name,  self.__where_clause, self.__sort_column, self.__sort_ascending, CACHE_MAX_ROWS, self.__data_pos, self.__as_dataframe)
            self.__cache_min = self.__data_pos
            self.__cache_max = self.__data_pos + len(self.__cache) - 1
            if self.__as_dataframe:
                row = self.__cache.iloc[self.__data_pos - self.__cache_min]
            else:
                row = self.__cache[self.__data_pos - self.__cache_min]
            
        # increment the position
        self.__data_pos += 1
        return row

    