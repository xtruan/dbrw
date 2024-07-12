import logging
import time

from dbrw import DbUtilities, create_connection

# removed prepared statement support, it's not needed
#from preparing_cursor import PreparingCursor

logger = logging.getLogger(__name__)

QUERY_SUFFIX = ":s"

class DbWriter:
    """DbWriter provides a simple way to insert dictionary-based data into the database.
    """

    def __init__(self, db, schema_name, auto_create_tables=False):
        """Initialization of DbWriter.
        """

        self.__db: DbUtilities = db
        """DbUtilities: database utility object for interacting with the database"""
        self.__schema_name: str = schema_name
        """str: schema or dataset to interact with"""
        self.__created_tables = []
        """list of str: names of tables which have been created so far"""
        # self.__table_to_insert_cursor_map = {}
        # """dict: names of tables which have been created so far mapped to cursors with a cached insert statement"""
        self.__table_to_statement_map = {}
        """dict: names of tables which have been created so far mapped to insert statements"""
        self.__auto_create_tables: bool = auto_create_tables
        """bool: flag to determine whether to auto-create required tables"""

        self.__cnn = self.__db.get_connection()
        """connection: database connection"""

        # re-create schema in auto mode
        # if auto_create_tables:
        #     if self.__schema_name in self.__db.get_all_schema_names():
        #         self.__db.drop_schema(self.__schema_name)
        #     self.__db.create_schema(self.__schema_name)

    def __create_table(self, table_name, table_row):
        self.__db.create_table_from_values(self.__schema_name, table_name, table_row)
        self.__created_tables.append(table_name)

    def __prepare_insert_statement(self, table_name, table_row):
        try:
            statement = self.__db.build_table_insert_statement(self.__schema_name, table_name, table_row)
            self.__table_to_statement_map[table_name + QUERY_SUFFIX] = statement

            # removed prepared statement support, it's not needed
            #insert_cursor = self.__cnn.cursor(cursor_factory=PreparingCursor)
            #insert_cursor.prepare(statement)
            #logger.debug('prepared: ' + statement)
            
            # no longer using a cursor per table, just create a new one each time
            #insert_cursor = self.__cnn.cursor()
            #self.__table_to_insert_cursor_map[table_name] = insert_cursor

        except Exception:
            logger.error('failed to prepare: ' + statement)

    def write_table_data(self, table_data, max_attempts=5):
        """Writes the given table values to their specified tables
        {
         "table1" : 
            [
                {
                    "fieldA" : 0,
                    "fieldB" : "foo"
                },
                {
                    "fieldA" : 1,
                    "fieldB" : "bar"
                },
            ]
         "table2" :
            [
                {
                    "fieldC" : true
                }
            ]
        }
        would insert into table1 a row with fieldA set to 0 and fieldB set to 'foo', 
        another row with fieldA set to 1 and fieldB set to 'bar', and into table2 a row 
        with fieldC set to true.
        """
        for table_name in table_data:
            attempts = 0
            while attempts <= max_attempts:
                attempts = attempts + 1
                try:
                    table_rows = table_data[table_name]
                    num_rows = len(table_rows)
                    if self.__auto_create_tables and table_name not in self.__created_tables and num_rows > 0:
                        self.__create_table(table_name, table_rows[0])
                    if table_name + QUERY_SUFFIX not in self.__table_to_statement_map and num_rows > 0:
                        self.__prepare_insert_statement(table_name, table_rows[0])
                    
                    # TODO: no longer using a cursor per table, just create a new one each time
                    #insert_cursor = self.__table_to_insert_cursor_map.get(table_name)
                    
                    # if this is the last attempt, create a new connection to the database
                    if (attempts == max_attempts):
                        self.__cnn = create_connection(self.__db.get_dsn())

                    with self.__cnn.cursor() as insert_cursor:
                    
                        #self.begin_transaction(insert_cursor)
                        
                        values = []
                        values_placeholder_str = ""
                        for table_row in table_rows:
                            cols = table_row.keys()
                            if values_placeholder_str == "":
                                values_placeholder = ['%s'] * len(cols)
                                values_placeholder_str = "(" + ','.join(values_placeholder) + ")"
                            
                            row_values = [table_row[col] for col in cols]
                            # mogrify and then replace is the fastest way to do this, faster than
                            # other methods including binding parameters
                            row_values_mogrify = insert_cursor.mogrify(values_placeholder_str, tuple(row_values))
                            values.append(row_values_mogrify)

                        # join values
                        values_str = b','.join(values)

                        # build final statement by replacing placeholder with real values
                        statement = self.__table_to_statement_map.get(table_name + QUERY_SUFFIX)
                        final_statement = statement.replace("VALUES " + values_placeholder_str, "VALUES " + values_str.decode("utf-8"))

                        insert_cursor.execute(final_statement)
                        self.__cnn.commit()

                        #self.commit_transaction(insert_cursor)

                        # if this is the last attempt, clean up the new connection to the database
                        if (attempts == max_attempts and self.__cnn and self.__cnn.closed == 0):
                            self.__cnn.close()
                    
                    logger.debug('inserted ' + str(num_rows) + ' rows into: ' + table_name)
                    return True
                except Exception as e:
                    if attempts <= max_attempts:
                        # logger.warn('failed to insert ' + str(num_rows) + ' rows into: ' + table_name + ' on attempt: ' + str(attempts))
                        if self.__cnn and self.__cnn.closed == 0:
                            self.__cnn.rollback()
                        # logger.warn(e.__cause__)
                        # logger.warn(e.__str__())
                        time.sleep(1)
                    else:
                        logger.error('failed to insert ' + str(num_rows) + ' rows into: ' + table_name)
                        logger.error(e, exc_info=True)

        return False


    def begin_transaction(self, cursor):
        cursor.execute('BEGIN TRANSACTION;')

    def commit_transaction(self, cursor):
        cursor.execute('COMMIT TRANSACTION;')