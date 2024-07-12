import logging
import os

from dbrw import DbUtilities, create_connection_pool, DbReader, DbWriter

logger = logging.getLogger(__name__)

class DbSession:

    def __init__(self, host=None, port=None, dbname=None, user=None, password=None, poolsize=None):
        self.__host = host
        self.__port = port
        self.__dbname = dbname
        self.__user = user
        self.__password = password
        self.db_poolsize = poolsize

        if self.__host is None:
            self.__host = os.getenv("DBRW_PGHOST", "localhost")
        if self.__port is None:
            self.__port = int(os.getenv("DBRW_PGPORT", "5432"))
        if self.__dbname is None:
            self.__dbname = os.getenv("DBRW_PGDBNAME", "postgres")
        if self.__user is None:
           self.__user = os.getenv("DBRW_PGUSER", "postgres")
        if self.__password is None:
            self.__password = os.getenv("DBRW_PGPASSWORD", "")
        if self.db_poolsize is None:
            self.db_poolsize = int(os.getenv("DBRW_PGPOOLSIZE", "10"))

        self.dsn = "host='{0}' port='{1}' dbname='{2}' user='{3}' password='{4}'".format(
            self.__host,
            str(self.__port),
            self.__dbname,
            self.__user,
            self.__password
        )
        self.db_pool = create_connection_pool(self.dsn, 1, self.db_poolsize)

        logger.info("db host='{0}' port='{1}' dbname='{2}' user='{3}' password='{4}'".format(
            self.__host,
            str(self.__port),
            self.__dbname,
            self.__user,
            '*' * len(self.__password)
        ))
        logger.info("db pool size: {0}".format(str(self.db_poolsize)))

    def get_db_reader(self, schema, table, where, sort_col, sort_dir):
        db_conn = self.db_pool.getconn()
        db_reader = DbReader(DbUtilities(self.dsn, db_conn), schema, table, where, sort_col, sort_dir)
        try:
            yield db_reader
        finally:
            db_reader.close()
            self.db_pool.putconn(db_conn)

    def get_db_writer(self, schema):
        db_conn = self.db_pool.getconn()
        db_writer = DbWriter(DbUtilities(self.dsn, db_conn), schema, auto_create_tables=False)
        try:
            yield db_writer
        finally:
            self.db_pool.putconn(db_conn)
        
    def get_db(self):
        db_conn = self.db_pool.getconn()
        db = DbUtilities(self.dsn, db_conn)
        try:
            yield db
        finally:
            self.db_pool.putconn(db_conn)