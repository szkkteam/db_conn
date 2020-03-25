# Common Python library imports
from functools import wraps
from contextlib import contextmanager
from concurrent.futures import ThreadPoolExecutor
import time
import os


# Pip package imports
import psycopg2
from psycopg2.pool import ThreadedConnectionPool
from loguru import logger

# Internal package imports
from db_conn.utils import get_nested, Singleton, Tunnel


class Connection():

    config = {
        'db_username': '',
        'db_password': '',
        'db_address': ('127.0.0.1', 8080),
        'db_name': os.environ.get('DB_NAME')
    }

    def __init__(self, *args, **kwargs):

        self._config = { **Connection.config , **kwargs.get('config', {})}
        self._tunnel = kwargs.get('tunnel', None)
        self._cursor = None

        username = os.environ.get('DB_USERNAME', kwargs.get('username', self._get_config('db_username')))
        password = os.environ.get('DB_PASSWORD', kwargs.get('password', self._get_config('db_password')))

        # Open the tunnel if give, to establish the database connection.
        self._open_tunnel()
        # The connection has to be started first to get the local bind address
        if self._tunnel is not None:
            assert isinstance(self._tunnel, Tunnel), "Input argument \'tunnel\' must be a type of Tunnel object"
            self._config['db_address'] = self._tunnel.local_bind_addresses[0]

        self._connection = self._create_connection(username, password)

    def __del__(self):
        try:
            self.terminate()
        except Exception:
            pass

    @property
    def connection(self):
        return self._connection

    @contextmanager
    def get_cursor(self, **kwargs):
        connection = kwargs.get('connection', self._connection)
        commit = kwargs.get('commit', False)
        c = connection.cursor()
        try:
            yield c
        except psycopg2.IntegrityError:
            # logger.error(err)
            connection.rollback()
        finally:
            # Code to release resource, e.g.:
            if commit:
                connection.commit()


    def sql_query(self, query):
        try:
            import pandas as pd
            return pd.read_sql_query(str(query), self._connection)
        except ImportError as err:
            logger.error(err)

    @staticmethod
    def s_sql_query(connection, query):
        try:
            import pandas as pd
            return pd.read_sql_query(str(query), connection)
        except ImportError as err:
            logger.error(err)

    def close(self):
        self._connection.close()

    def terminate(self):
        logger.debug("Terminating connection")
        # Close all DB connection
        try:
            self.close()
        except Exception as err:
            logger.error(err)
        # Close the tunnel also
        self._close_tunnel()

    def restart(self):
        if self._tunnel is not None:
            self._tunnel.restart()

    def _create_connection(self, username, password):
        return psycopg2.connect(user=username,
                         password=password,
                         host=self._get_config('db_address')[0],
                         port=self._get_config('db_address')[1],
                         database=self._get_config('db_name'))

    def _get_config(self, *args):
        data = get_nested(self._config, *args)
        assert data is not None, "config \'%s\' is not supported." % args
        return data

    def _open_tunnel(self):
        if self._tunnel is not None:
            self._tunnel.start()
            time.sleep(10)

    def _close_tunnel(self):
        if self._tunnel is not None:
            self._tunnel.stop()

class ConnectionPool(Connection, metaclass=Singleton):

    config = {
        'db_username': '',
        'db_password': '',
        'db_address': ('127.0.0.1', 8080),
        'db_name': os.environ.get('DB_NAME'),
        'min_connection': 1,
        'max_connection': 800
    }

    def __init__(self, *args, **kwargs):

        kwargs['config'] = { **ConnectionPool.config, **kwargs.get('config',{}) }

        super(ConnectionPool, self).__init__(*args, **kwargs)


    def _create_connection(self, username, password):
        return ThreadedConnectionPool(minconn=self._get_config('min_connection'),
                                      maxconn=self._get_config('max_connection'),
                                      user=username,
                                      password=password,
                                      host=self._get_config('db_address')[0],
                                      port=self._get_config('db_address')[1],
                                      database=self._get_config('db_name'))

    @contextmanager
    def get_cursor(self, **kwargs):
        connection = kwargs.get('connection', None)
        commit = kwargs.get('commit', False)
        if connection is None:
            with self.get_connection() as connection:
                c = connection.cursor()
                try:
                    yield c
                except psycopg2.IntegrityError:
                    # logger.error(err)
                    connection.rollback()
                finally:
                    # Code to release resource, e.g.:
                    if commit:
                        connection.commit()
        else:
            c = connection.cursor()
            try:
                yield c
            except psycopg2.IntegrityError:
                # logger.error(err)
                connection.rollback()
            else:
                # Code to release resource, e.g.:
                if commit:
                    connection.commit()

    def sql_query(self, query):
        try:
            import pandas as pd
            with self.get_connection() as conn:
                return pd.read_sql_query(str(query), conn)
        except ImportError as err:
            logger.error(err)

    @contextmanager
    def get_connection(self):
        conn = self._connection.getconn()
        # TODO: Test code
        #conn.autocommit = False
        try:
            yield conn
        finally:
            # Code to release resource, e.g.:
            self._connection.putconn(conn)

    def close(self):
        self._connection.closeall()

def db_session(username, password):
    def deco_session(f):
        @wraps(f)
        def f_connect(*args, **kwargs):
            db = Connection(username=username,
                            password=password,
                            tunnel=Tunnel(username=username,
                                          password=password))

            with db.get_cursor() as curr:
                kwargs['cursor'] = curr
                kwargs['connection'] = db.connection
                return f(*args, **kwargs)

        return f_connect  # true decorator
    return deco_session

