import os
import pytest
import db_conn as conn

SSH_CONFIGURATION = {
    'ssh_address_or_host': (os.environ.get('SSH_ADDRESS'), 22),
    'ssh_username': os.environ.get('SSH_USERNAME'),
    'ssh_password': os.environ.get('SSH_PASSWORD'),
    'remote_bind_address': ('127.0.0.1', 5432),
    'local_bind_address': ('127.0.0.1', 8080),  # could be any available port
}

DB_CONFIGURATION = {
    'db_username': os.environ.get('DB_USERNAME'),
    'db_password': os.environ.get('DB_PASSOWRD'),
    'db_address': ('127.0.0.1', 8080),
    'db_name': os.environ.get('DB_NAME')
}

@pytest.fixture
def setup_tunnel():
    return conn.utils.Tunnel(config=SSH_CONFIGURATION)

def test_single_connection_fetch(setup_tunnel):

    db = conn.psql.Connection(config=DB_CONFIGURATION, tunnel=setup_tunnel)
    with db.get_cursor() as c:
        c.execute("select match_id from matches limit 1;")
        res = c.fetchall()
        assert len(res) > 0


def get_connection(tunnel):
    return conn.psql.Connection(config=DB_CONFIGURATION, tunnel=tunnel)

def get_connection_pool(tunnel):
    return conn.psql.ConnectionPool(config=DB_CONFIGURATION, tunnel=tunnel)

# Test specifications
def test_single_connection_pandas_query(setup_tunnel):
    try:
        import pandas as pd
    except Exception as err:
        print("Test skipped. No pandas module installed")
    else:
        db = get_connection(setup_tunnel)
        df = db.sql_query("select match_id from matches limit 1;")
        assert len(df.index) == 1

def test_pool_connection_pandas_query(setup_tunnel):
    try:
        import pandas as pd
    except Exception as err:
        print("Test skipped. No pandas module installed")
    else:
        db = get_connection_pool(setup_tunnel)
        df = db.sql_query("select match_id from matches limit 1;")
        assert len(df.index) == 1
