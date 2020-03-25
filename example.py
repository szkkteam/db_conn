import os

from pypika import Query, Table, Field, enums

from db_conn.query.sc_soccer import tables

import db_conn as conn
matches = [
177566,
   177567,
   177568,
   177569
]
#q = conn.query.sc_soccer.get_all_match_data(match_ids=[177566, 177567])
#q = conn.query.sc_soccer.get_player_lineups(matches)
#print(str(q))

configuration = {
    'ssh_address_or_host': (os.environ.get('SSH_ADDRESS'), 22),
    'ssh_username': os.environ.get('SSH_USERNAME'),
    'ssh_password': os.environ.get('SSH_PASSWORD'),
    'remote_bind_address': ('127.0.0.1', 5432),
    'local_bind_address': ('127.0.0.1', 8080),  # could be any available port
}
tunnel = conn.utils.Tunnel(config=configuration)
configuration = {
    'db_username': os.environ.get('DB_USERNAME'),
    'db_password': os.environ.get('DB_PASSWORD'),
    'db_address': ('127.0.0.1', 8080),
    'db_name': 'sc_soccer'
}
db = conn.psql.Connection(config=configuration, tunnel=tunnel)

df = conn.query.sc_soccer.get_combined_data(db, match_ids=matches)
print(df)

db.terminate()
