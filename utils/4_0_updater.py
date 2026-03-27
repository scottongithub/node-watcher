import sqlite3


node_watcher_db = "./node-watcher.db"
conn = sqlite3.connect( node_watcher_db )
db_conn = conn.cursor()


# 'CREATE TABLE IF NOT EXISTS subscriptions(node_ip TEXT, advertised_router TEXT, metric INT, subscribers TEXT DEFAULT (json_array()) NOT NULL, 
#  UNIQUE(node_ip,advertised_router,metric))'


db_conn.execute('SELECT * from subscriptions WHERE node_ip IS NOT NULL')
print("\n\nBEFORE")
for row in db_conn.fetchall():
	print(row)
db_conn.execute('CREATE TABLE IF NOT EXISTS subscriptions_new(node_ip TEXT, advertised_router TEXT, metric INT, subscribers TEXT DEFAULT (json_array()) NOT NULL, UNIQUE(node_ip,advertised_router,metric))')
db_conn.execute('INSERT INTO subscriptions_new (node_ip, subscribers) SELECT node_ip, subscribers FROM subscriptions')
db_conn.execute('UPDATE subscriptions_new SET advertised_router = "none" WHERE node_ip NOT NULL')
db_conn.execute('UPDATE subscriptions_new SET metric = -1 WHERE node_ip NOT NULL')
db_conn.execute('DROP TABLE subscriptions')
db_conn.execute('ALTER TABLE subscriptions_new RENAME TO subscriptions')
conn.commit()

db_conn.execute('SELECT * from subscriptions WHERE node_ip IS NOT NULL')
print("\n\nAFTER")
for row in db_conn.fetchall():
	print(row)