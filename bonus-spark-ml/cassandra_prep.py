from cassandra.cluster import Cluster
from cassandra.auth import PlainTextAuthProvider
auth_provider = PlainTextAuthProvider(username='cassandra', password='cassandra')
cluster = Cluster(auth_provider=auth_provider)
cluster = Cluster(['localhost'], auth_provider=auth_provider)
session = cluster.connect('telegram_data')



create_channels = "CREATE TABLE IF NOT EXISTS channels (sender_username text, send_date text,send_hour text,message_id text, \
                        PRIMARY KEY ((sender_username),send_date,send_hour,message_id))"
session.execute(create_channels)


create_hashtags_Context = "CREATE TABLE IF NOT EXISTS hashtags_context2 (hashtag text, send_date text,context text,message_id text, \
                        PRIMARY KEY ((hashtag),send_date,context,message_id))"
session.execute(create_hashtags_Context)



# create_hashtags_Context = "CREATE TABLE IF NOT EXISTS hashtags_context3 (hashtag text, send_date text,context text,message_id text, \
#                         PRIMARY KEY ((hashtag),send_date,context,message_id))"
# session.execute(create_hashtags_Context)
