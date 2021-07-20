from kafka import KafkaConsumer
from datetime import datetime, timedelta
from elasticsearch import Elasticsearch
import json
from cassandra.cluster import Cluster
from cassandra.auth import PlainTextAuthProvider

auth_provider = PlainTextAuthProvider(username='cassandra', password='cassandra')
cluster = Cluster(auth_provider=auth_provider)
cluster = Cluster(['localhost'], auth_provider=auth_provider)
session = cluster.connect('telegram_data')


class DataTransport:
    # TOPIC_NAME = "quickstart-events"
    # BOOTSTRAP_SERVERS = "localhost:9092"
    TOPIC_NAME = "preProcessedData"
    INDEX_NAME = "telegram-data"
    BOOTSTRAP_SERVERS = "185.236.37.254:9092"

    def __init__(self):
        self._es = Elasticsearch()
        self._consumer = KafkaConsumer(DataTransport.TOPIC_NAME, bootstrap_servers=DataTransport.BOOTSTRAP_SERVERS)

    def start_loop(self):
        for msg in self._consumer:
            try:
                msg_value = msg.value.decode('utf-8')
                msg_value = json.loads(msg_value)
                # because of UTC datetime
                timeSubtract = timedelta(minutes=270)
                msg_value["timestamp"] = datetime.now() - timeSubtract
                self._es.index(index=DataTransport.INDEX_NAME, body=msg_value)
                self._es.indices.refresh(index=DataTransport.INDEX_NAME)
                print(msg_value)

                # print(message_id)
                # print(send_date)
                # print(send_hour)
                # print(sender_id)
                # print(hashtags)
                # print(context)


                msg_value["timestamp"] = datetime.now()
                message_id = str(msg_value['id'])
                send_date = str(msg_value['timestamp'])
                send_hour = str(msg_value['timestamp'].hour)
                sender_username = str(msg_value['sender_username'])
                context = str(msg_value['context'])
                hashtags = msg_value['hashtags']
                # print(send_date)

                query = "INSERT INTO channels(sender_username,send_date,send_hour,message_id) VALUES (?,?,?,?)"
                prepared = session.prepare(query)
                session.execute(prepared, (sender_username, send_date, send_hour, message_id))
                if len(hashtags) != 0:
                    for hashtag in hashtags:
                        query = "INSERT INTO hashtags(hashtag,send_date,send_hour,message_id) VALUES (?,?,?,?)"
                        prepared = session.prepare(query)
                        session.execute(prepared, (str(hashtag), send_date, send_hour, message_id))
                        # query = "INSERT INTO hashtags_context3(hashtag,send_date,context,message_id) VALUES (?,?,?,?)"
                        # prepared = session.prepare(query)
                        # session.execute(prepared, (str(hashtag), send_date, context, message_id))

            except:
                print("here")
                continue


def delete_index(self):
    self._es.indices.delete(index=DataTransport.INDEX_NAME, ignore=[400, 404])
