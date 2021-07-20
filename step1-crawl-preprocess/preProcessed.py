#!/usr/bin/env python
# coding: utf-8




# !wget https://github.com/sobhe/hazm/releases/download/v0.5/resources-0.5.zip
# !mkdir resource
# !unzip resources-0.5.zip -d resource/


import configparser
import json
config = configparser.ConfigParser()
config.read("config.ini")
api_id = config['Telegram']['api_id']
api_hash = config['Telegram']['api_hash']
api_hash = str(api_hash)
phone = config['Telegram']['phone']
username = config['Telegram']['username']


stop_words = []

with open('stop_words.txt', "r") as f:
    for line in f:
        stop_words.extend(line.split())


print(stop_words[0:5])





static_keywords=['انتخابات','دلار','طلا','تورم','دانشگاه','کرونا','بورس','اقنصاد','تحریم','دولت','کویید','کوید''کوید١٩','کویید١٩','روحانی']
#حسن روحانی




import re
import nltk
#nltk.download('punkt')
from nltk.tokenize import word_tokenize
from hazm import *
from sklearn.feature_extraction.text import CountVectorizer
from sentence_transformers import SentenceTransformer
from sklearn.metrics.pairwise import cosine_similarity

def is_number(word):
    return word.isnumeric()

def check_static_keyword(processed_message,keywords):
   for word in processed_message.split():
        if word in static_keywords and word not in keywords:
            keywords.append(word)
   return keywords

def pre_process_message(message):

  normalizer = Normalizer()
  norm_doc=normalizer.normalize(message)

  doc_tokens=word_tokenize(norm_doc)

  tagger = POSTagger(model='resource/postagger.model')
  pos=tagger.tag(word_tokenize(message))
  verbs=[]
  for (key, val) in pos:
    if(val=='V'):
     verbs.append(key)
  stemmer = Stemmer()
  pre_doc= [word for word in doc_tokens if ( stemmer.stem(word)  not in stop_words and  word  not in stop_words)  and (stemmer.stem(word) not in verbs and  word not in verbs ) and not is_number(word)]
  pre_doc=' '.join(pre_doc) 

  return pre_doc

def extract_keyword(message,keyword_count):
  processed_message=pre_process_message(message)
  keywords=[]
  if(len(processed_message)<1):
       
        return keywords
  n_gram_range = (1, 1)
  # Extract candidate words/phrases
  count = CountVectorizer(ngram_range=n_gram_range).fit([processed_message])
  candidates = count.get_feature_names()
  model = SentenceTransformer('distilbert-base-nli-mean-tokens')
  doc_embedding = model.encode([processed_message])
  candidate_embeddings = model.encode(candidates)
  top_n = keyword_count
  distances = cosine_similarity(doc_embedding, candidate_embeddings)
  keywords = [candidates[index] for index in distances.argsort()[0][-top_n:]]
  keywords= check_static_keyword(processed_message,keywords)
  return keywords



def extract_hashtags(message):
    return re.findall(r"#(\w+)",message)
    
    
def extract_emails(message):
    return re.findall(r'[\w\.-]+@[\w\.-]+',message)






from telethon.sync import TelegramClient 

import asyncio


from telethon import TelegramClient, events, sync
import json 
import time
import datetime
import uuid
from kafka import KafkaProducer
from pymongo import MongoClient



mongoClient = MongoClient()
db=mongoClient.messages

def save_message(message):
        message = {
            'id' : message['id'],
            'sender_name' : message['sender_name'],
            'sender_username': message['sender_username'],
            'sender_id': message['sender_id'],
            'context': message['context'],
            'keywords': message['keywords'],
            'hashtags': message['hashtags'],
            'emails': message['emails'],
            'send_time': message['send_time']}
        db.messages.insert_one(message)
    
    
def date_format(message):
    if type(message) is datetime:
        return message.strftime("%Y-%m-%d %H:%M:%S")




async def preprocess(message):
	print("============================new message=============================")
	d = json.loads(message)
	if(len(d['message'])>2):

		ts = time.time()
		message={}
		message['id']=str(uuid.uuid4())
		message['context']=d['message']
		message['sender_id']=d['peer_id']['channel_id']
		message['sender_name']=d['chennel_name']
		message['sender_username']=d['chennel_username']

		message['send_time']=ts
		try :        
               		message['keywords']=extract_keyword(d['message'],4)
               		message['hashtags']=extract_hashtags(d['message'])
               		message['emails']=extract_emails(d['message'])

		except Exception:
			message['hashtags']=[]
			message['keywords']=[]
			message['emails']=[]
			pass
		print(message)
		save_message(message)
		message=json.dumps(message)
		return message;
    
 
from kafka import KafkaConsumer
consumer=KafkaConsumer(bootstrap_servers='172.20.20.31:9092')
consumer.subscribe('test5')
for msg in consumer:
	message=asyncio.run(preprocess(msg.value.decode('utf-8')))
	if not message is None:
		producer = KafkaProducer(bootstrap_servers='172.20.20.31:9092')
		future= producer.send('preProcessedData', message.encode('utf-8'))
		result = future.get(timeout=60)

