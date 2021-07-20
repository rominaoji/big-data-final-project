#!/usr/bin/env python
# coding: utf-8

from telethon import TelegramClient, events, sync
import json 
import time
import datetime
import uuid
from kafka import KafkaProducer
from pymongo import MongoClient
import configparser
import json
from telethon import TelegramClient
from telethon.errors import SessionPasswordNeededError
import os
import asyncio


config = configparser.ConfigParser()
config.read("config.ini")
# Setting configuration values
api_id = config['Telegram']['api_id']
api_hash = config['Telegram']['api_hash']
api_hash = str(api_hash)
phone = config['Telegram']['phone']
username = config['Telegram']['username']
print(username)

mongoClient = MongoClient()
db=mongoClient.channelInfo


#Create the client and connect
async def create_client():
  client = TelegramClient(username, api_id, api_hash,proxy=("http",'46.4.48.43',60222))
  await client.start()
  await client.connect()
  print("Client Created")
  #assert await client.connect()
  if  not  await client.is_user_authorized():
      client.send_code_request(phone)
      try:
         await  client.sign_in(phone, input('Enter the code: '))
      except SessionPasswordNeededError:
         await   client.sign_in(password=input('Password: '))
  return client
client= asyncio.run(create_client())


def save_channel_info(channelId,channelName,channelUsername):
    channelInfo = {
            '_id' : channelId,
            'name' : channelName,
            'username': channelUsername
        }
    db.channelInfo.insert_one(channelInfo)
    
    
def find_channel_info(channelId):
   return db.channelInfo.find_one({"_id":channelId})


async def fetch_channel_info(channel_id):
	result = await client(functions.channels.GetFullChannelRequest(channel=channel_id))
	return(result)

from telethon import TelegramClient, functions, types
from asyncio import run

async def get_channel_name(channel_id):
    res=find_channel_info(channel_id)
    print(res)
    if res is None:

      print('going to get channel {0} info'.format(channel_id))
      ch_info=await fetch_channel_info(channel_id)
        
      channel_name=ch_info.to_dict()['chats'][0]['title']
      channel_username=ch_info.to_dict()['chats'][0]['username']

      print('going to save channel {0} info'.format(channel_id))
      save_channel_info(channel_id,channel_name,channel_username)

    else:
        channel_name=res['name']
        channel_username=res['username']

    return [channel_name,channel_username]


def date_format(message):
    if type(message) is datetime:
        return message.strftime("%Y-%m-%d %H:%M:%S")



from telethon import events
@events.register( events.NewMessage(func=lambda e: e.is_channel and not e.is_group))
async def my_event_handler(event):
   
    print("============================new message=============================")
    data = json.loads(json.dumps(event.message.to_dict(),default=date_format))
    info=await get_channel_name(data['peer_id']['channel_id'])
    data['chennel_name']=info[0]
    data['chennel_username']=info[1]
    json_data = json.dumps(data)
    print(data)
    producer = KafkaProducer(bootstrap_servers='localhost:9092')
    future= producer.send('test5', json_data.encode('utf-8'))
    result = future.get(timeout=60)
    
        


#another way to get data (based on env)

# with TelegramClient(username, api_id, api_hash) as client:
#     client.add_event_handler(my_event_handler)
#     client.run_until_disconnected()


    
