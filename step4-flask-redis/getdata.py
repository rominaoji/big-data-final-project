import numpy as np
import json
from kafka import KafkaConsumer
import redis
from datetime import datetime
from pymongo import MongoClient
import os
import ast



redis_host = "localhost"
redis_port = 6379
redis_password = ""

r = redis.StrictRedis(host=redis_host, port=redis_port, password=redis_password, decode_responses=True)








def WriteToRedis(key,val):
    
    #try:

        x=r.set(key, val)
        r.expire(key,604800)
        return x
        # step 5: Retrieve the hello message from Redis
        #msg = r.get(key)
        #print(msg)        

    #except Exception as e:
        #print(e)

#======================================  part 1  count of channel post in 6 houres      

def SaveChannelName(data):
    username=str(data['sender_username'])
    key="ch_name:"+username
    r.set(key, username)
    r.expire(key,604800)


def SaveMinChannelPostsCount(data):
    username=str(data['sender_username'])
    stime=data['send_time']
    stimeformated=datetime.fromtimestamp(stime).strftime('%Y-%m-%d %H:%M:%S')
    septime=datetime.strptime(stimeformated, '%Y-%m-%d %H:%M:%S')
    zaman=datetime.fromtimestamp(stime).strftime('%Y%m%d%H%M')
    key="minChPostCount:"+str(zaman)
    r.zadd(key,{username:1},incr=True)
    r.expire(key,604800)



#=========================================  part 2   count of posts in range
def SaveHoures(data):
    stime=data['send_time']
    stimeformated=datetime.fromtimestamp(stime).strftime('%Y-%m-%d %H:%M:%S')
    septime=datetime.strptime(stimeformated, '%Y-%m-%d %H:%M:%S')
    zaman=datetime.fromtimestamp(stime).strftime('%Y%m%d%H')
    print(data,zaman)
    key="hourset:"+str(zaman)
    r.set(key, zaman)
    r.expire(key,604800)


def SaveMinPostsCount(data):
    stime=data['send_time']
    stimeformated=datetime.fromtimestamp(stime).strftime('%Y-%m-%d %H:%M:%S')
    septime=datetime.strptime(stimeformated, '%Y-%m-%d %H:%M:%S')
    zaman=datetime.fromtimestamp(stime).strftime('%Y%m%d%H%M')
    key="minPostCount:"+str(zaman)
    r.zadd(key,{'Count':1},incr=True)
    r.expire(key,604800)


#=========================================== part 3 count unique hashtags in 1 hour

def SaveMinHashtags(data):

    stime=data['send_time']
    stimeformated=datetime.fromtimestamp(stime).strftime('%Y-%m-%d %H:%M:%S')
    septime=datetime.strptime(stimeformated, '%Y-%m-%d %H:%M:%S')
    zaman=datetime.fromtimestamp(stime).strftime('%Y%m%d%H%M')
    hashtags=data['hashtags']
    print(hashtags)
    for hasht in hashtags:
        print(hasht)

        key="MinHashtagSet:"+str(zaman)
        r.sadd(key,hasht)
        r.expire(key,604800)



#=========================================== part 4 last 1000 hashtags
def SaveHashtaginList(data):
    hashtags=data['hashtags']
    #print(hashtags)
    for hasht in hashtags:
        #print(hasht)
        key="ListOfHashtags:List"
        r.lpush(key,hasht)
        r.ltrim(key,0,999)

#============================================ part 5 last 100 posts
def SavePostinList(data):
    stime=data['send_time']
    stimeformated=datetime.fromtimestamp(stime).strftime('%Y-%m-%d %H:%M:%S')
    septime=datetime.strptime(stimeformated, '%Y-%m-%d %H:%M:%S')
    zaman=datetime.fromtimestamp(stime).strftime('%Y%m%d%H%M')
    channel=str(data['sender_username'])
    text=channel+" | "+ stimeformated +" | "+str(data['context'])
    key="ListOfPosts:List"
    r.lpush(key,text)
    r.ltrim(key,0,99)





def SaveContexts(msg,zaman):
    username=msg['sender_username']
    id=msg['id']
    #print(id)
    stime=msg['send_time']
    context=msg['context']
    key="context:"+str(username)+","+str(zaman)
    r.set(key,context)
    r.expire(key,604800)






def ProcessData(msg):

    #print(msg)
    username=msg['sender_username']
    id=msg['id']
    #print(id)
    stime=msg['send_time']
    stimeformated=datetime.fromtimestamp(stime).strftime('%Y-%m-%d_%H:%M:%S')
    septime=datetime.strptime(stimeformated, '%Y-%m-%d_%H:%M:%S')
    zaman=str(septime.year)+"-"+str(septime.month)+"-"+str(septime.day)+"-"+str(septime.hour)+"-"+str(septime.minute)+"-"+str(septime.second)
    zaman=datetime.fromtimestamp(stime).strftime('%Y%m%d-%H%M%S')
    print(stime,stimeformated ,zaman)
    #text=d['context']
    text=str(msg)
    #print(text)
    key="msg:"+str(username)+","+str(zaman)
    print(key)
    x=WriteToRedis(key,text)
    SaveChannelName(msg)
    SaveContexts(msg,zaman)
    SaveMinHashtags(msg)
    SaveHoures(msg)
    SaveHashtaginList(msg)
    SavePostinList(msg)
    SaveMinChannelPostsCount(msg)
    SaveMinPostsCount(msg)
#================================================================================================
#================================================================================================


def GetArchiveData():
    try:
        mongoClient = MongoClient('185.236.37.254:9093')
        db=mongoClient.messages

    except Exception as e:
        print(e)

    i=0
    res=db.messages.find()
    for msg in res:
        ProcessData(msg)
        i+=1
        print(i,' records Added')





def GetOnlineData():
    print('start')
    consumer=KafkaConsumer(bootstrap_servers='185.236.37.254:9092')
    print(consumer)
    consumer.subscribe('preProcessedData')
    i=0
    for msg in consumer:
        d=json.loads(msg.value.decode('utf-8'))
        ProcessData(d)
        i+=1
        print(i,' records Added')


#================================================================================================
#================================================================================================
#GetOnlineData()
GetArchiveData()



#================================================================================================
#================================================================================================
#================================================================================================
#================================================================================================



