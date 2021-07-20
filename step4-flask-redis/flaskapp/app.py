from random import choices
from flask import Flask, redirect, url_for, request
from flask import render_template
from kafka import KafkaConsumer
import redis
import json
import datetime,timedelta
from timedelta import Timedelta

redis_host = "localhost"
redis_port = 6379
redis_password = ""

r = redis.StrictRedis(host=redis_host, port=redis_port, password=redis_password, decode_responses=True)



#======================================  part 1  count of channel post in 6 houres

def GetCurrentHour():
    current_time = datetime.datetime.now()#.strftime('%Y%m%d%H%M')
    #h=current_time.hour
    return current_time

def Get6HouresAgo():
    current_time = datetime.datetime.now()
    decr=current_time -Timedelta(hours=6)
    #d=decr.strftime('%Y%m%d%H%M')
    return decr

def ConvTimetoStr(d):
    return d.strftime('%Y%m%d%H%M')


def GetChannelNames():
    all=[]
    for key in r.scan_iter("ch_name:*"):
        # delete the key
        msg=r.get(key)
        all.append(msg)
        #print(msg)
    return all    


def GetMinRangeTime(start,end):
    listofTime=[]
    listofTime.append(ConvTimetoStr(start))
    while (start<=end):
        start=start+Timedelta(minutes=1)
        listofTime.append(ConvTimetoStr(start))

    return listofTime

def GetCount6HourPostsz(choice,decresedTime6h,currentTime):
    count=0
    minList=GetMinRangeTime(decresedTime6h,currentTime)

    for t in minList:
        #print(t)
        key="minChPostCount:"+str(t)
        #print(str(r.zscore(key,choice)))
        if r.zscore(key,choice) is not None:
            count+=r.zscore(key,choice)

    return count



#=========================================  part 2   count of posts in range

def GetHourSet():
    all=[]
    for key in r.scan_iter("hourset:*"):
        msg=r.get(key)
        all.append(int(msg))
        #print(msg)
    return all  



def GetRangePostCount(start,end):
    sdate=datetime.datetime.strptime(start, '%Y%m%d%H')
    print(start,sdate)
    edate=datetime.datetime.strptime(end, '%Y%m%d%H')
    print(end,edate)
    minList=GetMinRangeTime(sdate,edate)
    print(minList)
    count=0
    for t in minList:
        
        key="minPostCount:"+str(t)
        print(t,r.zscore(key,'Count'))
        if r.zscore(key,'Count') is not None:
            count+=r.zscore(key,'Count')

    return count


#=========================================== part 3 count unique hashtags in 1 hour

def GetLastHourUniqueHashtags():
    current_time=datetime.datetime.now()
    lastHour=current_time-Timedelta(hours=1)
    minList=GetMinRangeTime(lastHour,current_time)
    allhourHashtags=[]
    for t in minList:
    
        key="MinHashtagSet:"+str(t)
        minhash=r.smembers(key)
        print(t,minhash)
        for mh in minhash:
            if mh not in allhourHashtags:
                allhourHashtags.append(mh)

    count=len(allhourHashtags)
    print(allhourHashtags)
    print(count)
    return count


#=========================================== part 4 last 1000 hashtags

def GetLast1000Hashtags():
    key="ListOfHashtags:List"
    listofH=r.lrange(key,0,999)
    return listofH



#============================================ part 5 last 100 posts

def GetLast100posts():
    key="ListOfPosts:List"
    listofP=r.lrange(key,0,99)
    return listofP






def GetAllData():
    allmsg=[]
    for key in r.scan_iter("*"):
        # delete the key
        msg=r.get(key)
        allmsg.append([key,msg])
        #print(msg)
    return allmsg

def GetAllContext():
    allmsg=[]
    for key in r.scan_iter("context:*"):
        # delete the key
        msg=r.get(key)
        #print(type(msg))
        allmsg.append([key,msg])
        #print(msg)
    return allmsg





 
#************************************************************************  home page
app = Flask(__name__)

@app.route('/',methods = ['POST', 'GET'])
def home():
    print('start')
    chns=GetChannelNames()
    hours=GetHourSet()
    #print(hours)
    hoursSorted=sorted(hours)
    print(hoursSorted)
    return render_template("index.html",chns=chns,hours=hoursSorted)
	
#======================================  part 1  count of channel post in 6 houres

@app.route('/count6hchz',methods = ['POST', 'GET'])
def count6hchz(): 
    choice=""
    if request.method == 'POST':
        print('post')
        choice = str(request.form['chn'])
    else:
        choice = str(request.args.get('chn'))
    currentTime=GetCurrentHour()
    decresedTime6h=Get6HouresAgo()
    count=GetCount6HourPostsz(choice,decresedTime6h,currentTime)
    print('count of ',choice,' is ',count)
    data=[]
    data.append(count)
    return render_template("singleData.html", data=data)
    

#=========================================  part 2   count of posts in range

@app.route('/countTime',methods = ['POST', 'GET'])
def countTime(): 
    choice=""
    if request.method == 'POST':
        start = str(request.form['startt'])
        end = str(request.form['endt'])
    else:
        start = str(request.args.get('startt'))
        end = str(request.args.get('endt'))

    tcount=GetRangePostCount(start,end)
    print(start,end,tcount)
    data=[]
    data.append(tcount)
    return render_template("singleData.html", data=data)



#=========================================== part 3 count unique hashtags in 1 hour
@app.route('/hourhashtags')
def hourhashtags():
    count=GetLastHourUniqueHashtags()
    #print(type(allmsg))
    #print(allmsg)
    #d=json.load(allmsg)
    data=[]
    data.append(count)
    return render_template("singleData.html", data=data)
    


#=========================================== part 4 last 1000 hashtags

@app.route('/last1000hashtags')
def last1000hashtags():
    hashtags=GetLast1000Hashtags()
    #print(hashtags)
    #print(type(hashtags))

    return render_template("singleData.html", data=hashtags)


#============================================ part 5 last 100 posts

@app.route('/last100posts')
def last100posts():
    posts=GetLast100posts()

    return render_template("singleData.html", data=posts)





@app.route('/contents')
def GetContents():
    allmsg=GetAllContext()
    #print(type(allmsg))
    #print(allmsg)
    #d=json.load(allmsg)
    return render_template("app.html", allmsg=allmsg)
	


	
app.run(host='127.0.0.1', port=5000)