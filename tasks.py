from celery import Celery
#from collections import Counter
from config import rds, TWEET, WORDSET, WORD_BUCKETS, WORD_PREFIX 
app = Celery('2021MCS2157', broker='pyamqp://guest@localhost//',backend='redis://localhost:6579', worker_prefetch_multiplier=1)

#######################################################################################
# Note - I have used some part of trend.py and serial.py to match ground truth
#######################################################################################
@app.task(acks_late=True)
def tokha(i):
    cour = 0 
    
    consumer_name = f"g_3_c_{i}"
    while True:
        if (cour == 0):
            res2 = rds.xautoclaim(TWEET,"g_3",consumer_name,60000,count=200000)
            #print(res2)
            if (res2!=[]):
                wc0 = {"foo":0}
                wc1 = {"foo":0}
                wci = []
                #print(res2)
                for text in res2:
                    wci.append(text[0])
                    if text[1]['tweet'] == '\n':
                        continue
                    af = (text[1]['tweet']).split(',')[4:-2]
                    tweet = " ".join(af)
                    for word in tweet.split(" "):
                        word =word.lower()
                        if ((hash(word)%2) == 0):
                            if word not in wc0:
                                wc0[word] = 0
                            wc0[word] = wc0[word] + 1
                        else:
                            if word not in wc1:
                                wc1[word] = 0
                            wc1[word] = wc1[word] + 1
                pipe = rds.pipeline()
                pipe.multi()
                abd = rds.xack(TWEET,"g_3",*wci)
                if int(abd) == len(wci):
                    rds.xadd('w_0',wc0 )
                    rds.xadd('w_1',wc1 )
                    pipe.execute()
                else:
                    pipe.discard()
                
        cour = (cour + 1)%100
        res1 = rds.xreadgroup("g_3",consumer_name,{TWEET:">"},count=200000,block=(((i+1)//5)+1)*1000)
        if(res1 == []):
            if (i > 40):
                continue
            break
        wc0 = {"foo":0}
        wc1 = {"foo":0}
        wci = []
        for text in res1[0][1]:
            wci.append(text[0])
            abc = text[1]['tweet']
            if  abc == '\n':
                continue
            af = abc.split(',')[4:-2]
            tweet = " ".join(af)
            for word in tweet.split(" "):
                word =word.lower()
                if ((hash(word)%2) == 0):
                    if word not in wc0:
                        wc0[word] = 0
                    wc0[word] = wc0[word] + 1
                else:
                    if word not in wc1:
                        wc1[word] = 0
                    wc1[word] = wc1[word] + 1
        #for word in wc0:
            #rds.xadd('w_0',{word:wc0[word]} )
        #for word in wc1:
            #rds.xadd('w_1',{word:wc1[word]} )
        pipe = rds.pipeline()
        pipe.multi()
        abd = rds.xack(TWEET,"g_3",*wci)
        if int(abd) == len(wci):
            rds.xadd('w_0',wc0 )
            rds.xadd('w_1',wc1 )
            pipe.execute()
        else:
            pipe.discard()
        
        
        #print(dict(**wc0))

#######################################################################################
# Note - I have used some part of trend.py and serial.py to match ground truth
#######################################################################################


@app.task(acks_late=True)
def adda0(i):
    cour = 0
    
    consumer_name = f"g_0_c_{i}"
    while True:
        if (cour == 0):
            res2 = rds.xautoclaim("w_0","g_0",consumer_name,60000,count=100000)
            #print(res2)
            if (res2!=[]):
                #print(res2)
                wc = {}
                wci = []
                for text in res2:
                    wci.append(text[0])
                    for word in text[1]:
                        if word not in wc:
                            wc[word] = 0
                        wc[word] = wc[word] + int(text[1][word])
                pipe = rds.pipeline()
                pipe.multi()  
                abc = rds.xack("w_0","g_0",*wci) 
                if int(abc) == len(wci):
                    for word in wc:
                        rds.zincrby(WORDSET,wc[word],word)
                    pipe.execute()
                else:
                    pipe.discard()
        cour = (cour + 1)%100
        res1 = rds.xreadgroup("g_0",consumer_name,{"w_0":">"},count=100000,block=(i+1)*1000)
        if(res1 == []):
            if (i > 40):
                continue
            break
        wc = {}
        wci =[]
        for text in res1[0][1]:
            wci.append(text[0])
            for word in text[1]:
                #print(type(text[1][word]))
                #print(text)
                if word not in wc:
                    wc[word] = 0
                wc[word] = wc[word] + int(text[1][word])
        pipe = rds.pipeline()
        pipe.multi()
        abc = rds.xack("w_0","g_0",*wci)
        if int(abc) == len(wci):
            for word in wc:
                rds.zincrby(WORDSET,wc[word],word)
            pipe.execute()
        else:
            pipe.discard()
        
#######################################################################################
# Note - I have used some part of trend.py and serial.py to match ground truth
#######################################################################################

@app.task(acks_late=True)
def adda1(i):
    cour = 0
    consumer_name = f"g_1_c_{i}"
    while True:
        if (cour == 0):
            res2 = rds.xautoclaim("w_1","g_1",consumer_name,60000,count=100000)
            #print(res2)
            if (res2!=[]):
                #print(res2)
                wc = {}
                wci =[]
                for text in res2:
                    wci.append(text[0])
                    for word in text[1]:
                        if word not in wc:
                            wc[word] = 0
                        wc[word] = wc[word] + int(text[1][word])
                pipe = rds.pipeline()
                pipe.multi()
                abc = rds.xack("w_1","g_1",*wci)
                if int(abc) == len(wci):
                    for word in wc:
                        rds.zincrby(WORDSET,wc[word],word)
                    pipe.execute()
                else:
                    pipe.discard()
        cour = (cour + 1)%100
        res1 = rds.xreadgroup("g_1",consumer_name,{"w_1":">"},count=100000,block=(i+1)*1000)
        #print(res1)
        if(res1 == []):
            if (i > 40):
                continue
            break
        wc = {}
        wci = []
        for text in res1[0][1]:
            wci.append(text[0])
            for word in text[1]:
                if word not in wc:
                    wc[word] = 0
                wc[word] = wc[word] + int(text[1][word])
        pipe = rds.pipeline()
        pipe.multi()

        abc = rds.xack("w_1","g_1",*wci)
        if int(abc) == len(wci):
            for word in wc:
                rds.zincrby(WORDSET,wc[word],word)
                #print(type(wc[word]))
            pipe.execute()
        else:
            pipe.discard()
        #print("done")    
        
#######################################################################################
# Note - I have used some part of trend.py and serial.py to match ground truth
#######################################################################################
