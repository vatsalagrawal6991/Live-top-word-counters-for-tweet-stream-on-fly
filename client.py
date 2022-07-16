import sys
import os
#import time
import sys
from config import rds, TWEET, WORDSET, WORD_BUCKETS, WORD_PREFIX
#import tasks
#from celery import group

if (len(sys.argv) < 2):
    print("Use the command: python3 client.py <data_dir>")

DIR=sys.argv[1]

# Clear the output
rds.delete(WORDSET)
rds.zadd(WORDSET, {"foo": 0})
# Clear the input
rds.xtrim(TWEET, 0)
'''
GROUP_PREFIX = "g_"
for i in range(WORD_BUCKETS):
    stream_name = f"{WORD_PREFIX}{i}"
    group_name = f"{GROUP_PREFIX}{i}"
    #print(stream_name)
    rds.xgroup_create(stream_name,group_name)
    
rds.xgroup_create(TWEET,"g_3")
'''

#grp = group([tasks.tokha.s(i),tasks.adda1.s(i),tasks.adda0.s() for i in range (0,3)])()
#grp = group(x for i in range(40) for x in [tasks.tokha.s(i),tasks.adda1.s(i),tasks.adda0.s(i)])()
#grp = group(tasks.tokha.s(0))()

for pth, dirs, files in os.walk(DIR):
    for f in files:
        # f = files[0]
        abs_file = os.path.join(pth, f)
        for line in open(abs_file, 'r'):
            # push all the lines into input stream
            rds.xadd(TWEET, {TWEET: line})
'''
grp = group(x for i in range(60) for x in [tasks.tokha.s(i),tasks.adda1.s(i),tasks.adda0.s(i)])()

ctr = 0
while True:
    # print top 10 words
    print(rds.zrevrangebyscore(WORDSET, '+inf', '-inf', 0, 10, withscores=True))
    print(ctr)
    time.sleep(1)
    ctr = ctr + 1
    if ctr >= 1000:
        break
#3003125
'''