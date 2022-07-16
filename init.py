from config import rds, TWEET, WORD_PREFIX, WORD_BUCKETS
import tasks
from celery import group

## Configure consumer groups and workers

def setup_stream(stream_name: str):
  garbage = {"a": 1}
  # Create the stream by adding garbage values
  id = rds.xadd(stream_name, garbage)
  rds.xdel(stream_name, id)

rds.flushall()
setup_stream(TWEET)

for i in range(WORD_BUCKETS):
  stream_name = f"{WORD_PREFIX}{i}"
  #print(stream_name)
  setup_stream(stream_name)

GROUP_PREFIX = "g_"

for i in range(WORD_BUCKETS):
    stream_name = f"{WORD_PREFIX}{i}"
    group_name = f"{GROUP_PREFIX}{i}"
    #print(stream_name)
    rds.xgroup_create(stream_name,group_name)
    
rds.xgroup_create(TWEET,"g_3")

grp = group(x for i in range(60) for x in [tasks.tokha.s(i),tasks.adda1.s(i),tasks.adda0.s(i)])()
