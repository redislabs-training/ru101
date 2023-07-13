"""Utility to dump and load keys from Redis. Key values are encoded in JSON. In
this module the following functions are available:

  * dump(fn, compress, match)
  * load(fn, compress)

"""
from redis import StrictRedis
import sys


def dump(redis, filename="/data/ru101.json", compress=False, match="*"):
  """Dump matching keys into JSOn file format"""
  import json
  import base64
  import gzip

  count = 0
  try:
    if compress:
      filen = gzip.open(filename, "wb")
    else:
      filen = open(filename, "w")
    for k in redis.scan_iter(match=match, count=1000):
      obj = {}
      t = redis.type(k)
      obj['t'] = t
      obj['k'] = k
      obj['ttl'] = redis.ttl(k)
      if t == "hash":
        obj['v'] = redis.hgetall(k)
      elif t == "set":
        obj['v'] = list(redis.smembers(k))
      elif t == "zset":
        obj['v'] = redis.zrange(k, 0, -1, withscores=True)
      elif t == "list":
        obj['v'] = redis.lrange(k, 0, -1)
      elif t == "string":
        encoding = redis.object("encoding", obj['k'])
        obj['e'] = encoding
        if encoding == "embstr":
          obj['v'] = redis.get(k)
        elif encoding == "raw":
          obj['v'] = base64.b64encode(bytearray(redis.get(k)))
        else:
          print("got a string encoded as {}".format(encoding))
          continue
      else:
        print("got a type I don't do: {}".format(t))
        continue
      count += 1
      filen.write(json.dumps(obj))
      filen.write("\n")
  finally:
    filen.close()
    print("total keys dumped: {}".format(count))

def load(redis, filename="/data/ru101.json", compress=False):
  """Load keys from file in JSON format"""
  import json
  import base64
  import gzip

  count = 0
  if compress:
    filen = gzip.open(filename, "rb")
  else:
    filen = open(filename, "r")
  try:
    line = filen.readline()
    p = redis.pipeline()
    while line:
      obj = json.loads(line)
      p.delete(obj['k'])
      if obj['t'] == "hash":
        p.hmset(obj['k'], obj['v'])
      elif obj['t'] == "set":
        for j in range(len(obj['v'])):
          p.sadd(obj['k'], obj['v'][j])
      elif obj['t'] == "zset":
        for j in range(len(obj['v'])):
          v, s = obj['v'][j]
          p.zadd(obj['k'], {v: s})
      elif obj['t'] == "list":
        for j in range(len(obj['v'])):
          p.rpush(obj['k'], obj['v'][j])
      elif obj['t'] == "string":
        if obj['e'] == "embstr":
          p.set(obj['k'], obj['v'])
        elif obj['e'] == "raw":
          bin_val = bytearray(base64.b64decode(obj['v']))
          vals = ["SET", "u8", 0, 0]
          for i in range(len(bin_val)):
            vals[2] = i * 8
            vals[3] = bin_val[i]
            p.execute_command("BITFIELD", obj['k'], *vals)
      else:
        print("got a type I don't do: {}".format(obj['t']))
        continue
      if 'ttl' in obj and obj['ttl'] >= 0:
        p.expire(obj['k'], obj['ttl'])
      p.execute()
      count += 1
      line = filen.readline()
  finally:
    filen.close()
    print("total keys loaded: {}".format(count))

def main(command, datafile):
  """Entry point to execute either the dump or load"""
  import os
  redis_c = StrictRedis(host=os.environ.get("REDIS_HOST", "localhost"),
                        port=os.environ.get("REDIS_PORT", 6379),
                        password=os.environ.get("REDIS_PASSWORD", None),
                        db=0)
  if command == "load":
    load(redis_c, filename=datafile)
  elif command == "dump":
    dump(redis_c, filename=datafile)
  else:
    print("Don't know how to do {}".format(command))

if __name__ == "__main__":
  main(sys.argv[1], sys.argv[2])
