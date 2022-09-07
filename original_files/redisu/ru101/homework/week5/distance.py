"""Sample solution to homwork problem."""
from redis import StrictRedis
import os

redis = StrictRedis(host=os.environ.get("REDIS_HOST", "localhost"),
                    port=os.environ.get("REDIS_PORT", 6379),
                    password=os.environ.get("REDIS_PASSWORD", None),
                    db=0)

redis.delete("event:Football:distances")
for outer in redis.zrange("geo:event:Football", 0, -1):
  for inner in redis.zrange("geo:event:Football", 0, -1):
    if outer != inner:
      dist = redis.geodist("geo:event:Football", inner, outer, "km")
      redis.zadd("event:Football:distances",
                 redis.geodist("geo:event:Football", inner, outer, "km"),
                 min(inner, outer) + "/" + max(inner, outer))

for res in redis.zrevrange("event:Football:distances", 0, -1, withscores=True):
  print(res)

