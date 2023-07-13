"""Utility to clean up any data created by running any of the example. It uses
the defined seperator to achive this
"""
from redis import StrictRedis
import os
import sys
import redisu.utils.keynamehelper as keynamehelper


def clean_keys(redis_c, prefix=None):
  """Remove keys with a given prefix. Stop if the default prefix would result in
  removing all keys. This is used by the various use cases to clean up their
  tets data before running again."""
  key_prefix = prefix if prefix != None else keynamehelper.get_prefix()
  count = 0
  if key_prefix != None:
    count = 0
    for k in redis_c.scan_iter(match=key_prefix + "*", count=1000):
      redis_c.delete(k)
      count += 1
  else:
    print("No prefix, no way am I going to remove '*' !")
  return count

def main(prefix):
  """Entry point, allowing the function to be called from command line
  arguments"""
  redis = StrictRedis(host=os.environ.get("REDIS_HOST", "localhost"),
                      port=os.environ.get("REDIS_PORT", 6379),
                      password=os.environ.get("REDIS_PASSWORD", None),
                      db=0)
  count = clean_keys(redis, prefix)
  print("Removed {} keys".format(count))

if __name__ == "__main__":
  if len(sys.argv) == 2:
    main(sys.argv[1])
  else:
    print("Wrong number of args, specify the key prefix")
