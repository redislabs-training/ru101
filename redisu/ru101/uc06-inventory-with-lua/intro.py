# Use Case: Examples with Lua and Python
# Usage: Part of Redis University RU101 courseware
from redis import Redis
import os
import redisu.utils.keynamehelper as keynamehelper

# Takes two numeric keys and performs the requested operation.
# Like all Lua scripts, this operation is atomic and that the
# result will be derived from the value of the two keys at a particular
# moment in time.
#
# KEYS[1] A key holding a numeric value
# KEYS[2] A second key holding a numeric value
# ARGV[1] A string naming the operation to be performed. Valid
#         values are "max" and "sum"
# Returns the result of the operation (max or sum).
stats_script = """
    -- Convert arguments to numbers
    local k1 = redis.call('get', KEYS[1])
    local k2 = redis.call('get', KEYS[2])

    if ARGV[1] == "sum" then
      return k1 + k2
    elseif ARGV[1] == "max" then
      return math.max(k1, k2)
    else
      return nil
    end
"""

def main():
    from redisu.utils.clean import clean_keys
    global redis
    redis = Redis(host=os.environ.get("REDIS_HOST", "localhost"),
                  port=os.environ.get("REDIS_PORT", 6379),
                  password=os.environ.get("REDIS_PASSWORD", None),
                  db=0, decode_responses=True)

    clean_keys(redis, "hits")
    redis.set("hits:homepage", 2000)
    redis.set("hits:loginpage", 75)

    # Register our script with the Redis Python client and
    # return a callable object for invoking our script.
    stats = redis.register_script(stats_script)

    # Invoke our "sum" script.
    # This calls SCRIPT LOAD and then stores
    # the SHA1 digest of the script for future use.
    total = stats(["hits:homepage", "hits:loginpage"], ["sum"])
    assert(total == 2075)

    # Two more tests.
    max = stats(["hits:homepage", "hits:loginpage"], ["max"])
    assert(max == 2000)

if __name__ == "__main__":
    main()
