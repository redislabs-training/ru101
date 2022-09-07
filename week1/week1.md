# WEEK 1

## KEYS

- Unique key
- Binary strings
- Flat namespace
- Can be up to 512MB
- Automatically expired (TTL)

## Strings

- Text data, int, float, binary data
- Binary safe
- MAX 512MB

### Store

SET KEY STRING [EX TIME TTL]

GET KEY

### INCR

INCR KEY (will create if not exists)

INCRBY KEY NUMBER (can be negative)

enconding can be int

## ENVIROMENT SETUP

There is a docker image that encapsulates an IDE, Redis Server , source code and sample data

```bash
docker run --rm --name redis-lab -p8888:8888 redisuniversity/ru101-lab

```

After spin up docker open your browser

[localhost:8888/entry.html](localhost:8888/entry.html)

## Hashs

- key with named fields
- single level (no embed list or set)
- commands for fields
- dynamically add or remove fields

- session or ratting limit

HSET KEY name jose race Elf level 4 gold 100 hp 20

HGETALL KEY

HSET jose status daze

HDEL jose status

HGET jose level

HINCRBY jose gold 10

all operatios on hash is O(1)

GELALL command is O(n) (fields)

## Lists

- ordered collection of strings
- duplicates are allowed
- elements can be added and removed at left or right

- stacks or queues ,

- not nested only strings
- under the hood double linked list

RPUSH key element

LPOP key

LRANGE KEY index end

LLEN key

O(1)

LRANGE is O(n) size of range

4 billion elements

## Sets

- unique elements unordered list
- allow strings
- no nested

- operations
  - difference
  - intersect
  - union

SADD KEY value

SMEMBERS key ( all elements)

SCARD key  (size)

SISMEMBER key

sscan key match

SREM key value

SINTER KEY1 KEY2 (interssection)
SDIFF - difference
SUNION - union

EXPIREAT key time (to set ttl)

uses =

- unique players
- unique visitors

## Sorted Sets

- ordered collection of unique strings
- floating point SCORE
- commands ( union intersection diff)
- no nested

-uses :

- priority queues
- leaderbord

ZADD KEY SCORE MEMBER (value)

ZINCRBT key increment member

ZRANGE (low to high)

ZREVRANGE (high to low )

ZRANK
ZREVRANK

ZSCORE key member

ZCOUNT key index end
