# Week 2

Redis don't support secondary indexes


## Capped collection 
- Retain subset of members(items)


LTRIM key start stop (retain by start and stop)

ZREMRANGEBYRANK key start stop (remove by start and stop)


## Set operations

ZINTERSTORE key numkeys key1 key2 aggregate sum|min|max

ZINTERUNION key numkeys key1 key2 aggregate sum|min|max


## Object Inspection
- load each element and match the keys
O(N)
## Faceted search 
- Create a list of sets and run SINTER to return the match for the keys 
- inverted index

O(N*M) size the smallest set and number of sets

Only matched objects

## Hashed index
- Create hash for all fields that need to be search 
- Create a set for generated hash for each new value


## Big-O

Explain how does Redis deals with Big-O notation when performing queries.
[https://en.wikipedia.org/wiki/Big_O_notation](Big-O)

string O(1)

operations in more the one key O(n)

lists sets sorted sets O(m)

set operations O(n*m)

LRANGE O(s+n) start and end index

