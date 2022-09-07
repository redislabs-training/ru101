# Week 3


## Transactions 

- All or None 
- Isolated

MULTI start
EXEC queues commands
DISCARD  cancel all queues events

- [optimistic concurrency control](https://en.wikipedia.org/wiki/Optimistic_concurrency_control)

WATCH key (if changes fail the transaction)


## Object Storage Hash

HSET key field value ...
HGET key field
HMGET key field field ...
HGETALL key 
HSCAN key cursor MATCH pattern COUNT count
HEXISTS key field 

HSETNX key field value (if not exists)

HINCRBY key field increment
HINCRBYFLOAT key field increment

HDEL key field

### Store complex objects 

- flatten key inside hash 
- add new hash representing nested object 
- track relations using set 



## Use case: Inventory control  

[example code](https://github.com/uelei/ru101/blob/main/original_files/redisu/ru101/uc02-inventory-control/inventory.py)
