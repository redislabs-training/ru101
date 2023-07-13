"""Use Case: Inventory Control.
Usage:
Part of Redis University RU101 courseware"""
from redis import Redis, WatchError
import os
import time
import redisu.utils.keynamehelper as keynamehelper
import redisu.ru101.common.generate as generate

redis = None

customers = [{'id': "bill", 'customer_name': "bill smith"},
             {'id': "mary", 'customer_name': "mary jane"},
             {'id': "jamie", 'customer_name': "jamie north"},
             {'id': "joan", 'customer_name': 'joan west'},
             {'id': "fred", 'customer_name': "fred smith"},
             {'id': "amy", 'customer_name': 'amy south'},
             {'id': "jim", 'customer_name': 'jim somebody'}
            ]

def create_customers(cust_array):
  """Create customer keys from the array of passed customer details"""
  for cust in cust_array:
    c_key = keynamehelper.create_key_name("customer", cust['id'])
    redis.hmset(c_key, cust)

events = [{'sku': "123-ABC-723",
           'name': "Men's 100m Final",
           'disabled_access': "True",
           'medal_event': "True",
           'venue': "Olympic Stadium",
           'category': "Track & Field",
           'capacity': 60102,
           'available:General': 20000,
           'price:General': 25.00
          },
          {'sku': "737-DEF-911",
           'name': "Women's 4x100m Heats",
           'disabled_access': "True",
           'medal_event': "False",
           'venue': "Olympic Stadium",
           'category': "Track & Field",
           'capacity': 60102,
           'available:General': 10000,
           'price:General': 19.50
          },
          {'sku': "320-GHI-921",
           'name': "Womens Judo Qualifying",
           'disabled_access': "False",
           'medal_event': "False",
           'venue': "Nippon Budokan",
           'category': "Martial Arts",
           'capacity': 14471,
           'available:General': 5000,
           'price:General': 15.25
          }
         ]

def create_events(event_array, available=None, price=None, tier="General"):
  """ Create events from the array of passed event details. Provides overrides
for number of available tickets, price and ticket tier."""
  e_set_key = keynamehelper.create_key_name("events")
  for event in event_array:
    # Override the availability & price if provided
    if available != None:
      event['available:' + tier] = available
    if price != None:
      event['price:' + tier] = price
    e_key = keynamehelper.create_key_name("event", event['sku'])
    redis.hmset(e_key, event)
    redis.sadd(e_set_key, event['sku'])

# Part One - Check availability and Purchase
def check_availability_and_purchase(customer, event_sku, qty, tier="General"):
  """Check if there is sufficient inventory before making the purchase"""
  p = redis.pipeline()
  try:
    e_key = keynamehelper.create_key_name("event", event_sku)
    redis.watch(e_key)
    available = int(redis.hget(e_key, "available:" + tier))
    price = float(redis.hget(e_key, "price:" + tier))
    if available >= qty:
      p.hincrby(e_key, "available:" + tier, -qty)
      order_id = generate.order_id()
      purchase = {'order_id': order_id, 'customer': customer,
                  'tier': tier, 'qty': qty, 'cost': qty * price,
                  'event_sku': event_sku, 'ts': int(time.time())}
      so_key = keynamehelper.create_key_name("sales_order", order_id)
      p.hmset(so_key, purchase)
      p.execute()
    else:
      print("Insufficient inventory, have {}, requested {}".format(available,
                                                                   qty))
  except WatchError:
    print("Write Conflict check_availability_and_purchase: {}".format(e_key))
  finally:
    p.reset()
  print("Purchase complete!")

def print_event_details(event_sku):
  """Print the details of the event, based on the passed SKU"""
  e_key = keynamehelper.create_key_name("event", event_sku)
  print(redis.hgetall(e_key))

def test_check_and_purchase():
  """Test function Check & purchase method"""
  print("\n==Test 1: Check stock levels & purchase")
  # Create events with 10 tickets available
  create_events(events, available=10)

  # Stock available
  print("== Request 5 ticket, success")
  requestor = "bill"
  event_requested = "123-ABC-723"
  check_availability_and_purchase(requestor, event_requested, 5)
  print_event_details(event_requested)

  # No purchase, not enough stock
  print("== Request 6 ticket, failure because of insufficient inventory")
  requestor = "mary"
  event_requested = "123-ABC-723"
  check_availability_and_purchase(requestor, event_requested, 6)
  print_event_details(event_requested)

# Part Two - Reserve stock & Credit Card auth
def reserve(customer, event_sku, qty, tier="General"):
  """First reserve the inventory and perform a credit authorization. If successful
then confirm the inventory deduction or back the deducation out."""
  p = redis.pipeline()
  try:
    e_key = keynamehelper.create_key_name("event", event_sku)
    redis.watch(e_key)
    available = int(redis.hget(e_key, "available:" + tier))
    if available >= qty:
      order_id = generate.order_id()
      ts = int(time.time())
      price = float(redis.hget(e_key, "price:" + tier))
      p.hincrby(e_key, "available:" + tier, -qty)
      p.hincrby(e_key, "held:" + tier, qty)
      # Create a hash to store the seat hold information
      hold_key = keynamehelper.create_key_name("ticket_hold", event_sku)
      p.hsetnx(hold_key, "qty:" + order_id, qty)
      p.hsetnx(hold_key, "tier:" + order_id, tier)
      p.hsetnx(hold_key, "ts:" + order_id, ts)
      p.execute()
  except WatchError:
    print("Write Conflict in reserve: {}".format(e_key))
  finally:
    p.reset()
  if creditcard_auth(customer, qty * price):
    try:
      purchase = {'order_id': order_id, 'customer': customer,
                  'tier': tier, 'qty': qty, 'cost': qty * price,
                  'event_sku': event_sku, 'ts': int(time.time())}
      redis.watch(e_key)
      # Remove the seat hold, since it is no longer needed
      p.hdel(hold_key, "qty:" + order_id,)
      p.hdel(hold_key, "tier:" + order_id)
      p.hdel(hold_key, "ts:" + order_id)
      # Update the Event
      p.hincrby(e_key, "held:" + tier, -qty)
      # Post the Sales Order
      so_key = keynamehelper.create_key_name("sales_order", order_id)
      p.hmset(so_key, purchase)
      p.execute()
    except WatchError:
      print("Write Conflict in reserve: {}".format(e_key))
    finally:
      p.reset()
    print("Purchase complete!")
  else:
    print("Auth failure on order {} for customer {} ${}".format(order_id,
                                                                customer,
                                                                price * qty))
    backout_hold(event_sku, order_id)

def creditcard_auth(customer, order_total):
  """Test function to approve/denigh an authorization request"""
  # Always fails Joan's auth
  if customer.upper() == "JOAN":
    return False
  else:
    return True

def backout_hold(event_sku, order_id):
  """Remove the ticket reservation"""
  p = redis.pipeline()
  try:
    hold_key = keynamehelper.create_key_name("ticket_hold", event_sku)
    e_key = keynamehelper.create_key_name("event", event_sku)
    redis.watch(e_key)
    qty = int(redis.hget(hold_key, "qty:" + order_id))
    tier = redis.hget(hold_key, "tier:" + order_id)
    p.hincrby(e_key, "available:" + tier, qty)
    p.hincrby(e_key, "held:" + tier, -qty)
    # Remove the hold, since it is no longer needed
    p.hdel(hold_key, "qty:" + order_id)
    p.hdel(hold_key, "tier:" + order_id)
    p.hdel(hold_key, "ts:" + order_id)
    p.execute()
  except WatchError:
    print("Write Conflict in backout_hold: {}".format(e_key))
  finally:
    p.reset()

def test_reserve():
  """Test function reserve & credit auth"""
  print("\n==Test 2: Reserve stock, perform credit auth and complete purchase")
  # Create events with 10 tickets available
  create_events(events, available=10)

  # Make purchase with reservation and credit authorization steps
  print("== Reserve & purchase 5 tickets")
  requestor = "jamie"
  event_requested = "737-DEF-911"
  reserve(requestor, event_requested, 5)
  print_event_details(event_requested)

  print("== Reserve 5 tickets, failure on auth, return tickets to inventory")
  requestor = "joan"
  event_requested = "737-DEF-911"
  reserve(requestor, event_requested, 5)
  print_event_details(event_requested)

# Part Three - Expire Reservation
def create_expired_reservation(event_sku, tier="General"):
  """Test function to create a set of reservation that will shortly expire"""
  cur_t = time.time()
  tickets = {'available:' + tier: 485,
             'held:' + tier: 15}
  holds = {'qty:VPIR6X': 3, 'tier:VPIR6X': tier, 'ts:VPIR6X': int(cur_t - 16),
           'qty:B1BFG7': 5, 'tier:B1BFG7': tier, 'ts:B1BFG7': int(cur_t - 22),
           'qty:UZ1EL0': 7, 'tier:UZ1EL0': tier, 'ts:UZ1EL0': int(cur_t - 30)
          }
  k = keynamehelper.create_key_name("ticket_hold", event_sku)
  redis.hmset(k, holds)
  k = keynamehelper.create_key_name("event", event_sku)
  redis.hmset(k, tickets)

def expire_reservation(event_sku, cutoff_time_secs=30):
  """ Check if any reservation has exceeded the cutoff time. If any have, then
backout the reservation and return the inventory back to the pool."""
  cutoff_ts = int(time.time()-cutoff_time_secs)
  e_key = keynamehelper.create_key_name("ticket_hold", event_sku)
  for field in redis.hscan_iter(e_key, match="ts:*", count=1000):
    if int(field[1]) < cutoff_ts:
      (_, order_id) = field[0].split(":")
      backout_hold(event_sku, order_id)

def test_expired_res():
  """Test function expired reservations"""
  print("\n==Test 3: Back out reservations when expiration threshold exceeded")

  # Create events
  create_events(events)

  # Create expired reservations for the Event
  print("== Create ticket holds, expire > 30 sec, return tickets to inventory")
  event_requested = "320-GHI-921"
  create_expired_reservation(event_requested)

  tier = "General"
  h_key = keynamehelper.create_key_name("ticket_hold", event_requested)
  e_key = keynamehelper.create_key_name("event", event_requested)
  while True:
    expire_reservation(event_requested)
    outstanding = redis.hmget(h_key, "qty:VPIR6X", "qty:B1BFG7", "qty:UZ1EL0")
    available = redis.hget(e_key, "available:" + tier)
    print("{}, Available:{}, Reservations:{}".format(event_requested,
                                                     available,
                                                     outstanding))
    # Break if all items in outstanding list are None
    if all(v is None for v in outstanding):
      break
    else:
      time.sleep(1)

def main():
  """ Main, used to call test cases for this use case"""
  from redisu.utils.clean import clean_keys

  global redis
  redis = Redis(host=os.environ.get("REDIS_HOST", "localhost"),
                port=os.environ.get("REDIS_PORT", 6379),
                password=os.environ.get("REDIS_PASSWORD", None),
                db=0, decode_responses=True)
  clean_keys(redis)
  create_customers(customers)
  # Performs the tests
  test_check_and_purchase()
  test_reserve()
  test_expired_res()

if __name__ == "__main__":
  keynamehelper.set_prefix("uc02")
  main()
