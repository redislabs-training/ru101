"""Generate sample data for RU101 course"""
from redis import StrictRedis
import sys
import random
from faker import Faker
import redisu.utils.textincr as textincr
import redisu.ru101.common.generate
from redisu.utils.keynamehelper import create_key_name, create_field_name

redis = None
fake = None
p = None
customers = []
ticket_tiers = ["Lottery", "General", "Reserved", "VIP"]
events = []
search_attrs = ['medal_event', 'disabled_access', 'venue']
max_seats_per_block = 32

def create_customers(num):
  """Generate customer profiles"""
  fake.seed(94002)
  for _ in range(num):
    cust_id = redisu.ru101.common.generate.cust_id()
    attr = {'customer_name': fake.name(),
            'address': fake.address(),
            'phone': fake.phone_number()}
    p.hmset(create_key_name("customer", cust_id), attr)
    p.sadd(create_key_name("customers"), cust_id)
    p.execute()
    customers.append(cust_id)

def create_event(event,
                 venue,
                 capacity=None,
                 geo=None,
                 add_faceted_search=False,
                 add_hashed_search=False,
                 add_seatmap=False):
  """Create event details"""
  sku = redisu.ru101.common.generate.sku()
  p.sadd(create_key_name("events"), sku)
  p.sadd(create_key_name("event", "skus", event), sku)
  p.sadd(create_key_name("venues", sku), venue)
  attrs = {'name': event,
           'venue': venue,
           'medal_event' : str(fake.random_element((True, False))),
           'disabled_access' : str(fake.random_element((True, False)))}
  if capacity is not None:
    event_capacity = random.randint(capacity // 10, capacity // 2)
    attrs['capacity'] = event_capacity
    tiers_abailable = random.randint(1, 3)
    tier_capacity = int(round(event_capacity / tiers_abailable))
    for k in range(tiers_abailable, 0, -1):
      attrs[create_field_name('available', ticket_tiers[k])] = tier_capacity
      attrs[create_field_name('price', ticket_tiers[k])] =\
        random.randint(10 * (k+1), 10 * (k+1) + 9)
    p.hmset(create_key_name("event", sku), attrs)
    if add_seatmap:
      create_seatmap(sku, tiers_abailable, tier_capacity)
  attrs['sku'] = sku
  if geo is not None:
    p.geoadd(create_key_name("geo", "venues", venue),
             geo['long'], geo['lat'], event)
    p.geoadd(create_key_name("geo", "events", event),
             geo['long'], geo['lat'], venue)
  if add_faceted_search:
    create_faceted_search(attrs)
  if add_hashed_search:
    create_hashed_search(attrs)
  events.append(sku)
  return attrs

def create_faceted_search(obj, key="sku", attrs=search_attrs):
  """Add keys for faceted search unit"""
  for k in range(len(attrs)):
    if search_attrs[k] in obj:
      fs_key = create_key_name("fs", search_attrs[k], str(obj[search_attrs[k]]))
      redis.sadd(fs_key, obj[key] if (key in obj) else None)

def create_hashed_search(obj, key="sku", attrs=search_attrs):
  """Add keys for hashed search unit"""
  import hashlib
  hfs = []
  for k in range(len(attrs)):
    if search_attrs[k] in obj:
      hfs.append((search_attrs[k], obj[search_attrs[k]]))
  hfs_k = create_key_name("hfs", hashlib.sha256(str(hfs).encode('utf-8')).hexdigest())
  redis.sadd(hfs_k, obj[key] if (key in obj) else None)

def create_seatmap(event_sku, tiers, capacity):
  """Add keys for seat reservation unit"""
  import math
  block_name = "A"
  # Use this formula if you want multiple 32bit blocks stored in a single key.
  # More compact, harder to understand
  # seats_per_block = min(max_seats_per_block, -(-capacity / tiers))
  seats_per_block = max_seats_per_block
  blocks_to_fill = -(-capacity // seats_per_block)
  to_fill = capacity
  for k in range(blocks_to_fill):
    seats_in_block = min(to_fill, seats_per_block)
    filled_seat_map = int(math.pow(2, seats_in_block))-1
    # vals = ["SET", "u32", k * seats_per_block, filled_seat_map]
    vals = ["SET", "u32", 0, filled_seat_map]
    seat_key = create_key_name("seatmap", event_sku,
                               ticket_tiers[(k % tiers) +1], block_name)
    p.execute_command("BITFIELD", seat_key, *vals)
    to_fill -= seats_in_block
    block_name = textincr.incr_str(block_name)
  p.execute()

def create_transit(transit, venue, event_sku, geo=None):
  """Add keys for transit search unit"""
  p.sadd(create_key_name("transit", transit, "events"), event_sku)
  if geo is not None:
    p.geoadd(create_key_name("geo", "transits", transit),
             geo['long'], geo['lat'], venue)

def create_venues(fn="/src/redisu/ru101/data/venues.json"):
  """Create venues from the flatfile JSON representation"""
  import json
  random.seed(94002)
  f = open(fn)
  venues = json.load(f)
  for i in range(len(venues)):
    v = venues[i]
    attrs = {'zone': v['zone']}
    if 'capacity' in v:
      attrs['capacity'] = v['capacity']
    p.hmset(create_key_name("venue", v['venue']), attrs)
    p.sadd(create_key_name("venues"), v['venue'])
    for k in range(len(v['events'])):
      e = create_event(v['events'][k],
                       v['venue'],
                       v['capacity'] if ('capacity' in v) else None,
                       v['geo'] if ('geo' in v) else None,
                       True,
                       True,
                       True)
      p.sadd(create_key_name("venue", v['venue'], "events"), e['sku'])
    if 'transit' in v:
      for k in range(len(v['transit'])):
        create_transit(v['transit'][k],
                       v['venue'],
                       e['sku'],
                       v['geo'] if ('geo' in v) else None)
    if 'geo' in v:
        p.geoadd(create_key_name("geo", "venues"),
                 v['geo']['long'], v['geo']['lat'], v['venue'])
    p.execute()

def create_orders(num_customers, max_orders_per_customer=20):
  """Create orders"""
  import time
  import datetime
  for _ in range(num_customers):
    print('.', end='', flush=True)
    num_orders = random.randint(1, max_orders_per_customer)
    customer_id = customers[random.randint(0, len(customers)-1)]
    customer_name = redis.hget(create_key_name("customer", customer_id),
                               "customer_name")
    for _ in range(num_orders):
      order_id = redisu.ru101.common.generate.order_id()
      event_sku = events[random.randint(0, len(events)-1)]
      for k in range(len(ticket_tiers)-1, 0, -1):
        event_k = create_key_name("event", event_sku)
        ticket_tier = ticket_tiers[k]
        if redis.hexists(event_k, create_field_name("available", ticket_tier)):
          price = float(redis.hget(event_k,
                                   create_field_name("price", ticket_tier)))
          abailable = int(redis.hget(event_k,
                                      create_field_name("available",
                                                        ticket_tier)))
          event_name = redis.hget(event_k, "name")
          if abailable > 1:
            qty = random.randint(1, min(75, abailable // 2))
          elif abailable == 1:
            qty = 1
          else:
            continue
          res = find_seats(event_sku, ticket_tiers[k], qty)
          ts = int(time.time())
          qty_allocated = res['assigned']
          order_total = qty_allocated * price
          purchase = {'customer': customer_id,
                      'customer_name': customer_name,
                      'order_id': order_id,
                      'event': event_sku,
                      'event_name': event_name,
                      'tier': ticket_tiers[k],
                      'qty': qty_allocated,
                      'cost': order_total,
                      'seats' : str(res['seats']),
                      'ts': ts}
          p.hmset(create_key_name("sales_order", order_id), purchase)
          inv = {'customer': customer_id,
                 'order_date': ts,
                 'due_date': str(datetime.date.fromtimestamp(ts) +
                             datetime.timedelta(days=90)),
                 'amount_due': order_total,
                 'status': "Invoiced"}
          p.hmset(create_key_name("invoice", order_id), inv)
          p.rpush(create_key_name("invoices", customer_id), order_id)
          p.zadd(create_key_name("invoice_totals"), {order_id: order_total})
          p.hincrby(create_key_name("event", event_sku),
                    create_field_name("available", ticket_tiers[k]),
                    -qty_allocated)
          p.sadd(create_key_name("event", event_sku, "sales_orders"),
                 order_id)
          sum_key = create_key_name("sales_summary", event_name)
          p.hincrbyfloat(sum_key, "total_sales", order_total)
          p.hincrby(sum_key, "total_tickets_sold", qty_allocated)
          p.hincrbyfloat(sum_key,
                         create_field_name("total_sales", ticket_tiers[k]),
                         order_total)
          p.hincrby(sum_key,
                    create_field_name("total_tickets_sold", ticket_tiers[k]),
                    qty_allocated)
          sum_key = create_key_name("sales_summary")
          p.hincrbyfloat(sum_key, "total_sales", order_total)
          p.hincrby(sum_key, "total_tickets_sold", qty_allocated)
          p.hincrbyfloat(sum_key,
                         create_field_name("total_sales", ticket_tiers[k]),
                         order_total)
          p.hincrby(sum_key,
                    create_field_name("total_tickets_sold", ticket_tiers[k]),
                    qty_allocated)
          p.execute()
          break

def find_seats(event_sku, tier, qty):
  """Find abailable seats"""
  # Find seat maps
  import math
  allocated_seats = []
  total_allocated = 0
  to_allocate = qty
  for key in redis.scan_iter(match=create_key_name("seatmap", event_sku, tier, "*"), count=1000):
    available = redis.bitcount(key)
    if available > 0:
      vals = ["GET", "u32", 0]
      new_seat_map = int(redis.execute_command("BITFIELD", key, *vals)[0])
      # Take some seats from this block
      num_taking = max(1, min(available // 2, to_allocate // 2))
      pos = list(range(0, 31))
      random.shuffle(pos)
      current_pos = 0
      for _ in range(num_taking):
        if new_seat_map >> pos[current_pos] & 1:
          new_seat_map -= int(math.pow(2, pos[current_pos]))
          vals = ["SET", "u32", 0, new_seat_map]
          redis.execute_command("BITFIELD", key, *vals)
          block_name = str(key).split(":")[3]
          allocated_seats.append(block_name + ":" + str(pos[current_pos]))
          current_pos += 1
          total_allocated += 1
          to_allocate -= 1
      if to_allocate == 0:
        break
    if to_allocate == 0:
      break
  return {'requested': qty, 'assigned': total_allocated,
          'seats': allocated_seats}

def main(argv):
  """ Main, used to call routines"""
  import os
  global redis
  redis = StrictRedis(host=os.environ.get("REDIS_HOST", "localhost"),
                      port=os.environ.get("REDIS_PORT", 6379),
                      password=os.environ.get("REDIS_PASSWORD", None),
                      db=0)
  global fake
  fake = Faker()

  global p
  p = redis.pipeline()

  # Create data
  print("Creating customers...")
  create_customers(501)
  print("Creating venues...")
  if len(argv) >1:
    create_venues(fn=argv[1])
  else:
    create_venues()
  print("Creating orders (note: this might take some time!)...", end='', flush=True)
  print("")
  create_orders(num_customers=250)
  print("creating `hello` key")
  redis.set("hello", "world")

if __name__ == "__main__":
    main(sys.argv)
