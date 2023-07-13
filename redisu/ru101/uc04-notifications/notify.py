"""Use Case: Nofications.
Usage:
Part of Redis University RU101 courseware"""
from redis import Redis
import os
import time
import random
import threading
import redisu.utils.keynamehelper as keynamehelper
import redisu.ru101.common.generate as generate

redis = None

def create_event(event_sku):
  """Create the event key from the provided details."""
  e_key = keynamehelper.create_key_name("event", event_sku)
  redis.hmset(e_key, {'sku': event_sku})

def purchase(event_sku):
  """Simple purchase function, that pushes the sales order for publishing"""
  qty = random.randrange(1, 10)
  price = 20
  order_id = generate.order_id()
  s_order = {'who': "Jim", 'qty': qty, 'cost': qty * price,
             'order_id': order_id, 'event': event_sku,
             'ts': generate.random_time_today()}
  post_purchases(order_id, s_order)

def post_purchases(order_id, s_order):
  """Publish purchases to the queue."""
  so_key = keynamehelper.create_key_name("sales_order", order_id)
  redis.hmset(so_key, s_order)
  notify_key = keynamehelper.create_key_name("sales_order_notify")
  redis.publish(notify_key, order_id)
  notify_key = keynamehelper.create_key_name("sales_order_notify",
                                             s_order['event'])
  redis.publish(notify_key, order_id)

def listener_events_analytics(channel):
  """Listener to summarize total sales by ticket numbers and order value."""
  l = redis.pubsub(ignore_subscribe_messages=True)
  c_key = keynamehelper.create_key_name(channel)
  l.subscribe(c_key)
  p = redis.pipeline()
  for message in l.listen():
    order_id = message['data']
    so_key = keynamehelper.create_key_name("sales_order", order_id)
    (cost, qty, event_sku) = redis.hmget(so_key, 'cost', 'qty', 'event')
    so_set_key = keynamehelper.create_key_name("sales", event_sku)
    p.sadd(so_set_key, order_id)
    sum_key = keynamehelper.create_key_name("sales_summary")
    p.hincrbyfloat(sum_key,
                   keynamehelper.create_field_name(event_sku, "total_sales"),
                   cost)
    p.hincrby(sum_key,
              keynamehelper.create_field_name(event_sku, "total_tickets_sold"),
              qty)
    p.execute()

def listener_sales_analytics(channel):
  """Listener to summarize the sales statistics. Histograms, using
 BITFIELDs are maintained to show sales by hour."""
  l = redis.pubsub(ignore_subscribe_messages=True)
  c_key = keynamehelper.create_key_name(channel)
  l.subscribe(c_key)
  for message in l.listen():
    order_id = message['data']
    so_key = keynamehelper.create_key_name("sales_order", order_id)
    (ts, qty, event_sku) = redis.hmget(so_key, 'ts', 'qty', 'event')
    hour_of_day = int(time.strftime("%H", time.gmtime(int(ts))))
    vals = ["INCRBY", "u16", max(hour_of_day * 16, 0), int(qty)]
    tod_event_hist_key = keynamehelper.create_key_name("sales_histogram",
                                                       "time_of_day",
                                                       event_sku)
    redis.execute_command("BITFIELD", tod_event_hist_key, *vals)

def print_statistics(stop_event):
  """Thread that prints current event statistics."""
  from binascii import hexlify
  sum_key = keynamehelper.create_key_name("sales_summary")
  print("\n === START")
  print("{:8} | {:12} | {:3} |  Histogram by hour".format("T/S",
                                                          "Event",
                                                          "#"), end=' ')
  while not stop_event.is_set():
    ts = time.strftime("%H:%M:%S")
    e_key = keynamehelper.create_key_name("event", "*")
    for event in redis.scan_iter(match=e_key, count=1000):
      (_, event_sku) = event.rsplit(":", 1)
      field_key = keynamehelper.create_field_name(event_sku,
                                                  "total_tickets_sold")
      t_tickets = redis.hget(sum_key, field_key)
      t_tickets = int(t_tickets) if t_tickets != None else 0
      tod_hist_key = keynamehelper.create_key_name("sales_histogram",
                                                   "time_of_day",
                                                   event_sku)
      hist = redis.get(tod_hist_key)
      if hist != None:
        hist_vals = [hist[i:i+2] for i in range(0, len(hist), 2)]
        print("\n{:8} | {:12} | {:3d} | ".format(ts,
                                                  event_sku,
                                                  t_tickets), end=' ')
        for i in range(0, 24):
          num = int(hexlify(hist_vals[i].encode(encoding='utf-8')), 16) if i < len(hist_vals) else 0
          print("{:02d}/{:03d}".format(i, num), end=' ')
    time.sleep(1)
  print("\n === END")

# Part One - simple publish & subscribe
def test_pub_sub():
  """Test function for pub/sub messages for fan out"""
  print("== Test 1: Simple pub/sub")

  events = ["Womens Judo"]
  for e in events:
    create_event(e)

  threads = []
  stop_event = threading.Event()
  threads.append(threading.Thread(target=listener_sales_analytics,
                                  args=("sales_order_notify",)))
  threads.append(threading.Thread(target=listener_events_analytics,
                                  args=("sales_order_notify",)))
  threads.append(threading.Thread(target=print_statistics,
                                  args=(stop_event,)))

  for i in range(len(threads)):
    threads[i].setDaemon(True)
    threads[i].start()

  for i in range(15):
    purchase(events[random.randrange(0, len(events))])
    time.sleep(random.choice((0.75, 1, 1.25, 1.5)))
  stop_event.set()
  time.sleep(2)

# Part Two - pattern subscriptions

# Subscribe for 'Opening Ceremony' events, pick every 5th purchase as the
# lottery winner
def listener_ceremony_alerter(channel):
  """Listener that looks for either 'Opening Ceremony' or 'Closing Ceremony'
  events only. If then tracks a Lottery content, award a prize for every 5th
  order for this event only."""
  l = redis.pubsub(ignore_subscribe_messages=True)
  c_key = keynamehelper.create_key_name(channel, "*Ceremony")
  l.psubscribe(c_key)
  for message in l.listen():
    order_id = message['data']
    _, event = message['channel'].rsplit(":", 1)
    sum_key = keynamehelper.create_key_name("sales_summary")
    field_key = keynamehelper.create_field_name(event, "total_orders")
    total_orders = redis.hincrby(sum_key, field_key, 1)
    if total_orders % 5 == 0:
      print("===> Winner!!!!! Ceremony Lottery - Order Id: {}"\
        .format(order_id))

# Subscribe to all event, except 'Opening Ceremony' events
def listener_event_alerter(channel):
  """Listener for purchases for events other than 'Opening Ceremony'."""
  l = redis.pubsub(ignore_subscribe_messages=True)
  c_key = keynamehelper.create_key_name(channel, "[^Opening]*")
  l.psubscribe(c_key)
  for message in l.listen():
    order_id = message['data']
    so_key = keynamehelper.create_key_name("sales_order", order_id)
    (event_sku, qty, cost) = redis.hmget(so_key, 'event', 'qty', 'cost')
    print("Purchase {}: #{} ${}".format(event_sku, qty, cost))

def test_patterned_subs():
  """Test function for patterned subscriptions"""
  print("==Test 2: Patterned subscribers - Opening Ceremony Lottery picker")

  threads = []
  threads.append(threading.Thread(target=listener_ceremony_alerter,
                                  args=("sales_order_notify",)))
  threads.append(threading.Thread(target=listener_event_alerter,
                                  args=("sales_order_notify",)))

  for i in range(len(threads)):
    threads[i].setDaemon(True)
    threads[i].start()

  events = ["Mens Boxing", "Womens 4x400",
            "Opening Ceremony", "Closing Ceremony"]
  for e in events:
    create_event(e)

  for i in range(50):
    purchase(events[random.randrange(0, len(events))])
    time.sleep(random.random())


def main():
  """ Main, used to call test cases for this use case"""
  from redisu.utils.clean import clean_keys

  global redis
  redis = Redis(host=os.environ.get("REDIS_HOST", "localhost"),
                port=os.environ.get("REDIS_PORT", 6379),
                password=os.environ.get("REDIS_PASSWORD", None),
                db=0, decode_responses=True)
  clean_keys(redis)

  # Performs the tests
  test_pub_sub()
  #test_patterned_subs()

if __name__ == "__main__":
  keynamehelper.set_prefix("uc04")
  main()
