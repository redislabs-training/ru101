"""Use Case: Seat Reservation.
Usage: Part of Redis University RU101 courseware"""
from redis import Redis
import os
import math
import redisu.utils.keynamehelper as keynamehelper
import redisu.utils.textincr as textincr
import redisu.ru101.common.generate as generate

redis = None

__max__seats_per_block__ = 32

def create_event(event_sku, blocks=2, seats_per_block=32, tier="General"):
  """Create the seats blocks for the given event. 32 bits are available for
seats. This could be extended to accommodate more bits, by storing multiple
u32 fields."""
  block_name = "A"
  for _ in range(blocks):
    filled_seat_map = int(math.pow(2, min(seats_per_block,
                                          __max__seats_per_block__)))-1
    vals = ["SET", "u32", 0, filled_seat_map]
    key = keynamehelper.create_key_name("seatmap", event_sku, tier, block_name)
    redis.execute_command("BITFIELD", key, *vals)
    block_name = textincr.incr_str(block_name)

def get_event_seat_block(event_sku, tier, block_name):
  """For the given Event, Tier and Block, return the seat map"""
  vals = ["GET", "u32", 0]
  key = keynamehelper.create_key_name("seatmap", event_sku, tier, block_name)
  return  redis.execute_command("BITFIELD", key, *vals)[0]

def print_event_seat_map(event_sku, tier="*"):
  """Format the seat map for display purposes."""
  key = keynamehelper.create_key_name("seatmap", event_sku, tier, "*")
  for block in redis.scan_iter(match=key, count=1000):
    (_, tier_name, block_name) = block.rsplit(":", 2)
    seat_map = get_event_seat_block(event_sku, tier_name, block_name)
    print(("{:40s} ").format(block), end=' ')
    for i in range(seat_map.bit_length()):
      if (i % 10) == 0:
        print("|", end=' ')
      print((seat_map >> i) & 1, end=' ')
    print("|")

def test_create_seat_map():
  """Part One - Create the event map"""
  print("\n==Test - Create & Print seat map")
  print("== Create two blocks of 10 seats")
  event = "123-ABC-723"
  seats = 10
  create_event(event, seats_per_block=seats)
  print_event_seat_map(event)

def get_available(seat_map, seats_required):
  """Return the available contiguous seats that match the criteria"""
  seats = []
  end_seat = seat_map.bit_length()+1
  if seats_required <= end_seat:
    required_block = int(math.pow(2, seats_required))-1
    for i in range(1, end_seat+1):
      if (seat_map & required_block) == required_block:
        seats.append({'first_seat': i, 'last_seat': i + seats_required -1})
      required_block = required_block << 1
  return seats

def find_seat_selection(event_sku, tier, seats_required):
  """Find seats ranges that meet the criteria"""
  # Get all the seat rows
  seats = []
  key = keynamehelper.create_key_name("seatmap", event_sku, tier, "*")
  for block in redis.scan_iter(match=key, count=1000):
    # Find if there are enough seats in the row, before checking if they
    # are contiguous
    if redis.bitcount(block) >= seats_required:
      (_, tier_name, block_name) = block.rsplit(":", 2)
      seat_map = get_event_seat_block(event_sku, tier_name, block_name)
      block_availability = get_available(seat_map, seats_required)
      if len(block_availability) > 0:
        seats.append({'event': event_sku, 'tier' : tier_name,
                      'block': block_name, 'available': block_availability})
    else:
      print("Row '{}' does not have enough seats".format(block))
  return seats

def print_seat_availabiliy(seats):
  """Print out the seat availbaility"""
  for block in seats:
    print("Event: {}".format(block['event']))
    current_block = block['available']
    for i in range(len(current_block)):
      print("-Row: {}, Start {}, End {}".format(block['block'],
                                                current_block[i]['first_seat'],
                                                current_block[i]['last_seat'],))

def set_seat_map(event_sku, tier, block_name, seat_map):
  """ Set the seatmap to the given value"""
  vals = ["SET", "u32", 0, seat_map]
  key = keynamehelper.create_key_name("seatmap", event_sku, tier, block_name)
  redis.execute_command("BITFIELD", key, *vals)

def test_find_seats():
  """ Test function to find various combinations of seats."""
  print("\n==Test - Find Seats")
  event = "123-ABC-723"
  seats = 10
  create_event(event, seats_per_block=seats)

  print("== Find 6 contiguous available seats")
  available_seats = find_seat_selection(event, "General", 6)
  print_seat_availabiliy(available_seats)

  # Check that we skip rows
  print("""== Remove a 4 seat from Block A, so only Block B has the right
 availability for 6 seats""")
  # Unset bits 2-5
  set_seat_map(event, "General", "A", int(math.pow(2, seats) - 31))
  print_event_seat_map(event)
  available_seats = find_seat_selection(event, "General", 6)
  print_seat_availabiliy(available_seats)

# Part Two - reserve seats
class Error(Exception):
  """Base class for exceptions in this module."""
  pass

class SeatTaken(Error):
  """Expception if a seat is taken during the reservation process."""
  def __init__(self, expression, message):
    super(SeatTaken, self).__init__()
    self.expression = expression
    self.message = message

def reservation(event_sku, tier, block_name, first_seat, last_seat):
  """ Reserve the required seats. Create an expiring key (i.e. a latch) to
 reserve each seat. If that is successful, then an XOR can be executed to
 update the seat map, without needed a Watch."""
  reserved = False
  p = redis.pipeline()
  try:
    for i in range(first_seat, last_seat+1):
      # Reserve individual seat, raise exception is already reserved
      seat_key = keynamehelper.create_key_name("seatres", event_sku,
                                               tier, block_name, str(i))
      if redis.set(seat_key, "True", px=5000, nx=True) != True:
        raise SeatTaken(i, seat_key)
    order_id = generate.order_id()
    required_block = int(math.pow(2,
                                  last_seat-first_seat +1)
                        ) -1 << (first_seat-1)
    vals = ["SET", "u32", 0, required_block]
    res_key = keynamehelper.create_key_name("seatres", event_sku,
                                            tier, block_name, order_id)
    p.execute_command("BITFIELD", res_key, *vals)
    p.expire(res_key, 5)
    block_key = keynamehelper.create_key_name("seatmap", event_sku,
                                              tier, block_name)
    p.bitop("XOR", block_key, block_key, res_key)
    p.execute()
    reserved = True
  except SeatTaken as error:
    print("Seat Taken/{}".format(error.message))
  finally:
    p.reset()
  return reserved

def test_reserved_seats():
  """Test function for reserving seats"""
  print("\n==Test - Reserved Seats")
  print("== Block of 10 seats, with seat 4 taken")
  event = "737-DEF-911"
  seats = 10
  create_event(event, 1, seats, "VIP")
  # Seat 4 (the 8th bit) is already sold. We calc this as
  # (2^(seats)-1) - bit_number_of_seat
  # e.g. 1023 - 8
  set_seat_map(event, "VIP", "A", int(math.pow(2, seats)-1-8))
  print_event_seat_map(event)

  print("== Request 2 seats, succeeds")
  seats = find_seat_selection(event, "VIP", 2)
  print_seat_availabiliy(seats)
  # Just choose the first found
  made_reservation = reservation(event, "VIP", seats[0]['block'],
                                 seats[0]['available'][0]['first_seat'],
                                 seats[0]['available'][0]['last_seat'])
  print("Made reservation? {}".format(made_reservation))
  print_event_seat_map(event)

  # Find space for 5 seats
  print("== Request 5 seats, succeeds")
  seats = find_seat_selection(event, "VIP", 5)
  print_seat_availabiliy(seats)
  # Just choose the first found
  made_reservation = reservation(event, "VIP", seats[0]['block'],
                                 seats[0]['available'][0]['first_seat'],
                                 seats[0]['available'][0]['last_seat'])
  print("Made reservation? {}".format(made_reservation))
  print_event_seat_map(event)

  # Find space for 2 seat, but not enough inventory
  print("== Request 2 seats, fails")
  seats = find_seat_selection(event, "VIP", 2)
  if len(seats) == 0:
    print("Not enough seats")

  # Find space for 1 seat
  print("== Simulate two users trying to get the same seat")
  seats = find_seat_selection(event, "VIP", 1)
  # Create a seat reservation (simulating another user), so that the
  # reservation fails
  seat_num = str(seats[0]['available'][0]['first_seat'])
  key = keynamehelper.create_key_name("seatres", event, "VIP",
                                      seats[0]['block'],
                                      seat_num)
  redis.set(key, "True", px=5000)
  made_reservation = reservation(event, "VIP", seats[0]['block'],
                                 seats[0]['available'][0]['first_seat'],
                                 seats[0]['available'][0]['last_seat'])
  print("Made reservation? {}".format(made_reservation))
  print_event_seat_map(event)

def main():
  """ Main, used to call test cases for this use case"""
  from redisu.utils.clean import clean_keys

  global redis
  redis = Redis(host=os.environ.get("REDIS_HOST", "localhost"),
                port=os.environ.get("REDIS_PORT", 6379),
                password=os.environ.get("REDIS_PASSWORD", None),
                db=0, decode_responses=True)
  clean_keys(redis)
  # Perform the test cases
  test_create_seat_map()
  test_find_seats()
  test_reserved_seats()

if __name__ == "__main__":
  keynamehelper.set_prefix("uc03")
  main()
