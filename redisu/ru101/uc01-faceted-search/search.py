"""Use Case: Faceted search.
Usage:
Part of Redis University RU101 courseware"""
from redis import Redis
import os
import hashlib
import json
import redisu.utils.keynamehelper as keynamehelper

redis = None

__events__ = [{'sku': "123-ABC-723",
               'name': "Men's 100m Final",
               'disabled_access': True,
               'medal_event': True,
               'venue': "Olympic Stadium",
               'category': "Track & Field"
              },
              {'sku': "737-DEF-911",
               'name': "Women's 4x100m Heats",
               'disabled_access': True,
               'medal_event': False,
               'venue': "Olympic Stadium",
               'category': "Track & Field"
              },
              {'sku': "320-GHI-921",
               'name': "Womens Judo Qualifying",
               'disabled_access': False,
               'medal_event': False,
               'venue': "Nippon Budokan",
               'category': "Martial Arts"
              }
             ]

def create_events(e_array):
  """ Create events from the passed array."""
  for i in range(len(e_array)):
    key = keynamehelper.create_key_name("event", e_array[i]['sku'])
    redis.set(key, json.dumps(e_array[i]))

def print_event_name(event_sku):
  """Helper to get the Event, extract and print the venue name."""
  key = keynamehelper.create_key_name("event", event_sku)
  event = json.loads(redis.get(key))
  print((event['name'] if ('name' in event) else event['sku']))

def match_by_inspection(*keys):
  """Match Method 1 - Object inspection
  Find all matching keys, retreive value and then exeamine for all macthing
  attributes."""
  matches = []
  key = keynamehelper.create_key_name("event", "*")
  for key in redis.scan_iter(match=key, count=1000):
    match = False
    event = json.loads(redis.get(key))
    for keyval in keys:
      key, val = keyval
      if key in event and event[key] == val:
        match = True
      else:
        match = False
        break
    if match:
        matches.append(event['sku'])
  return matches

def test_object_inspection():
  """Test function for Method 1: Object Inspection"""
  print("\n== Method 1: Object Inspection")
  # Create events
  create_events(__events__)

  # Find the match
  print("=== disabled_access=True")
  matches = match_by_inspection(('disabled_access', True))
  for match in matches:
    print_event_name(match)

  print("=== disabled_access=True, medal_event=False")
  matches = match_by_inspection(('disabled_access', True),
                                ('medal_event', False))
  for match in matches:
    print_event_name(match)

  print("=== disabled_access=False, medal_event=False, venue='Nippon Budokan'")
  matches = match_by_inspection(('disabled_access', False),
                                ('medal_event', False),
                                ('venue', "Nippon Budokan"))
  for match in matches:
    print_event_name(match)


__lookup_attrs__ = ['disabled_access', 'medal_event', 'venue', 'tbd']

def create_events_with_lookups(e_array):
  """Match method 2 - Faceted Search
For each attribute & value combination, add the event into a Set"""
  for i in range(len(e_array)):
    key = keynamehelper.create_key_name("event", e_array[i]['sku'])
    redis.set(key, json.dumps(e_array[i]))
    for k in range(len(__lookup_attrs__)):
      if __lookup_attrs__[k] in e_array[i]:
        attr_name = str(e_array[i][__lookup_attrs__[k]])
        fs_key = keynamehelper.create_key_name("fs",
                                               __lookup_attrs__[k],
                                               attr_name)
        redis.sadd(fs_key, e_array[i]['sku'])

def match_by_faceting(*keys):
  """Use SINTER to find the matching elements"""
  facets = []
  for keyval in keys:
    key, val = keyval
    fs_key = keynamehelper.create_key_name("fs", key, str(val))
    facets.append(fs_key)
  return redis.sinter(facets)

def test_faceted_search():
  """Test function for Method 2: Faceted Search"""
  print("\n== Method 2: Faceted Search")
  # Create events
  create_events_with_lookups(__events__)
  # Find the match
  print("=== disabled_access=True")
  matches = match_by_faceting(('disabled_access', True))
  for match in matches:
    print_event_name(match)

  print("=== disabled_access=True, medal_event=False")
  matches = match_by_faceting(('disabled_access', True), ('medal_event', False))
  for match in matches:
    print_event_name(match)

  print("=== disabled_access=False, medal_event=False, venue='Nippon Budokan'")
  matches = match_by_faceting(('disabled_access', False),
                              ('medal_event', False),
                              ('venue', "Nippon Budokan"))
  for match in matches:
    print_event_name(match)

# Match method 3 - Hashed Faceted Search
def create_events_hashed_lookups(e_array):
  """Create hashed lookup for each event"""
  for i in range(len(e_array)):
    key = keynamehelper.create_key_name("event", e_array[i]['sku'])
    redis.set(key, json.dumps(e_array[i]))
    hfs = []
    for key in range(len(__lookup_attrs__)):
      if __lookup_attrs__[key] in e_array[i]:
        hfs.append((__lookup_attrs__[key], e_array[i][__lookup_attrs__[key]]))
      hashed_val = hashlib.sha256(str(hfs).encode('utf-8')).hexdigest()
      hfs_key = keynamehelper.create_key_name("hfs", hashed_val)
      redis.sadd(hfs_key, e_array[i]['sku'])

def match_by_hashed_faceting(*keys):
  """Match method 3 - Hashed Faceted Search"""
  matches = []
  hfs = []
  for i in range(len(__lookup_attrs__)):
    key = [x for x in keys if x[0] == __lookup_attrs__[i]]
    if key:
      hfs.append(key[0])
  hashed_val = hashlib.sha256(str(hfs).encode('utf-8')).hexdigest()
  hashed_key = keynamehelper.create_key_name("hfs", hashed_val)
  for found_key in redis.sscan_iter(hashed_key, count=1000):
    matches.append(found_key)
  return matches

def test_hashed_faceting():
  """Test function for Method 3: Hashed Faceting"""
  print("\n== Method 3: Hashed Faceting")
  # Create events
  create_events_hashed_lookups(__events__)
  # Find the match
  print("=== disabled_access=True")
  matches = match_by_hashed_faceting(('disabled_access', True))
  for match in matches:
    print_event_name(match)

  print("=== disabled_access=True, medal_event=False")
  matches = match_by_hashed_faceting(('disabled_access', True),
                                     ('medal_event', False))
  for match in matches:
    print_event_name(match)

  print("=== disabled_access=False, medal_event=False, venue='Nippon Budokan'")
  matches = match_by_hashed_faceting(('disabled_access', False),
                                     ('medal_event', False),
                                     ('venue', "Nippon Budokan"))
  for match in matches:
    print_event_name(match)

def main():
  """ Main, used to call test cases for this use case"""
  from redisu.utils.clean import clean_keys

  global redis
  redis = Redis(host=os.environ.get("REDIS_HOST", "localhost"),
                port=os.environ.get("REDIS_PORT", 6379),
                password=os.environ.get("REDIS_PASSWORD", None),
                db=0)
  clean_keys(redis)

  # Perform the tests
  test_object_inspection()
  test_faceted_search()
  test_hashed_faceting()

if __name__ == "__main__":
  keynamehelper.set_prefix("uc01")
  main()
