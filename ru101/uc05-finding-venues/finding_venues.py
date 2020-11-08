"""Use Case: Finding Venues.
Usage:
Part of Redis University RU101 courseware"""
from redis import Redis
import os
import redisu.utils.keynamehelper as keynamehelper

redis = None

olympic_stadium = {'venue': "Olympic Stadium",
                   'capacity': 60000,
                   'events': [("Athletics", "LQRW-GDOE-GZJR-KWXT"),
                              ("Football", "BERK-SGQF-FBFZ-NEVK")],
                   'geo': {'long': 139.76632, 'lat': 35.666754},
                   'transit': ["Toei Odeo Line", "Chuo Main Line"]
                  }

nippon_budokan = {'venue': "Nippon Budokan",
                  'capacity': 12000,
                  'events': [("Judo", "AGHC-TXLI-ZTFN-ZHBP"),
                             ("Karate", "DDZE-PPZQ-PLNO-XBNI")],
                  'geo': {'long': 139.75, 'lat': 35.693333},
                  'transit':["Toei Shinjuku Line",
                             "Tozai Line",
                             "Hanzomon Line"]
                 }

makuhari_messe = {'venue': "Makuhari Messe",
                  'capacity': 6000,
                  'events': [("Fencing", "HDSM-OJNQ-UBFZ-AKBM"),
                             ("Taekwondo", "QTOP-FKNS-LMGQ-JHOF"),
                             ("Wrestling", "QBGH-ARMD-DFOY-JLLW")],
                  'geo': {'long': 140.034722, 'lat': 35.648333},
                  'transit': ["Keiyo Line"]
                 }

saitama_super_arena = {'venue': "Saitama Super Arena",
                       'capacity': 22000,
                       'events': [("Basketball", "MYUD-MPRZ-RSRV-DLYM")],
                       'geo': {'long': 139.630833, 'lat': 35.894889},
                       'transit': ["Saitama-Shintoshin", "Takasaki Line",
                                   "Utsunomiya Line", "Keihin-Tohoku Line",
                                   "Saikyo Line"]
                      }

international_stadium = {'venue': "International Stadium Yokohama",
                         'capacity': 70000,
                         'events': [("Football", "VGCM-ESPF-MXJZ-XKEX")],
                         'geo': {'long': 139.606247, 'lat': 35.510044},
                         'transit': ["Tokaido Shinkansen", "Yokohama Line",
                                     "Blue Line"]
                        }

isc = {'venue': "Tokyo Tatsumi International Swimming Center",
       'capacity': 5000,
       'events': [("Water polo", "SGHI-AMGC-EFZA-RAEZ")],
       'geo': {'long': 139.818943, 'lat': 35.647668},
       'transit': ["Keiyo Line", "Rinkai Line", "Yurakucho Line"]
      }

def create_venue(venue):
  """Create key and geo entry for passed venue"""
  key = keynamehelper.create_key_name("geo", "venues")
  redis.geoadd(key, venue['geo']['long'], venue['geo']['lat'], venue['venue'])

def test_venue_search():
  """Test 1 - geo searches around a venue"""
  print("\n==Test 1 - geo searches around a venue")
  create_venue(olympic_stadium)
  create_venue(nippon_budokan)
  create_venue(makuhari_messe)
  create_venue(saitama_super_arena)
  create_venue(international_stadium)
  create_venue(isc)

  print("== Find venues with 5km of 'Tokyo Station'")
  geo_key = keynamehelper.create_key_name("geo", "venues")
  print(redis.georadius(geo_key,
                        139.771977, 35.668024, 5, "km", withdist=True))

  print("== Find venues within 25km of 'Olympic Stadium'")
  print(redis.georadiusbymember(geo_key,
                                "Olympic Stadium", 25, "km", withdist=True))

def create_event_locations(venue):
  """Create geo entry for venues"""
  p = redis.pipeline()
  for i in range(len(venue['events'])):
    event, _ = venue['events'][i]
    key = keynamehelper.create_key_name("geo", "events", event)
    p.geoadd(key, venue['geo']['long'], venue['geo']['lat'], venue['venue'])
  p.execute()

def test_event_search():
  """Test 2 - geo searches around events"""
  print("\n==Test 2 - geo searches around events")
  create_event_locations(olympic_stadium)
  create_event_locations(nippon_budokan)
  create_event_locations(makuhari_messe)
  create_event_locations(saitama_super_arena)
  create_event_locations(international_stadium)
  create_event_locations(isc)

  print("== Find venues for 'Football' within 25km of 'Shin-Yokohama Station'")
  geo_key = keynamehelper.create_key_name("geo", "events", "Football")
  print(redis.georadius(geo_key,
                        139.606396, 35.509996, 25, "km", withdist=True))

def create_event_transit_locations(venue):
  """Create geo entries for transit stops for the passed venue"""
  p = redis.pipeline()
  for i in range(len(venue['transit'])):
    key = keynamehelper.create_key_name("geo", "transits",
                                        venue['transit'][i])
    p.geoadd(key, venue['geo']['long'], venue['geo']['lat'], venue['venue'])
  p.execute()

def test_transit_search():
  """Test 3 - geo searched around transit"""
  print("\n==Test 3 - geo searched around transit")
  create_event_transit_locations(olympic_stadium)
  create_event_transit_locations(nippon_budokan)
  create_event_transit_locations(makuhari_messe)
  create_event_transit_locations(saitama_super_arena)
  create_event_transit_locations(international_stadium)
  create_event_transit_locations(isc)

  print("== Find venues 5km from 'Tokyo Station' on the 'Keiyo Line'")
  geo_key = keynamehelper.create_key_name("geo", "transits", "Keiyo Line")
  print(redis.georadius(geo_key,
                        139.771977, 35.668024, 5, "km", withdist=True))

  print("""== Find the distance between 'Makuhari Messe' and 'Tokyo Tatsumi
   International Swimming Center' on the 'Keiyo Line'""")
  print(redis.geodist(geo_key, "Makuhari Messe",
                      "Tokyo Tatsumi International Swimming Center", "km"))

  print("== Find venues within 20km of 'Makuhari Messe' on the 'Keiyo Line'")
  # Note: This only works if the member we are search for is on the
  # "Keiyo Line". For example, "Olympic Statdium" is not
  # on the "Keiyo Line" so would return zero results.
  print(redis.georadiusbymember(geo_key,
                                "Makuhari Messe", 20, "km", withdist=True))

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
  test_venue_search()
  test_event_search()
  test_transit_search()

if __name__ == "__main__":
  keynamehelper.set_prefix("uc05")
  main()
