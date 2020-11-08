"""Helper functions to create fake identifiers in various formats"""
from faker import Faker

__fake__ = Faker()

def cust_id():
  """Generate customer id in the format AAAAAAAA"""
  return __fake__.pystr(min_chars=8, max_chars=8).upper()

def sku():
  """Generate sku in the format AAAA-AAAA-AAAA-AAAA"""
  return "{0}-{1}-{2}-{3}".format(__fake__.pystr(min_chars=4, max_chars=4),
                                  __fake__.pystr(min_chars=4, max_chars=4),
                                  __fake__.pystr(min_chars=4, max_chars=4),
                                  __fake__.pystr(min_chars=4, max_chars=4)
                                 ).upper()

def order_id():
  """Generate fake order id's in the format AAAAAA-AAAAAA"""
  return "{0}-{1}".format(__fake__.pystr(min_chars=6, max_chars=6),
                          __fake__.pystr(min_chars=6, max_chars=6)).upper()

def random_time_today():
  """Gnerate a random time during the current day"""
  from random import uniform
  from time import mktime
  import datetime
  #
  date1 = date2 = datetime.datetime.now()
  date1 = date1.replace(hour=0, minute=0, second=0)
  date2 = date2.replace(hour=23, minute=59, second=59)
  #
  time1 = mktime(date1.timetuple())
  time2 = mktime(date2.timetuple())
  return int(uniform(time1, time2))
