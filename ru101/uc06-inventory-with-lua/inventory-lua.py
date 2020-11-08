"""Use Case: Inventory Control.
Usage: Part of Redis University RU101 courseware"""
from redis import Redis
import os
import time
import unittest
from redisu.utils.clean import clean_keys
import redisu.ru101.common.generate as generate
import redisu.utils.keynamehelper as keynamehelper

CUSTOMERS = [{'id': "1357", 'customer_name': "bill smith"},
             {'id': "2468", 'customer_name': "mary jane"}]

EVENTS = [{'sku': "123-ABC",
           'name': "Men's 100m Final",
           'available:General': 200,
           'price:General': 25.00
          },
          {'sku': "456-DEF",
           'name': "Women's 4x100m Heats",
           'available:General': 100,
           'price:General': 19.50 }]

##### Lua Scripts #####


# Compare-and-set for the state of a ticket purchase. This effectively
# implements a state machine using compare-and-set. This function
# supports the following state changes.
# RESERVE -> [AUTHORIZE, TIMEOUT]
# AUTHORIZE -> [COMPLETE, TIMEOUT]
#
# KEYS[1] is a key of type Hash pointing to a purchase.
# ARGV[1] is the newly-requested state.
# ARGV[2] is the current timestamp.
# Returns 1 if successful. Otherwise, return 0.
update_purchase_state_script = """
    local current_state = redis.call('HGET', KEYS[1], 'state')
    local requested_state = ARGV[1]

    if ((requested_state == 'AUTHORIZE' and current_state == 'RESERVE') or
        (requested_state == 'FAIL' and current_state == 'RESERVE') or
        (requested_state == 'FAIL' and current_state == 'AUTHORIZE') or
        (requested_state == 'COMPLETE' and current_state == 'AUTHORIZE')) then
        redis.call('HMSET', KEYS[1], 'state', requested_state, 'ts', ARGV[2])
        return 1
    else
        return 0
    end
"""

# Request a lease on a number of tickets. In this first
# version of the script, there's a subtle bug which we'll
# fix below in the second version.
#
# KEYS[1] is a key of type Hash pointing to an event.
# ARGV[1] is the customer ID.
# ARGV[2] is the requested number of general admission tickets.
# ARGV[3] is the number of seconds to hold the tickets.
# Returns 1 if successful. Otherwise, returns 0.
request_ticket_hold_v1_script = """
    -- The keys we're using in the script
    local event_capacity = tonumber(redis.call('hget', KEYS[1], 'available:General'))
    local holds_key = 'holds:' .. KEYS[1]
    local customer_hold_key = 'hold:' .. ARGV[1] .. ':' .. KEYS[1]

    -- Store the number of requested tickets and the timeout for the ticket hold
    local requested_tickets = tonumber(ARGV[2])
    local hold_timeout = tonumber(ARGV[3])

    -- If there are available tickets, then create a new hold.
    if requested_tickets <= event_capacity then
        redis.call("HMSET", customer_hold_key, 'qty', requested_tickets, 'state', 'HELD')
        redis.call("EXPIRE", customer_hold_key, hold_timeout)
        redis.call("SADD", holds_key, customer_hold_key)
        return 1
    else
        return 0
    end
"""

# Returns the total number of tickets currently
# being held for a given event.
#
# KEYS[1] is a key of type Hash pointing to an event
held_ticket_count_script = """
    local ticket_holds_key = 'holds:' .. KEYS[1]
    local hold_keys = redis.call('SMEMBERS', ticket_holds_key)
    local tickets_held = 0

    for _,hold_key in ipairs(hold_keys) do
        local count = redis.call('HGET', hold_key, 'qty')
        -- If the return is nil, then remove the hold from the list
        if (count == nil) then
            redis.call('SREM', ticket_holds_key, hold_key)
        else
          tickets_held = tickets_held + count
        end
    end

    return tickets_held
"""

# Request a hold on a number of tickets.
#
# KEYS[1] is a key of type Hash pointing to an event
# ARGV[1] is the customer ID
# ARGV[2] is the requested number of general admission tickets
# ARGV[3] is the number of seconds to hold the tickets
request_ticket_hold_v2_script = """
    local event_capacity = tonumber(redis.call('HGET', KEYS[1], 'available:General'))
    local ticket_holds_key = 'holds:' .. KEYS[1]

    local customer_hold_key = 'hold:' .. ARGV[1] .. ':' .. KEYS[1]
    local requested_tickets = tonumber(ARGV[2])
    local hold_timeout = tonumber(ARGV[3])

    -- Calculate the total number of outstanding holds
    local hold_keys = redis.call('SMEMBERS', ticket_holds_key)
    local tickets_held = 0

    for _,hold_key in ipairs(hold_keys) do
        local count = redis.call('HGET', hold_key, 'qty')
        -- If the return is nil, then remove the hold from the list
        if (count == nil) then
            redis.call('SREM', ticket_holds_key, hold_key)
        else
          tickets_held = tickets_held + count
        end
    end

    -- If capacity remains, then create a new lease
    if (tickets_held + requested_tickets) <= event_capacity then
        redis.call("HMSET", customer_hold_key, 'qty', requested_tickets, 'state', 'HELD')
        redis.call("EXPIRE", customer_hold_key, hold_timeout)
        redis.call("SADD", ticket_holds_key, customer_hold_key)
        return 1
    else
        return requested_tickets
    end
"""

# KEYS[1] is a key of type Hash pointing to an event
# ARGV[1] is the customer ID
# ARGV[2] is the requested number of general admission tickets
# ARGV[3] is the amount of time to extend the customer hold by
prepare_purchase_script = """
    local customer_hold_key = 'hold:' .. ARGV[1] .. ':' .. KEYS[1]
    local requested_tickets = tonumber(ARGV[2])

    local hold_qty = redis.call('HGET', customer_hold_key, 'qty')
    if (hold_qty == nil) then
        return 0
    elseif (requested_tickets == tonumber(hold_qty)) then
        redis.call('HSET', customer_hold_key, 'state', 'PREPARE')
        redis.call('EXPIRE', customer_hold_key, tonumber(ARGV[3]))
        return 1
    else
      return 0
    end
"""

# KEYS[1] is a key of type Hash pointing to an event
# KEYS[2] is a key of type Hash pointing to the purchase
# ARGV[1] is the customer ID
# ARGV[2] is the requested number of general admission tickets
# ARGV[3] is the timestamp of the complete purchase
complete_purchase_script = """
    local customer_hold_key = 'hold:' .. ARGV[1] .. ':' .. KEYS[1]
    local requested_tickets = tonumber(ARGV[2])
    local purchase_state = redis.call('HGET', KEYS[2], 'state')

    local hold_qty = redis.call('HGET', customer_hold_key, 'qty')
    if (hold_qty == nil) then
        return 0
    elseif requested_tickets == tonumber(hold_qty) and
           purchase_state == 'AUTHORIZE' then

        -- Decrement the number of available tickets
        redis.call('HINCRBY', KEYS[1], "available:General", -requested_tickets)

        -- Delete the customer hold key
        redis.call('DEL', customer_hold_key)

        -- Set the purchase to 'COMPLETE'
        redis.call('HMSET', KEYS[2], 'state', 'COMPLETE', 'ts', ARGV[3])

        return 1
    else
      return 0
    end
"""

class TestLuaScripts(unittest.TestCase):
    def setUp(self):
        keynamehelper.set_prefix("uc06")
        self.redis = Redis(host=os.environ.get("REDIS_HOST", "localhost"),
                      port=os.environ.get("REDIS_PORT", 6379),
                      password=os.environ.get("REDIS_PASSWORD", None),
                      db=0, decode_responses=True)
        self.redis.flushdb()
        self.event_keys = self.create_events(EVENTS)
        self.customer_keys = self.create_customers(CUSTOMERS)
        self.event_key = self.event_keys[0]
        self.customer_key = self.customer_keys[0]

    def create_customers(self, cust_array):
        """Create customer keys from an array of customer details"""
        keys = []
        for cust in cust_array:
            c_key = keynamehelper.create_key_name("customer", cust['id'])
            self.redis.hmset(c_key, cust)
            keys.append(c_key)
        return keys

    def create_events(self, event_array, available=None, price=None, tier="General"):
        """ Create events from an array of event details. Provides overrides
        for the number of available tickets, price, and ticket tier."""
        e_set_key = keynamehelper.create_key_name("events")
        keys = []
        for event in event_array:
            # Override the availability & price if provided
            if available != None:
                event['available:' + tier] = available
            if price != None:
                event['price:' + tier] = price
            e_key = keynamehelper.create_key_name("event", event['sku'])
            self.redis.hmset(e_key, event)
            self.redis.sadd(e_set_key, event['sku'])
            keys.append(e_key)
        return keys

    def create_purchase(self, customer, event, quantity):
        order_id = generate.order_id()
        purchase_key = keynamehelper.create_key_name("sales_order", order_id)
        purchase = {'state':'RESERVE', 'order_id': order_id,
                    'customer_id': customer['id'], 'qty': quantity,
                    'cost':     quantity * float(event['price:General']),
                    'event_sku': event['sku'], 'ts': int(time.time())}
        self.redis.hmset(purchase_key, purchase)
        return purchase_key

    def creditcard_auth(self, customer, order_total):
      """Test function to approve/deny an authorization request"""
      # Always fails Joan's auth
      if customer.upper() == "JOAN":
        return False
      else:
        return True

    def test_modify_purchase(self):
        quantity = 5
        # Get the event and customer and generate a purchase.
        event = self.redis.hgetall(self.event_key)
        customer = self.redis.hgetall(self.customer_key)
        purchase_key = self.create_purchase(customer, event, quantity)

        # Get a reference to our Lua script
        update_purchase_state = self.redis.register_script(update_purchase_state_script)

        # At first, the purchase is in a RESERVE state
        assert(self.redis.hget(purchase_key, 'state') == "RESERVE")

        # Try to move the purchase to an invalid state
        assert(update_purchase_state([purchase_key], ["COMPLETE", int(time.time())]) == 0)
        assert(self.redis.hget(purchase_key, 'state') == "RESERVE")

        # Try to move the purchase to a valid state
        assert(update_purchase_state([purchase_key], ["AUTHORIZE", int(time.time())]) == 1)
        assert(self.redis.hget(purchase_key, 'state') == "AUTHORIZE")

        # Try to move the purchase to another valid state
        assert(update_purchase_state([purchase_key], ["COMPLETE", int(time.time())]) == 1)
        assert(self.redis.hget(purchase_key, 'state') == "COMPLETE")

    def test_reserve_tickets(self):
        # Get the customer
        customer = self.redis.hgetall(self.customer_key)

        # Get a reference to our Lua script
        request_tickets = self.redis.register_script(request_ticket_hold_v1_script)

        # Request 10 tickets and hold for 30 seconds
        result = request_tickets([self.event_key], [customer['id'], 10, 30])
        assert(result == 1)

        # Request 500 tickets and hold for 30 seconds
        result = request_tickets([self.event_key], [customer['id'], 500, 30])
        assert(result == 0)

        # Clear all hold keys
        clean_keys(self.redis, "hold")

    def test_sum_reserved_tickets(self):
        customer_ids = [c['id'] for c in CUSTOMERS]
        # Number of tickets to reserve per customer
        reserve_count = 13

        # Get a reference to our reservation and summation scripts
        request_tickets = self.redis.register_script(request_ticket_hold_v1_script)
        sum = self.redis.register_script(held_ticket_count_script)

        # Request several tickets and hold them for 30 seconds
        for customer_id in customer_ids:
            result = request_tickets([self.event_key], [customer_id, reserve_count, 30])
            assert(result == 1)

        assert(sum([self.event_key]) == reserve_count * len(customer_ids))

        # Clear all hold keys
        clean_keys(self.redis, "hold")

    def test_successful_purchase_flow(self):
        # Get the event and customer, and set up the purchase key.
        event = self.redis.hgetall(self.event_key)
        customer = self.redis.hgetall(self.customer_key)

        # Initialize the Lua scripts.
        request_tickets = self.redis.register_script(request_ticket_hold_v2_script)
        update_purchase_state = self.redis.register_script(update_purchase_state_script)
        prepare_purchase = self.redis.register_script(prepare_purchase_script)
        complete_purchase = self.redis.register_script(complete_purchase_script)

        # 1. A user requests a number of seats. Internally,
        # we attempt to set aside the requested number of seats.
        # If successful, we create a purchase, whose initial state is RESERVED.
        if (request_tickets([self.event_key], [customer['id'], 5, 10]) == 1):
            purchase_key = self.create_purchase(customer, event, 5)
        else:
            print("Failed")
            assert(False)

        # 2. The user enters payment info and authorizes us to charge
        # their credit card. First, we make sure we can complete the purchase
        # (i.e., that the user has done this within the requested time period.)
        if (prepare_purchase([self.event_key], [customer['id'], 5, 30]) == 1):
            update_purchase_state([purchase_key], ["AUTHORIZE", int(time.time())])
        else:
            # Inform the user that they did not complete the purchase in time.
            # Mark the purchase as failed.
            update_purchase_state([purchase_key], ["FAIL", int(time.time())])

        # Now, if we can authorize the credit card, then we complete the purchase.
        if self.creditcard_auth(customer['id'], 500):
            success = complete_purchase([self.event_key, purchase_key], [customer['id'], 5, int(time.time())])
        else:
            update_purchase_state([purchase_key], ["FAIL", int(time.time())])
            success = 0
            # The purchase failed. Note that we don't have to explicity
            # return inventory to the event because it will be reclaimed automatically
            # when the ticket lease expires.

        assert(success == 1)
        purchase = self.redis.hgetall(purchase_key)
        assert(purchase['state'] == "COMPLETE")

    def test_purchase_flow_with_timeout(self):
        # Get the event and customer and generate a purchase.
        event = self.redis.hgetall(self.event_key)
        customer = self.redis.hgetall(self.customer_key)

        # Initialize request and update scripts
        request_tickets = self.redis.register_script(request_ticket_hold_v2_script)
        update_purchase_state = self.redis.register_script(update_purchase_state_script)
        complete_purchase = self.redis.register_script(complete_purchase_script)

        # Request seats with a short timeout so that we can test
        # what happens when a user delays.
        if (request_tickets([self.event_key], [customer['id'], 5, 1]) == 1):
            purchase_key = self.create_purchase(customer, event, 5)

        # Simulate the user taking longer than the timeout
        time.sleep(2)

        # This should fail because we timed out.
        if (complete_purchase([self.event_key, purchase_key], [customer['id'], 5]) == 1):
            update_purchase_state([purchase_key], ["AUTHORIZE", int(time.time())])
        else:
            # Inform the user that they did not complete the purchase in time.
            # Mark the purchase as failed.
            update_purchase_state([purchase_key], ["FAIL", int(time.time())])

        purchase = self.redis.hgetall(purchase_key)
        assert(purchase['state'] == "FAIL")

def main():
  suite = unittest.TestLoader().loadTestsFromTestCase(TestLuaScripts)
  unittest.TextTestRunner(verbosity=2).run(suite)

if __name__ == "__main__":
  main()
