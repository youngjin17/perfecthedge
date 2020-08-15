import pickle
from math import ceil, log, exp
from typing import Dict, List
from cointegration_analysis import estimate_long_run_short_run_relationships

from optibook.synchronous_client import Exchange
from time import sleep

import logging
logger = logging.getLogger('client')
logger.setLevel('INFO')


ORDER_BOOK_X_ID = "TOTAL"
ORDER_BOOK_Y_ID = "UNILEVER"
SANITY_CHECK_RESET = 150
HEDGE_TIMER_RESET = 400

# Check the position before each hedge order and base hedge amount on that

# or

# Assume no missing hedge, and place limit orders for full hedge each time
#   every 200 cycles, pull all outstanding limit orders and fill missing hedge

class Autotrader:
    def __init__(self, host, username, password, log_stock_values):
        self._e = Exchange(host=host)
        self._a = self._e.connect(username=username, password=password)
        self.unilever_total_trader = Pair_Trader(self, "UNILEVER", "TOTAL", log_stock_values, 0.1, 60, "limit")
        self.lvmh_allianz_trader = Pair_Trader(self, "ALLIANZ", "LVMH", log_stock_values, 0.1, 50, "limit")
        self.asml_sap_trader = Pair_Trader(self, "ASML", "SAP", log_stock_values, 0.1, 50, "limit")

    def __del__(self):
        self._e.disconnect()

    def _log_order_book(self, instrument_id):
        pb = self._e.get_last_price_book(instrument_id)
        logger.debug("| BID |  PRICE  | ASK |")
        logger.debug("|-----|---------|-----|")
        for ask in pb.asks[::-1]:
            logger.debug("|     | {:^{width}.2f} | {:=3} |".format(ask.price, ask.volume, width=7))
        for bid in pb.bids:
            logger.debug("| {:=3} | {:^{width}.2f} |     |".format(bid.volume, bid.price, width=7))
        logger.debug("|-----|---------|-----|")
    
    def insert_order(self, order_book_id: str, price: float, volume: int, side: str, order_type: str) -> int:
        assert volume > 0 and volume < 500, "Volume must be between 0 and 500"
        assert price > 0, "Price must be greater than 0"
        return self._e.insert_order(order_book_id, price=round(price, 2), volume=volume, side=side, order_type=order_type)
    
    def delete_order(self, order_book_id: str, order_id: int) -> bool:
        assert order_id > 0
        return self._e.delete_order(order_book_id, order_id=order_id)
    
    def delete_all_orders(self, order_book_id: str) -> None:
        self._e.delete_orders(order_book_id)

    def get_order_book(self, order_book_id: str):
        return self._e.get_last_price_book(order_book_id)
    
    def get_position(self, order_book_id: str) -> int:
        return self._e.get_positions()[order_book_id]

    def start(self):
        # connection check
        if not self._e.is_connected():
            logger.error("Must be connected to exchange before calling. Quitting!")
            exit(1)

        self.unilever_total_trader.get_initial_data()
        self.lvmh_allianz_trader.get_initial_data()
        self.asml_sap_trader.get_initial_data()
        # Run the actual market operation loop
        while self._e.is_connected():
            self.unilever_total_trader.single_loop_iteration()
            self.lvmh_allianz_trader.single_loop_iteration()
            self.asml_sap_trader.single_loop_iteration()
            #sleep(0.01)

class Pair_Trader:
    def __init__(self, autotrader: Autotrader, stock_y_id: str, stock_x_id: str, log_stock_values: Dict[str, List[float]], required_credit: float, risk_limit: int = 50, hedge_type: str = "ioc"):
        assert risk_limit > 0 and risk_limit < 500, "Risk limit must be between 0 and 500" # Limits set by exchange
        assert hedge_type == "ioc" or hedge_type == "limit", "Can only hedge with limit or IOC orders"
        assert required_credit > 0, "Required credit can't be negative or we will lose money"
        self.stock_x_id = stock_x_id
        self.stock_y_id = stock_y_id
        self.required_credit = required_credit
        self._at = autotrader
        self._internal_risk_limit = risk_limit
        self._hedge_type = hedge_type
        self._c, self._gamma, _, _ = estimate_long_run_short_run_relationships(log_stock_values[stock_y_id], log_stock_values[stock_x_id])
        logger.info(f"For pair Y:{stock_y_id}, X:{stock_x_id} c is {self._c} and gamma is {self._gamma}")
        self._internal_position_x = 0
        self._internal_position_y = 0
        self._missing_hedge = 0
        self._hedge_timer = 0
        self._limit_order_out = False
        self._sanity_check_counter = SANITY_CHECK_RESET
    
    def get_initial_data(self):
        self._internal_position_x = self._at.get_position(self.stock_x_id)
        self._internal_position_y = self._at.get_position(self.stock_y_id)

        order_book_x = self._at.get_order_book(self.stock_x_id)
        order_book_y = self._at.get_order_book(self.stock_y_id)

        if len(order_book_x.asks) == 0 or len(order_book_y.bids) == 0:
            logger.error(f"Could not get orderbook for pair Y:{self.stock_y_id} x:{self.stock_x_id} initialization. Exiting!")
            exit(2)

        correct_position_y = self._calculate_hedge_amount(order_book_x.asks[0].price, order_book_y.bids[0].price, self._internal_position_x)
        self._missing_hedge = correct_position_y - self._internal_position_y

        logger.info(f"Collected all necessary information. {self.stock_y_id} position {self._internal_position_y} and {self.stock_x_id} position {self._internal_position_x} with missing hedge {self._missing_hedge}")
    
    def single_loop_iteration(self):
        if self._hedge_type == "limit" and self._missing_hedge != 0 and self._limit_order_out:
            self._process_limit_hedges()
        
        if self._sanity_check_counter == 0:
            self._sanity_check()
            self._sanity_check_counter = SANITY_CHECK_RESET
        else:
            self._sanity_check_counter -= 1
        
        order_book_x = self._at.get_order_book(self.stock_x_id)
        order_book_y = self._at.get_order_book(self.stock_y_id)

        if self._hedge_timer > 0:
            self._hedge_timer -= 1
        if self._missing_hedge != 0 and self._hedge_timer == 0:
            # Do something to hedge
            # self._fill_hedges(order_book_y)
            pass

        if len(order_book_y.bids) > 0 and len(order_book_x.asks) > 0:
            y_t = log(order_book_y.bids[0].price)
            x_t = log(order_book_x.asks[0].price)
            z_t = y_t - self._c - self._gamma * x_t
            implied_y_t = self._c + self._gamma * x_t
            implied_Y = exp(implied_y_t)
            credit = order_book_y.bids[0].price - implied_Y
            # logger.info(f"z_t for buying x, selling y: {z_t}. Seeing credit of {credit}")
            if (
                (credit > self.required_credit or (credit > 0 and self._internal_position_y > 0)) and
                self._internal_position_y > -self._internal_risk_limit and
                order_book_y.bids[0].volume >= 20
            ):
                # Insert Bid on X, Hedge with Ask on Y
                # logger.info(f"when selling {self.stock_y_id} and buying {self.stock_x_id}, y_t = {y_t} x_t = {x_t}, z_t = {z_t}. Credit is {credit}")
                self._buy_x_sell_y(order_book_x, order_book_y)

                # it's likely market has since moved, so get price books again
                order_book_x = self._at.get_order_book(self.stock_x_id)
                order_book_y = self._at.get_order_book(self.stock_y_id)
        
        if len(order_book_y.asks) > 0 and len(order_book_x.bids) > 0:
            y_t = log(order_book_y.asks[0].price)
            x_t = log(order_book_x.bids[0].price)
            z_t = y_t - self._c - self._gamma * x_t
            implied_y_t = self._c + self._gamma * x_t
            implied_Y = exp(implied_y_t)
            credit = implied_Y - order_book_y.asks[0].price
            # logger.info(f"z_t for selling x, buying y: {z_t}. Seeing credit of {credit}")
            if (
                (credit > self.required_credit or (credit > 0 and self._internal_position_y < 0)) and 
                self._internal_position_y < self._internal_risk_limit and 
                order_book_y.asks[0].volume >= 20
            ):
                # Insert Ask on X, Hedge with Bid on Y
                # logger.info(f"when buying {self.stock_y_id} and selling {self.stock_x_id}, y_t = {y_t} x_t = {x_t}, z_t = {z_t}. Credit is {credit}")
                self._sell_x_buy_y(order_book_x, order_book_y)
    
    def _calculate_hedge_amount(self, price_x: float, price_y: float, quantity_x: int) -> int:
        return round((self._gamma * price_x / price_y) * -quantity_x)
    
    def _sanity_check(self):
        if self._at.get_position(self.stock_x_id) != self._internal_position_x:
            logger.error(f"Actual {self.stock_x_id} position is {self._at.get_position(self.stock_x_id)}, not self.internal_position_x {self._internal_position_x}")
            exit(1)
        if self._at.get_position(self.stock_y_id) != self._internal_position_y:
            logger.error(f"Actual Y position is {self._at.get_position(self.stock_y_id)}, not self.internal_position_y {self._internal_position_y}")
            exit(1)
        
        # Check for proper missing hedge count
        order_book_x = self._at.get_order_book(self.stock_x_id)
        order_book_y = self._at.get_order_book(self.stock_y_id)
        if not len(order_book_x.asks) or not len(order_book_y.bids): return

        correct_position_y = self._calculate_hedge_amount(order_book_x.asks[0].price, order_book_y.bids[0].price, self._internal_position_x)
        if (self._missing_hedge > correct_position_y - self._internal_position_y + 3) or (self._missing_hedge < correct_position_y - self._internal_position_y - 3):
            logger.error(f"Missing hedge is {self._missing_hedge}, but the two positions are X({self.stock_x_id}): {self._internal_position_x}, Y({self.stock_y_id}): {self._internal_position_y}. Calculated missing hedge is {correct_position_y - self._internal_position_y}")
            # exit(1)
        self._missing_hedge = correct_position_y - self._internal_position_y
        logger.debug("Passed sanity check.")
    
    def _buy_x_sell_y(self, order_book_x, order_book_y):
        #insert bid on total
        self._at.insert_order(self.stock_x_id, price=order_book_x.asks[0].price, volume=6, side="bid", order_type="ioc")
        
        # look at how much total we got
        post_trade_position_x = self._at.get_position(self.stock_x_id)
        change_in_position = post_trade_position_x - self._internal_position_x
        self._internal_position_x = post_trade_position_x

        # Hedge by asking on Y
        if change_in_position != 0:
            # calculate ratio based on gamma * Y_t/X_t. i.e. for every 1 lot in X you want to hedge with 'ratio' lots of Y
            volume_to_hedge = self._calculate_hedge_amount(order_book_x.asks[0].price, order_book_y.bids[0].price, -change_in_position) #round((self._unilever_total_gamma * price_bookY.bids[0].price / price_bookX.asks[0].price) * change_in_position)
            hedge_to_issue = volume_to_hedge - self._missing_hedge #volume_to_hedge = 16, missing_hedge = -32, want to hedge -16
            if hedge_to_issue > 0:
                # insert ask on unilever
                self._at.insert_order(self.stock_y_id, price=order_book_y.bids[0].price, volume=hedge_to_issue, side="ask", order_type=self._hedge_type)
                logger.info(f"We aquired {change_in_position} new {self.stock_x_id} for the price of {order_book_x.asks[0].price}")
                logger.info(f"We are trying to hedge {hedge_to_issue} by selling on {self.stock_y_id} at price {order_book_y.bids[0].price}. Missing_hedge is currently {self._missing_hedge}")
                self._missing_hedge = 0
                # look at how much y we got, wait for ioc trade
                post_trade_position_y = self._at.get_position(self.stock_y_id)
                change_in_position = post_trade_position_y - self._internal_position_y
                logger.info(f"Change in position from selling {self.stock_y_id} is {change_in_position}")
                self._internal_position_y = post_trade_position_y
                self._missing_hedge = -(hedge_to_issue + change_in_position) # hedge_to_issue = 16, change_in_position = -4, new missing hedge = -12
                # handle missed hedge at a later time
                if self._missing_hedge: 
                    logger.warning(f"We missed some hedge. Still need to hedge {self._missing_hedge} on {self.stock_y_id}")
                    self._limit_order_out = (self._hedge_type == "limit")
                    self._hedge_timer = HEDGE_TIMER_RESET
            else:
                logger.info(f"We aquired {change_in_position} new {self.stock_x_id} for the price of {order_book_x.asks[0].price}")
                logger.info(f"Avoided hedging on {self.stock_y_id} when it would be ill-advised.")
                logger.info(f"Missing hedge is {self._missing_hedge}, volume to hedge is {volume_to_hedge}. Setting new missing hedge to {-hedge_to_issue}")
                self._missing_hedge = -hedge_to_issue
        else:
            logger.info(f"Inserted IOC bid on {self.stock_x_id} at price {order_book_x.asks[0].price} for quantity {6} but missed")
    
    def _sell_x_buy_y(self, order_book_x, order_book_y):
        self._at.insert_order(self.stock_x_id, price=order_book_x.bids[0].price, volume=6, side="ask", order_type="ioc")
        # look at how much total we got
        post_trade_position_x = self._at.get_position(self.stock_x_id)
        change_in_position = post_trade_position_x - self._internal_position_x
        self._internal_position_x = post_trade_position_x

        # Hedge by bidding on Y
        if change_in_position != 0:
            # calculate ratio based on gamma * Y_t/X_t. i.e. for every 1 lot in X you want to hedge with 'ratio' lots of Y
            volume_to_hedge = self._calculate_hedge_amount(order_book_x.bids[0].price, order_book_y.asks[0].price, change_in_position)
            hedge_to_issue = volume_to_hedge + self._missing_hedge #volume_to_hedge = 5, missing_hedge = 7, want to hedge 12
            if hedge_to_issue > 0:
                # insert bid on unilever
                self._at.insert_order(self.stock_y_id, price=order_book_y.asks[0].price, volume=hedge_to_issue, side="bid", order_type=self._hedge_type)
                logger.info(f"We aquired {change_in_position} new {self.stock_x_id} for the price of {order_book_x.bids[0].price}")
                logger.info(f"We are trying to hedge {hedge_to_issue} by buying on {self.stock_y_id} at price {order_book_y.asks[0].price}. Missing_hedge is currently {self._missing_hedge}")
                self._missing_hedge = 0
                # look at how much unilever we got, wait for ioc trade
                post_trade_position_y = self._at.get_position(self.stock_y_id)
                change_in_position = post_trade_position_y - self._internal_position_y
                logger.info(f"Change in position from buying {self.stock_y_id} is {change_in_position}")
                self._internal_position_y = post_trade_position_y
                self._missing_hedge = hedge_to_issue - change_in_position
                # handle missed hedge at a later time
                if self._missing_hedge:
                    logger.warning(f"We missed some hedge. Still need to hedge {self._missing_hedge} on {self.stock_y_id}")
                    self._limit_order_out = (self._hedge_type == "limit")
                    self._hedge_timer = HEDGE_TIMER_RESET
            else:
                logger.info(f"We aquired {change_in_position} new {self.stock_x_id} for the price of {order_book_x.bids[0].price}")
                logger.info(f"Avoided hedging on {self.stock_y_id} when it would be ill-advised.")
                logger.info(f"Missing hedge is {self._missing_hedge}, volume to hedge is {volume_to_hedge}. Setting new missing hedge to {-hedge_to_issue}")
                self._missing_hedge = hedge_to_issue
        else:
            logger.info(f"Inserted IOC ask on {self.stock_x_id} at price {order_book_x.bids[0].price} for quantity {6} but missed")
    
    def _fill_hedges(self, order_book_y):
        if len(order_book_y.bids) > 0 and self._missing_hedge < 0:
            self._at.insert_order(self.stock_y_id, price=order_book_y.bids[0].price, volume=-self._missing_hedge, side="ask", order_type="ioc")
            logger.info(f"Filling out our hedge on {self.stock_y_id} by selling y. Inserting ask at {order_book_y.bids[0].price} for missing hedge quantity {self._missing_hedge}")
        if len(order_book_y.asks) > 0 and self._missing_hedge > 0:
            self._at.insert_order(self.stock_y_id, price=order_book_y.asks[0].price, volume=self._missing_hedge, side="bid", order_type="ioc")
            logger.info(f"Filling out our hedge on {self.stock_y_id} by buying y. Inserting bid at {order_book_y.asks[0].price} for missing hedge quantity {self._missing_hedge}")
        
        post_trade_position_y = self._at.get_position(self.stock_y_id)
        change_in_position = post_trade_position_y - self._internal_position_y
        self._missing_hedge = self._missing_hedge - change_in_position
        self._internal_position_y = post_trade_position_y
        logger.info(f"Change in position from hedge fill is {change_in_position}. Setting new missing hedge to {self._missing_hedge}")
    
    def _process_limit_hedges(self):
        self._at.delete_all_orders(self.stock_y_id)
        post_limit_position_y = self._at.get_position(self.stock_y_id)
        change_in_position = post_limit_position_y - self._internal_position_y
        self._internal_position_y = post_limit_position_y
        self._missing_hedge = self._missing_hedge - change_in_position

        logger.info(f"Pulling limit orders from hedge {self.stock_y_id}. Leaving limit out aquired {change_in_position} shares compared to required {self._missing_hedge}")
        self._limit_order_out = False

        if self._missing_hedge != 0:
            # Still need to insert IOC for trade
            order_book_y = self._at.get_order_book(self.stock_y_id)

            if self._missing_hedge < 0:
                if len(order_book_y.bids) == 0: return # can't hedge w/o price
                self._at.insert_order(self.stock_y_id, price=order_book_y.bids[0].price, volume=-self._missing_hedge, side="ask", order_type="ioc")
                logger.info(f"Inserting IOC ask on {self.stock_y_id} to finish filling hedge price {order_book_y.bids[0].price} quantity {self._missing_hedge}")
            else:
                if len(order_book_y.asks) == 0: return # can't hedge w/o price
                self._at.insert_order(self.stock_y_id, price=order_book_y.asks[0].price, volume=self._missing_hedge, side="bid", order_type="ioc")
                logger.info(f"Inserting IOC bid on {self.stock_y_id} to finish filling hedge price {order_book_y.asks[0].price} quantity {self._missing_hedge}")
            
            post_limit_position_y = self._at.get_position(self.stock_y_id)
            change_in_position = post_limit_position_y - self._internal_position_y
            self._internal_position_y = post_limit_position_y
            self._missing_hedge = self._missing_hedge - change_in_position

            logger.info(f"IOC hedge on {self.stock_y_id} after incomplete limits yielded {change_in_position} new quanity. Missing hedge is now {self._missing_hedge}")

# Used to initialize values
def read_data(timestamps_pckl, stock_values_pckl):
    with open(timestamps_pckl, 'rb') as f:
        timestamps = pickle.load(f)
    with open(stock_values_pckl, 'rb') as f:
        stock_values = pickle.load(f)
    return timestamps, stock_values

if __name__ == "__main__":
    # Load data into timestmaps and stock_values.
    timestamps, stock_values = read_data('timestamps.pckl', 'stock_values.pckl')

    # Create a new dictionary called log_stock_values with a similar structure to 
    # stock_values, but containing the log of these instead.
    log_stock_values = {name: [log(v) for v in stock_values[name]] for name in stock_values}
    at = Autotrader("chi-tech-starters.optibook.net", "PerfectHedge", "2v3gy923nr", log_stock_values)
    at.start()