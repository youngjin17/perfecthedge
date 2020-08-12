import pickle
from math import ceil, log
from cointegration_analysis import estimate_long_run_short_run_relationships

from optibook.synchronous_client import Exchange
from time import sleep

import logging
logger = logging.getLogger('client')
logger.setLevel('INFO')


ORDER_BOOK_X_ID = "TOTAL"
ORDER_BOOK_Y_ID = "UNILEVER"
SANITY_CHECK_RESET = 250

class Autotrader:
    def __init__(self, host, username, password, log_stock_values):
        self._e = Exchange(host=host)
        self._a = self._e.connect(username=username, password=password)
        self._internal_position_total = 0
        self._internal_position_unilever = 0
        self._missing_hedge = 0     # positive means we need more A. negative means we need to sell some A.
        self._risk_limit = 400
        self._max_order_volume = self._risk_limit / 10
        self._max_hedge_disconformity = 100
        self._last_issued_hedge = 0
        self._sanity_check_counter = SANITY_CHECK_RESET
        self._unilever_total_c = 0
        self._unilever_total_gamma = 0
        self._unilever_total_c, self._unilever_total_gamma, _, _ = estimate_long_run_short_run_relationships(log_stock_values[ORDER_BOOK_Y_ID], log_stock_values[ORDER_BOOK_X_ID])
        self._unilever_total_last_side = "bid" #TODO: remove
        logger.info(f"For pair Y:{ORDER_BOOK_Y_ID}, X:{ORDER_BOOK_X_ID} c is {self._unilever_total_c} and gamma is {self._unilever_total_gamma}")

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

    def start(self):
        # connection check
        if not self._e.is_connected():
            logger.error("Must be connected to exchange before calling. Quitting!")
            exit(1)

        # poll trade ticks to make sure its empty
        # self._e.poll_new_trades(self._instrument_id)
        # starting_positions = self._e.get_positions()
        # self._internal_position_a = starting_positions[ORDER_BOOK_A_ID]
        # self._internal_position_b = starting_positions[ORDER_BOOK_B_ID]
        # logger.info(f"Starting with exchange positions A: {self._internal_position_a} and B: {self._internal_position_b}")
        # self._missing_hedge = -(self._internal_position_a + self._internal_position_b)
        # logger.info(f"Still need to hedge {self._missing_hedge}")
        self._e.poll_new_trades(ORDER_BOOK_X_ID)
        self._e.poll_new_trades(ORDER_BOOK_Y_ID)
        positions = self._e.get_positions()
        self._internal_position_unilever = positions[ORDER_BOOK_Y_ID]
        self._internal_position_total = positions[ORDER_BOOK_X_ID]
        price_bookY = self._e.get_last_price_book(ORDER_BOOK_Y_ID)
        price_bookX = self._e.get_last_price_book(ORDER_BOOK_X_ID)

        if  not len(price_bookX.bids) or not len(price_bookX.asks) or not len(price_bookY.bids) or not len(price_bookY.asks):
            logger.error("Could not fill pricebook on startup. Quitting")
            exit(2)

        # get positions and prices
        correct_position_x = self._internal_position_total                                  # Sell 100 X
        correct_position_y = round(self._calculate_hedge_ratio(price_bookX.asks[0].price, price_bookY.bids[0].price) * -correct_position_x)         # Buy 50 Y

        self._missing_hedge = correct_position_y - self._internal_position_unilever         # HAve position 25 Y, Missing hedge is 25
        logger.info(f"Collected all necessary information. Starting with {ORDER_BOOK_X_ID} position {self._internal_position_total} and {ORDER_BOOK_Y_ID} position {self._internal_position_unilever}. Missing hedge is {self._missing_hedge}")

        # Run the actual market operation loop
        while self._e.is_connected:
            self._loop()
            #sleep(0.25)
        
    def _calculate_hedge_ratio(self, price_x, price_y):
        #volume_to_hedge = round((self._unilever_total_gamma * price_bookY[0].price / price_bookX[0].price) * change_in_position)
        return self._unilever_total_gamma * price_y / price_x
        

    # Sanity check on positions: If we screwed up, correct it and log it
    def _sanity_check(self, price_bookX, price_bookY):
        #posList = self._e.get_positions()

        # if posList[ORDER_BOOK_X_ID] != self._internal_position_total:
        #     logger.error(f"Actual X position is { posList[ORDER_BOOK_X_ID]}, not self.internal_position_total {self._internal_position_total}")
        #     self._internal_position_a = posList[ORDER_BOOK_X_ID]
        #     exit(1)
        # if posList[ORDER_BOOK_Y_ID] != self._internal_position_unilever:
        #     logger.error(f"Actual Y position is { posList[ORDER_BOOK_Y_ID]}, not self.internal_position_b {self._internal_position_b}")
        #     self._internal_position_b = posList[ORDER_BOOK_B_ID]
        #     exit(1)
        if not len(price_bookX.bids) or not len(price_bookY.asks): return
        correct_position_y = round(self._calculate_hedge_ratio(price_bookX.asks[0].price, price_bookY.bids[0].price) * -self._internal_position_total)
        if self._missing_hedge != correct_position_y - self._internal_position_unilever:
            logger.error(f"Missing hedge is {self._missing_hedge}, but the two positions are X: {self._internal_position_total}, B: {self._internal_position_unilever}")
            exit(1)

        self._missing_hedge = correct_position_y - self._internal_position_unilever
        logger.debug("Passed sanity check.")


    def _credit_volume_requirement(self, side_to_trade, available_volume):
        """
        Determines the minimum credit to ask for and maximum volume to shoot for based on internal position
        """
        MULTIPLIER = 1
        # Accept multiplier tick credits if it helps get us back to an even position
        if side_to_trade == "bid" and self._internal_position_b <= 0:
            return 0.1 * MULTIPLIER, min(self._max_order_volume, available_volume)
        elif side_to_trade == "ask" and self._internal_position_b >= 0:
            return 0.1 * MULTIPLIER, min(self._max_order_volume, available_volume)
        # Ask for more credit if we've already got a position
        if side_to_trade == "bid" and self._internal_position_b < self._risk_limit / 1.5:
            return 0.2 * MULTIPLIER, ceil(min(self._max_order_volume, available_volume) / 1.5)
        elif side_to_trade == "ask" and self._internal_position_b > self._risk_limit / -1.5:
            return 0.2 * MULTIPLIER, ceil(min(self._max_order_volume, available_volume) / 1.5)
        # Bigger position, bigger credit
        if side_to_trade == "bid" and self._internal_position_b < self._risk_limit / 2:
            return 0.4 * MULTIPLIER, ceil(min(self._max_order_volume, available_volume) / 2)
        elif side_to_trade == "ask" and self._internal_position_b > self._risk_limit / 2:
            return 0.4 * MULTIPLIER, ceil(min(self._max_order_volume, available_volume) / 2)
        # Biggest Position, MOAR CREDIT
        if side_to_trade == "bid" and self._internal_position_b < self._risk_limit:
            return MULTIPLIER, ceil(min(self._max_order_volume, available_volume) / 4)
        elif side_to_trade == "ask" and self._internal_position_b > -self._risk_limit:
            return MULTIPLIER, ceil(min(self._max_order_volume, available_volume) / 4)
        # Outside of risk limit
        return 100 * MULTIPLIER, 1 # Should never execute

    def _trim_hedges(self, order_book_a, order_book_b):
        """
        Basic hedge trimmer. This brings a hedge outside of our risk limits back into compliance by just taking whatever the
        current market rate for a hedge is.
        """
        if self._missing_hedge > 0 and len(order_book_a.asks) != 0:
            # Buy to hack out of hedge
            self._e.insert_order(ORDER_BOOK_A_ID, price=order_book_a.asks[0].price, volume=round(self._missing_hedge/2), side="bid", order_type="ioc")
            logger.info(f"Attempting to bid way out of hedge issue. Bidding {round(self._missing_hedge/2)} lots for {order_book_a.asks[0].price}")
            trade_result_a = self._e.poll_new_trades(ORDER_BOOK_A_ID)
            total_hedged = 0
            for trades in trade_result_a:
                total_hedged = total_hedged + trades.volume
            self._missing_hedge -= total_hedged
            self._internal_position_a += total_hedged
            logger.info(f"Reduced missing hedge by {total_hedged}")
        elif len(order_book_a.bids) != 0:
            self._e.insert_order(ORDER_BOOK_A_ID, price=order_book_a.bids[0].price, volume=-round(self._missing_hedge/2), side="ask", order_type="ioc")
            logger.info(f"Attempting to ask way out of hedge issue. Asking {-round(self._missing_hedge/2)} lots for {order_book_a.bids[0].price}")
            trade_result_a = self._e.poll_new_trades(ORDER_BOOK_A_ID)
            total_hedged = 0
            for trades in trade_result_a:
                total_hedged = total_hedged + trades.volume
            self._missing_hedge += total_hedged
            self._internal_position_a -= total_hedged
            logger.info(f"Reduced missing hedge by {total_hedged}")

    def _pull_illiquid_orders(self):
        """
        Removes all outstanding limit orders from the illiquid market (order book b)
        """
        outstanding = self._e.get_outstanding_orders(ORDER_BOOK_B_ID)
        for o in outstanding.values():
            self._e.delete_order(ORDER_BOOK_B_ID, order_id=o.order_id)
    
    def _pull_hedge_orders(self):
        """
        Removes all outstanding limit orders from the hedge market (order book a)
        """
        outstanding = self._e.get_outstanding_orders(ORDER_BOOK_A_ID)
        for o in outstanding.values():
            result = self._e.delete_order(ORDER_BOOK_A_ID, order_id=o.order_id)
    
    def _pull_all_orders(self):
        """
        Removes all outstanding limit orders in both order books
        """
        self._pull_illiquid_orders()
        self._pull_hedge_orders()
    
    # Non implemented functions
    def _loop(self):
        """
        The actual market operation loop for each 
        """

        price_bookY = self._e.get_last_price_book(ORDER_BOOK_Y_ID)
        price_bookX = self._e.get_last_price_book(ORDER_BOOK_X_ID)
        if self._sanity_check_counter == 0:
            self._sanity_check(price_bookX, price_bookY)
            self._sanity_check_counter = SANITY_CHECK_RESET
        else:
            self._sanity_check_counter -= 1

        #check if sell y, then buy x
        if len(price_bookY.bids) > 0 and len(price_bookX.asks) > 0:
            y_t = log(price_bookY.bids[0].price)
            x_t = log(price_bookX.asks[0].price)
            z_t = y_t - self._unilever_total_c - self._unilever_total_gamma * x_t
            if z_t > 0.001 and self._unilever_total_last_side != "bid":
                #insert bid on total
                self._e.insert_order(ORDER_BOOK_X_ID, price=price_bookX.asks[0].price, volume=6, side="bid", order_type="ioc")
                logger.info(f"when selling unilever and buying total, y_t = {y_t} x_t = {x_t}, z_t = {z_t}")
                logger.info(f"Inserting IOC bid on {ORDER_BOOK_X_ID} at price {price_bookX.asks[0].price} for quantity {6}")
                # look at how much total we got
                post_trade_position_total = self._e.get_positions()[ORDER_BOOK_X_ID]
                change_in_position = post_trade_position_total - self._internal_position_total
                self._internal_position_total = post_trade_position_total

                # Hedge by asking on Y
                if change_in_position != 0:
                    logger.info(f"We aquired {change_in_position} new {ORDER_BOOK_X_ID} for the price of {price_bookX.asks[0].price}")
                    # calculate ratio based on gamma * Y_t/X_t. i.e. for every 1 lot in X you want to hedge with 'ratio' lots of Y
                    volume_to_hedge = round((self._unilever_total_gamma * price_bookY.bids[0].price / price_bookX.asks[0].price) * change_in_position)
                    hedge_to_issue = volume_to_hedge - self._missing_hedge #volume_to_hedge = 5, missing_hedge = -7, want to hedge 12
                    self._unilever_total_last_side = "bid"
                    if hedge_to_issue > 0:
                        # insert ask on unilever
                        self._e.insert_order(ORDER_BOOK_Y_ID, price=price_bookY.bids[0].price, volume=hedge_to_issue, side="ask", order_type="ioc")
                        self._missing_hedge = 0
                        # look at how much unilever we got, wait for ioc trade
                        post_trade_position_unilever = self._e.get_positions()[ORDER_BOOK_Y_ID]
                        change_in_position = post_trade_position_unilever - self._internal_position_unilever
                        logger.info(f"Change in position from selling Y is {change_in_position}")
                        self._internal_position_unilever = post_trade_position_unilever
                        self._missing_hedge = hedge_to_issue + change_in_position
                        # handle missed hedge at a later time
                        if self._missing_hedge: logger.warning(f"We missed some hedge. Still need to hedge {self._missing_hedge} on {ORDER_BOOK_Y_ID}")
                    else:
                        logger.info(f"Avoided hedging on {ORDER_BOOK_Y_ID} when it would be ill-advised.")
                        logger.info(f"Missing hedge is {self._missing_hedge}, volume to hedge is {volume_to_hedge}. Setting new missing hedge to {-hedge_to_issue}")
                        self._missing_hedge = -hedge_to_issue
        # Buy Y, sell X
        if len(price_bookY.asks) > 0 and len(price_bookX.bids) > 0:
            y_t = log(price_bookY.asks[0].price)
            x_t = log(price_bookX.bids[0].price)
            z_t = y_t - self._unilever_total_c - self._unilever_total_gamma * x_t
            if z_t < -0.001 and self._unilever_total_last_side != "ask":
                #insert bid on total
                self._e.insert_order(ORDER_BOOK_X_ID, price=price_bookX.bids[0].price, volume=6, side="ask", order_type="ioc")
                logger.info(f"when buying unilever and selling total, y_t = {y_t} x_t = {x_t}, z_t = {z_t}")
                logger.info(f"Inserting IOC ask on {ORDER_BOOK_X_ID} at price {price_bookX.bids[0].price} for quantity {6}")
                # look at how much total we got
                post_trade_position_total = self._e.get_positions()[ORDER_BOOK_X_ID]
                change_in_position = post_trade_position_total - self._internal_position_total
                self._internal_position_total = post_trade_position_total

                # Hedge by bidding on Y
                if change_in_position != 0:
                    logger.info(f"We aquired {change_in_position} new {ORDER_BOOK_X_ID} for the price of {price_bookX.bids[0].price}")
                    # calculate ratio based on gamma * Y_t/X_t. i.e. for every 1 lot in X you want to hedge with 'ratio' lots of Y
                    volume_to_hedge = round((self._unilever_total_gamma * price_bookY.asks[0].price / price_bookX.bids[0].price) * -change_in_position)
                    hedge_to_issue = volume_to_hedge + self._missing_hedge #volume_to_hedge = 5, missing_hedge = 7, want to hedge 12
                    self._unilever_total_last_side = "ask"
                    if hedge_to_issue > 0:
                        # insert bid on unilever
                        self._e.insert_order(ORDER_BOOK_Y_ID, price=price_bookY.asks[0].price, volume=hedge_to_issue, side="bid", order_type="ioc")
                        self._missing_hedge = 0
                        # look at how much unilever we got, wait for ioc trade
                        post_trade_position_unilever = self._e.get_positions()[ORDER_BOOK_Y_ID]
                        change_in_position = post_trade_position_unilever - self._internal_position_unilever
                        logger.info(f"Change in position from buying Y is {change_in_position}")
                        self._internal_position_unilever = post_trade_position_unilever
                        self._missing_hedge = hedge_to_issue - change_in_position
                        # handle missed hedge at a later time
                        if self._missing_hedge: logger.warning(f"We missed some hedge. Still need to hedge {self._missing_hedge} on {ORDER_BOOK_Y_ID}")
                    else:
                        logger.info(f"Avoided hedging on {ORDER_BOOK_Y_ID} when it would be ill-advised.")
                        logger.info(f"Missing hedge is {self._missing_hedge}, volume to hedge is {volume_to_hedge}. Setting new missing hedge to {-hedge_to_issue}")
                        self._missing_hedge = hedge_to_issue
            
        
    def _process_hedge_trades(self):
        """
        This handles the cleanup of any hedge trades. The side effects for each differ slightly based on making or taking.
        """
        raise NotImplementedError

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