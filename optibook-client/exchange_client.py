# Copyright (c) Optiver I.P. B.V. 2019

import logging
import json
import itertools
from datetime import datetime
from collections import defaultdict, deque
from .base_client import Client, RawClient
from .common_types import PriceBook, PriceVolume, Trade, TradeTick, OrderStatus, Instrument

import capnp
from .idl import exec_capnp, info_capnp, common_capnp

DEFAULT_HOST = 'opamux0674'
DEFAULT_INFO_PORT = '7001'
DEFAULT_EXEC_PORT = '8001'

logger = logging.getLogger('client')

SIDE_BID = 'bid'
SIDE_ASK = 'ask'
ALL_SIDES = [SIDE_BID, SIDE_ASK]

ACTION_BUY = 'buy'
ACTION_SELL = 'sell'
ALL_ACTIONS = [ACTION_BUY, ACTION_SELL]

ORDER_TYPE_LIMIT = 'limit'
ORDER_TYPE_IOC = 'ioc'
ALL_ORDER_TYPES = [ORDER_TYPE_LIMIT, ORDER_TYPE_IOC]


class InfoClient(RawClient):
    def __init__(self, host, port, max_nr_trade_history=100, admin_password=None):
        super(InfoClient, self).__init__(host, port)

        self._admin_password = admin_password
        self._max_trade_history = max_nr_trade_history

    def _new_request_id(self):
        req_id = self._request_id
        self._request_id += 1
        return req_id

    def reset_data(self):
        super(InfoClient, self).reset_data()
        self._last_price_book_by_instrument_id = dict()

        self._trade_tick_history_last_polled_index = defaultdict(lambda: 0)
        self._trade_tick_history = defaultdict(deque)
        self._instruments = {}

    async def _on_connected(self):
        msg = common_capnp.RawMessage.new_message()
        msg.type = info_capnp.InfoSubscribeRequest.schema.node.id
        subscribe = info_capnp.InfoSubscribeRequest.new_message()
        subscribe.requestId = self._new_request_id()
        subscribe.bookUpdateType = 'price'
        if self._admin_password is not None:
            subscribe.adminPassword = self._admin_password
        msg.msg = subscribe
        await self.send_request(subscribe.requestId, msg)
        logger.debug('logged in!')

    async def _on_message(self, msg):
        if msg.type == info_capnp.PriceBook.schema.node.id:
            self.onPriceBook(msg.msg.as_struct(info_capnp.PriceBook.schema))
        elif msg.type == common_capnp.TradeTick.schema.node.id:
            self.onTradeTick(msg.msg.as_struct(common_capnp.TradeTick.schema))
        elif msg.type == info_capnp.InstrumentCreated.schema.node.id:
            self.onInstrumentCreated(msg.msg.as_struct(info_capnp.InstrumentCreated.schema))
        elif msg.type == info_capnp.InstrumentExpired.schema.node.id:
            self.onInstrumentExpired(msg.msg.as_struct(info_capnp.InstrumentExpired.schema))
        elif msg.type == info_capnp.InstrumentPaused.schema.node.id:
            self.onInstrumentPaused(msg.msg.as_struct(info_capnp.InstrumentPaused.schema))
        elif msg.type == info_capnp.InstrumentResumed.schema.node.id:
            self.onInstrumentResumed(msg.msg.as_struct(info_capnp.InstrumentResumed.schema))
        else:
            raise Exception(f"Unknown message from server {msg}")

    def onInstrumentCreated(self, msg):
        i = Instrument()
        i.id = msg.instrumentId
        i.tick_size = msg.tickSize
        i.extra_info = json.loads(msg.extraInfo)
        self._instruments[msg.instrumentId] = i

    def onInstrumentExpired(self, msg):
        del self._instruments[msg.instrumentId]

    def onInstrumentPaused(self, msg):
        self._instruments[msg.instrumentId].paused = True

    def onInstrumentResumed(self, msg):
        self._instruments[msg.instrumentId].paused = False

    def onPriceBook(self, priceBook):
        pb = PriceBook(instrument_id=priceBook.instrumentId, bids=[PriceVolume(r.price, r.volume) for r in priceBook.bids],
                       asks=[PriceVolume(r.price, r.volume) for r in priceBook.asks])
        pb.timestamp = datetime.now()
        self._last_price_book_by_instrument_id[priceBook.instrumentId] = pb

    def onTradeTick(self, trade):
        t = TradeTick()
        t.instrument_id = trade.instrumentId
        t.volume = trade.volume
        t.price = trade.price
        t.aggressor_side = str(trade.aggressorSide)
        t.timestamp = datetime.fromtimestamp(trade.timestamp // 1000000000)
        t.buyer = trade.buyer
        t.seller = trade.seller
        inst_hist = self._trade_tick_history[t.instrument_id]
        inst_hist.append(t)
        while len(inst_hist) > self._max_trade_history:
            inst_hist.popleft()
            self._trade_tick_history_last_polled_index[t.instrument_id] = max(
                self._trade_tick_history_last_polled_index[t.instrument_id] - 1, 0)

    def get_last_price_book(self, instrument_id):
        return self._last_price_book_by_instrument_id.get(instrument_id, None)

    def get_trade_tick_history(self, instrument_id):
        return list(self._trade_tick_history.get(instrument_id, []))

    def poll_new_trade_ticks(self, instrument_id):
        new_trade_ticks = list(itertools.islice(self._trade_tick_history[instrument_id],
                                                self._trade_tick_history_last_polled_index[instrument_id],
                                                len(self._trade_tick_history[instrument_id])))
        self._trade_tick_history_last_polled_index[instrument_id] = len(self._trade_tick_history[instrument_id])
        return new_trade_ticks

    def clear_trade_tick_history(self):
        self._trade_tick_history = defaultdict(deque)

    def get_instruments(self):
        return self._instruments


class PositionAccountant:
    def __init__(self, positions=defaultdict()):
        self._position_by_instrument_id = {}
        for inst in positions:
            self._position_by_instrument_id[inst.instrumentId] = { 'volume' : inst.position, 'cash' : inst.cash }

    def handle_trade(self, trade):
        logger.debug(f'Private trade: {trade}.')

        if trade.side == 'bid':
            sidemult = 1
        elif trade.side == 'ask':
            sidemult = -1
        else:
            raise Exception('Unknown trade side.')

        if trade.instrumentId not in self._position_by_instrument_id:
            self._position_by_instrument_id[trade.instrumentId] = { 'volume' : 0, 'cash' : 0.0 }
        self._position_by_instrument_id[trade.instrumentId]['volume'] += sidemult * trade.volume
        self._position_by_instrument_id[trade.instrumentId]['cash'] -= sidemult * trade.volume * trade.price

    def handle_single_sided_booking(self, ssb):
        logger.debug(f'Single sided booking: {ssb}')

        if ssb.action == ACTION_BUY:
            sidemult = 1
        elif ssb.action == ACTION_SELL:
            sidemult = -1
        else:
            raise Exception('Unknown action: ' + str(ssb.action))

        if ssb.instrumentId not in self._position_by_instrument_id:
            self._position_by_instrument_id[ssb.instrumentId] = { 'volume' : 0, 'cash' : 0.0 }
        self._position_by_instrument_id[ssb.instrumentId]['volume'] += sidemult * ssb.volume
        self._position_by_instrument_id[ssb.instrumentId]['cash'] -= sidemult * ssb.volume * ssb.price

    def get_positions(self):
        return self._position_by_instrument_id
    
    def get_cash(self):
        return sum([pos['cash'] for pos in self._position_by_instrument_id.values()])
    

class ExecClient(Client):
    def __init__(self, host, port, max_nr_trade_history=100):
        super().__init__(host=host, port=port)
        self._max_trade_history = max_nr_trade_history

    def reset_data(self):
        super(ExecClient, self).reset_data()
        self._exec = None
        self._username = None
        self._position_accountant = PositionAccountant()
        self._trade_history_last_polled_index = defaultdict(lambda: 0)
        self._trade_history = defaultdict(deque)
        self._order_status_by_order_id = defaultdict(dict)

    async def _on_connected(self):
        self._exec_portal = self._client.bootstrap().cast_as(exec_capnp.ExecPortal)

    async def authenticate(self, username, password, admin_password=None):
        self._username = username
        if admin_password is None:
            result = await self._exec_portal.login(username, password, self.ExecSubscription(self)).a_wait()
        else:
            result = await self._exec_portal.adminLogin(username, password, admin_password, self.ExecSubscription(self)).a_wait()
        self._exec = result.exec
        self._position_accountant = PositionAccountant(positions=result.positions.positions)

    async def insert_order(self, *, instrument_id, price, volume, side, order_type):
        assert side in ALL_SIDES, f"side must be one of {ALL_SIDES}"
        assert order_type in ALL_ORDER_TYPES, f"order_type must be one of {ALL_ORDER_TYPES}"
        return (await self._exec.insertOrder(instrument_id, price, volume, side, order_type).a_wait()).orderId

    async def amend_order(self, instrument_id, order_id, volume):
        return (await self._exec.amendOrder(instrument_id, order_id, volume).a_wait()).success

    async def delete_order(self, instrument_id, order_id):
        return (await self._exec.deleteOrder(instrument_id, order_id).a_wait()).success

    async def delete_orders(self, instrument_id):
        await self._exec.deleteOrders(instrument_id).a_wait()

    def get_positions(self):
        return { k : v['volume'] for k, v in self._position_accountant.get_positions().items() }

    def get_positions_and_cash(self):
        return self._position_accountant.get_positions()

    def get_cash(self):
        return self._position_accountant.get_cash()
                
    def get_outstanding_orders(self, instrument_id):
        return self._order_status_by_order_id[instrument_id].copy()

    def get_trade_history(self, instrument_id):
        return list(self._trade_history[instrument_id])

    def poll_new_trades(self, instrument_id):
        new_trades = list(
            itertools.islice(self._trade_history[instrument_id], self._trade_history_last_polled_index[instrument_id],
                             len(self._trade_history[instrument_id])))
        self._trade_history_last_polled_index[instrument_id] = len(self._trade_history[instrument_id])
        return new_trades

    def clear_trade_history(self):
        self._trade_history = defaultdict(deque)

    class ExecSubscription(exec_capnp.ExecPortal.ExecFeed.Server):
        def __init__(self, exec_client):
            self._exec = exec_client

        def onOrderUpdate(self, order, **kwargs):
            logger.debug('order start %s', order)
            order_id = order.orderId
            instrument_id = order.instrumentId

            o = OrderStatus()
            o.order_id = order.orderId
            o.instrument_id = order.instrumentId
            o.volume = order.volume
            o.side = order.side
            o.price = order.price
            self._exec._order_status_by_order_id[instrument_id][order_id] = o
            if order.volume == 0:
                self._exec._order_status_by_order_id[instrument_id].pop(order_id)
            logger.debug('order end %s', order)

        def onTrade(self, trade, **kwargs):
            tc = Trade()
            tc.price = trade.price
            tc.side = trade.side
            tc.volume = trade.volume
            tc.instrument_id = trade.instrumentId
            tc.order_id = trade.orderId
            inst_hist = self._exec._trade_history[tc.instrument_id]
            inst_hist.append(tc)
            while len(inst_hist) > self._exec._max_trade_history:
                inst_hist.popleft()
                self._exec._trade_history_last_polled_index[tc.instrument_id] = max(
                    self._exec._trade_history_last_polled_index[tc.instrument_id] - 1, 0)

            self._exec._position_accountant.handle_trade(trade)
            logger.debug('trade end %s', trade)

        def onSingleSidedBooking(self, ssb, **kwargs):
            self._exec._position_accountant.handle_single_sided_booking(ssb)

        def onForcedDisconnect(self, reason, **kwargs):
            logger.error(f'Forcing a disconnect due to an error: {reason}.')

        def ping(self, **kwargs):
            pass
