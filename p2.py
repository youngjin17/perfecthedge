import matplotlib.pyplot as plt
import pickle

import math
import numpy as np

from cointegration_analysis import estimate_long_run_short_run_relationships, engle_granger_two_step_cointegration_test

def read_data(timestamps_pckl, stock_values_pckl):
    with open(timestamps_pckl, 'rb') as f:
        timestamps = pickle.load(f)
    with open(stock_values_pckl, 'rb') as f:
        stock_values = pickle.load(f)
    return timestamps, stock_values

# Load data into timestmaps and stock_values.
timestamps, stock_values = read_data('timestamps.pckl', 'stock_values.pckl')

# Create a new dictionary called log_stock_values with a similar structure to 
# stock_values, but containing the log of these instead.
log_stock_values = {name: [math.log(v) for v in stock_values[name]] for name in stock_values}

# stock_types = ['AIRBUS', 'ALLIANZ', 'ASML', 'LVMH', 'SAP', 'SIEMENS', 'TOTAL', 'UNILEVER']
# for i in range(len(stock_types)):
#     for j in range(i + 1, len(stock_types)):
#         stock_y = stock_types[i]
#         stock_x = stock_types[j]
#         dfstat, pvalue = engle_granger_two_step_cointegration_test(
#             log_stock_values[stock_y], log_stock_values[stock_x])
#         if pvalue < .01:
#             print(f"Found cointegration pairs {stock_y}, {stock_x} with pvalue {pvalue}")
#             c, gamma, alpha, z = estimate_long_run_short_run_relationships(
#                 log_stock_values[stock_y], log_stock_values[stock_x])
#             ratio = gamma*stock_values[stock_y][-1]/stock_values[stock_x][-1]
#             print(f"ratio found, for each {stock_y}, trade opposite {stock_x} at ratio {ratio}")

c, gamma, alpha, z = estimate_long_run_short_run_relationships(log_stock_values['ALLIANZ'], log_stock_values['LVMH'])

while True:
    if iter % 250 == 0:
        _sanity_check()
    #check if sell y, then buy x
    y_t = math.log(price_bookY.bids[0].price)
    x_t = math.log(price_bookX.asks[0].price)
    z_t = y_t - c - gamma * x_t
    print(f"when selling allianz and buying lvmh, y_t = {y_t} x_t = {x_t}, z_t = {z_t}")
    if z_t > 0:
        #insert bid on LVMH
        # look at how much LVMH we got
        # calculate ratio based on gamma * Y_t/X_t. i.e. for every 1 lot in X you want to hedge with 'ratio' lots of Y
        # insert ask on ALLIANZ
        # look at how much ALLIANZ we got, wait for ioc trade