# -*- coding: utf-8 -*-
"""
Created on May 2020

@author: Aravind
"""

from scipy.optimize import minimize
import nordic490
from nordic490 import N490
import sys

"""Ohm per km of the lines"""
Z220 = 0.301
Z300 = 0.265
Z380 = 0.246

""" Transformer impedance """
t220 = 7e-2
t300 = 4e-2
t380 = 2.8e-2

# x0 = [Z380, Z300, Z220, t380, t300, t220]
x0 = [Z380, Z300, Z220]

def objective(x):
    ohm_per_km = [x[0], x[1], x[2]]
    # trafo_x = [x[3], x[4], x[5]]
    m = N490(year=2018)
    m.branch_params(ohm_per_km = ohm_per_km)
    # Make sure to modify the function to accept these parameters along with function call
    m.time_series('20180301:00', '20180401:00')
    err = m.calculate_errors()
    print(x, err['MAE'].sum())

    return err['MAE'].sum()


bz1 = (0.2, 0.4)
bz2 = (0.2, 0.4)
bz3 = (0.2, 0.4)
# bt1 = (2.8e-2, 1.5e-1)
# bt2 = (4e-2, 1.5e-1)
# bt3 = (7e-2, 1.5e-1)
# bounds = (bz1, bz2, bz3, bt1, bt2, bt3)
bounds = (bz1, bz2, bz3)

sol = minimize(objective, x0, method='SLSQP', bounds=bounds, options={'disp': True, 'eps': 1e-3})
print(sol)

""" To test the solution using a different solver """
# sol1 = minimize(objective, x0, method='L-BFGS-B', bounds=bounds, options={'disp': True, 'eps': 1e-3})
# print(sol1)
