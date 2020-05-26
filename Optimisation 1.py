
from scipy.optimize import minimize
import nordic490
from nordic490 import N490
import sys

"""X/R ratio for the lines
r220 = 5.0167
r300 = 8.17 #6.625
r380 = 10.34 #8.2
"""

"""Susceptance for the lines
C220 = 3e-6
C300 = 3e-6
C380 = 3e-6
"""

"""Ohm per km of the lines"""
Z220 = 0.301
Z300 = 0.265
Z380 = 0.246

"""Compensation factor for long distance lines"""
comp = 0.4

""" Transformer impedance """
t220 = 7e-2
t300 = 4e-2
t380 = 2.8e-2

x0 = [Z380, Z300, Z220, comp, t380, t300, t220]

def objective(x):
    ohm_per_km = [x[0], x[1], x[2]]
    compensate = [x[3], 380, 200]
    trafo_x = [x[4]/100, x[5]/100, x[6]/100]
    del sys.modules['nordic490']
    from nordic490 import N490
    m = N490(year=2018)
    m.branch_params(ohm_per_km, compensate, trafo_x)
    m.time_series('20180101:00', '20180107:23')
    err = m.calculate_errors()

    return err['MAE'].sum()

bz1 = (0.24, 0.42)
bz2 = (0.24, 0.34)
b2 = (0.40, 0.70)
bt1 = (2.8e-2, 1e-1)
bt2 = (4e-2, 1.5e-1)
bt3 = (7e-2, 1.5e-1)
bounds = (bz1, bz2, bz2, b2, bt1, bt2, bt3)

sol = minimize(objective, x0, method='SLSQP', bounds=bounds, options={'disp': True, 'eps': 1e-3})
#sol = minimize(objective, x0, method='L-BFGS-B', bounds=bounds)

print(sol)
