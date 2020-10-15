from multiprocessing import Process, cpu_count, Manager
from datetime import datetime, timedelta
from scipy.optimize import minimize
import nordic490
from nordic490 import N490
import sys, os
import json

# prepare datetimes
start_date = datetime(2018, 1, 1, 0)
# get rid of seconds and microseconds component
start_date.replace(second=0, microsecond=0)
end_date = datetime(2018, 12, 12, 0)
end_date.replace(second=0, microsecond=0)
interval = (end_date - start_date)/cpu_count()
def str_frm_int(i):
    return f'{start_date + i*interval:%Y%m%d:%H}'

work_intervals = [[str_frm_int(j) for j in [i, i+1]] for i in range(cpu_count())]

def optimize_interval(intervals, solutions):
    procnum = os.getpid()
    start_date, end_date = intervals
    """Ohm per km of the lines"""
    Z220 = 0.301
    Z300 = 0.265
    Z380 = 0.246
    x0 = [Z380, Z300, Z220]

    def objective(x):
        ohm_per_km = [x[0], x[1], x[2]]
        m = N490(year=2018)
        m.branch_params(ohm_per_km = ohm_per_km)
        # Make sure to modify the function to accept these parameters along with function call
        m.time_series(start_date, end_date)
        err = m.calculate_errors()
        return err['MAE'].sum()

    bz1 = (0.2, 0.4)
    bz2 = (0.2, 0.4)
    bz3 = (0.2, 0.4)
    bounds = (bz1, bz2, bz3)
    sol = minimize(objective, x0, method='SLSQP', bounds=bounds, options={'disp': True, 'eps': 1e-3})
    print(sol)
    solutions[procnum] = {'PID':procnum, 'x':sol.x.tolist(), 'fun':sol.fun , 'solution':str(dict(sol)), 'interval':intervals}


if __name__ == "__main__":  # confirms that the code is under main function
    procs = []
    manager = Manager()
    solutions = manager.dict()
    # instantiating process with arguments
    for intr in work_intervals:
        proc = Process(target=optimize_interval, args=(intr, solutions,))
        procs.append(proc)
        proc.start()
        print('process:', proc.pid, 'started on', intr)

    # complete the processes
    for proc in procs:
        proc.join()

    print('Saving results...',solutions)
    with open('solution.json', 'w') as fp:
        json.dump(solutions.copy(), fp)