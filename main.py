# %% init N490 object and prepare the network for the year 2018 (some elements removed)
import logging

import numpy as np

from network_map import Map
from nordic490 import N490, plt

logger = logging.getLogger("Nordic490")
logger.setLevel(logging.DEBUG)
ch = logging.StreamHandler()
ch.setLevel(logging.DEBUG)
formatter = logging.Formatter("%(asctime)s - %(name)s - %(levelname)s - %(message)s")
ch.setFormatter(formatter)
logger.addHandler(ch)

m = N490(year=2018)
logger.info(m.bus.iloc[0, :])  # first bus
m.save_xlsx("test.xlsx")  # save to excel
# %% Make a case and solve DCPF

m = N490(year=2018)
m.branch_params()  # simple assumptions on R, X and B
load, gen, link = m.get_measurements("20180120:18")  # download data for a certain hour
m.distribute_power(load, gen, link)  # distribute on buses and gens (simple method)
m.dcpf()  # solve DC power flow
m.compare_flows()  # See how interarea flows compare with measurements

# %% Time series

m = N490(year=2018, set_branch_params=True)
m.time_series("20180101:00", "20180107:23")  # download one week of data + DCPF for each hour
ac = m.compare_flows(plot=False)  # get AC interarea flows (measured and simulated)
ac["SE1-SE2"].plot()
plt.show()
# %% # Calcualte errors

m = N490(year=2018, set_branch_params=True)
m.time_series("20180101:00", "20180107:23")  # download one week of data + DCPF for each hour
error = m.calculate_errors()  # to calculate error in n'th timestep, pass n as argument
logger.info(error)

# %% Default network plots

m = N490(year=None, set_branch_params=True)  # year=None -> read all data (also uc and dismantled)
m.simple_plot(bus=[6139], line=[2300])  # identify certain lines, buses or links
m.plot()  # default interactive map (click on objects for info)
plt.show()
# %% Two custom maps

m = N490(year=2018, set_branch_params=True)
load, gen, link = m.get_measurements("20180120:18")
m.distribute_power(load, gen, link)
m.dcpf()
x = Map(m)
x.init_plot()  # init plot (set parameters and draw the base map, mandatory)
x.bus_name_fs = [7, 0, 0]  # manually change layout parameters, see plot_settings() and set_map_properties()
x.add_topo()  # add network on map (mandatory)
x.add_heatmap("angle")  # parameter name or numpy vector allowed
x.add_legend()
x.show()
y = Map(m)
y.init_plot()
y.line_colors_from("length")
w = (m.line.Vbase**2 / np.maximum(m.line.X, 0.01)).values  # some kind of calculation
y.line_widths_from(w)
y.add_topo()
y.show()

# %%
