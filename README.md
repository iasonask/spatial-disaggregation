# spatial-disaggregation

This class can be used for:
1) Read network topology and remove too old/new elements depending on scenario year
2) basic assumptions on branch parameters (+gen, bus; allt i mpc ska finnas i network)
3) Download data from Ensto-e and Nordpool for a given time
4) simple scenario for load, generation, etc. on each bus
5) DC power flow and AC power flow (AC requires more work...)
6) plotting
7) export

For using the N490 class, you need to extract the [Data](https://uopel-my.sharepoint.com/:u:/g/personal/iasonask_office365_uop_gr/EUTapi8S6cpKrSNYlSjYANwBCZCWvR5XWQJjVo7tvotGUA?e=gX85Bm) folder in the project's directory

### *** Examples ***

### init N490 object and prepare the network for the year 2018 (some elements removed)
Pandas DataFrames are created: bus, gen, line, link, trafo and farms (wind farms)
```python
from nordic490 import N490
m = N490(year=2018) 
print(m.bus.iloc[0,:]) # first bus
m.save_xlsx('test.xlsx') # save to excel 
```

### Make a case and solve DCPF
```python
from nordic490 import N490
m = N490(year=2018)
m.branch_params() # simple assumptions on R, X and B
load, gen, link = m.get_measurements('20180120:18') # download data for a certain hour
m.distribute_power(load, gen, link) # distribute on buses and gens (simple method)
m.dcpf() # solve DC power flow
m.compare_flows() # See how interarea flows compare with measurements
```

### Time series
```python
from nordic490 import N490, plt
m = N490(year=2018,set_branch_params=True)
m.time_series('20180101:00','20180107:23') # download one week of data + DCPF for each hour
ac = m.compare_flows(plot=False) # get AC interarea flows (measured and simulated)
ac['SE1-SE2'].plot()
plt.show()
```
### Calculate Errors 
```python
from nordic490 import N490, plt
m = N490(year=2018,set_branch_params=True)
m.time_series('20180101:00','20180107:23') # download one week of data + DCPF for each hour
error = m.calculate_errors() # to calculate error in n'th timestep, pass n as argument  
print(error)
```

### Default network plots
```python
from nordic490 import N490, plt
m = N490(year=None,set_branch_params=True) # year=None -> read all data (also uc and dismantled)
m.simple_plot(bus=[6139],line=[2300]) # identify certain lines, buses or links
m.plot() # default interactive map (click on objects for info)
plt.show()
```
### Two custom maps
```python
from nordic490 import N490
from network_map import Map
import numpy as np
m = N490(year=2018,set_branch_params=True)
load, gen, link = m.get_measurements('20180120:18')
m.distribute_power(load,gen,link)
m.dcpf()
x = Map(m)
x.init_plot() # init plot (set parameters and draw the base map, mandatory)
x.bus_name_fs = [7,0,0] # manually change layout parameters, see plot_settings() and set_map_properties()
x.add_topo() # add network on map (mandatory)
x.add_heatmap('angle') # parameter name or numpy vector allowed
x.add_legend()
x.show()
y = Map(m)
y.init_plot()
y.line_colors_from('length')
w = (m.line.Vbase**2 / np.maximum(m.line.X,0.01)).values # some kind of calculation
y.line_widths_from(w)
y.add_topo()
y.show()   
``` 

#### Notes

To install all required packages automatically, you can create and activate a 
virtual environment and then execute the following:

```bash
pip install -r requirements.txt
```
