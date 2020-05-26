# -*- coding: utf-8 -*-
"""
Created on Tue Oct 23 12:48:13 2018

@author: jolauson

Edited on

@by : Aravind S Kumar

"""

import os
import numpy as np
import pandas as pd
import pickle
from numpy import flatnonzero as find
from numpy import atleast_1d as arr
from numpy import concatenate as cat
import matplotlib.pyplot as plt
from pypower.runopf import runopf
from scipy.io import savemat
from pypower.api import rundcpf, runpf, case118, ppoption
import nordpool_db as nordpool
import entsoe_transparency_db as entsoe
from scipy.spatial.distance import cdist
import networkx as nx
from network_map import Map

warnings = False  # Display warnings?


def mult_ind(a, b, miss=np.nan):
    """ Get indices for elements of a in b, returns numpy array.
    E.g. mult_ind([1,2,1,4],[3,2,1]) -> array([2.,1.,2.,nan]) """
    bind = {}
    for i, elt in enumerate(b):
        if elt not in bind:
            bind[elt] = i
    return arr([bind.get(itm, miss) for itm in a])


class N490:

    def __init__(self, topology_file='Data', year=True, set_branch_params=False):
        """ Initiate object
        year: remove too old or not yet built. None -> include everything, True -> current year"""

        self.baseMVA = 100.
        self.dfs = ['bus', 'gen', 'line', 'link', 'trafo', 'farms']  # dataframes
        self.bidz = ['SE1', 'SE2', 'SE3', 'SE4', 'NO1', 'NO2', 'NO3', 'NO4', 'NO5', 'FI', 'DK2']
        self.country = ['SE', 'NO', 'FI', 'DK']
        self.gen_type = ['Nuclear', 'Hydro', 'Thermal', 'Wind']  # Main gen types
        self.load_data(topology_file)  # load network data from xlsx, npy etc.
        #self.prepare_network(year)  # possibly remove too new or old data, check islands
        #self.test = []
        self.modify_network(year)  # remove too new or old data, check islands and update loads in SE
        self.flow_measured = []  # store measured AC flows between areas
        self.flow_modelled = []  # store modelled -"- from e.g. dcpf()
        self.solved_mpc = []  # store solved cases
        self.time = 0  # store time when downloading from entso-e and nordpool
        if set_branch_params:
            self.branch_params()

    def load_data(self, topology_file):
        """ Load file with network topology (folder with pkl / excel / raw / matpower). """

        ext = os.path.splitext(topology_file)[1]  # file extension
        if ext == '':  # folder with pkl files
            for df in self.dfs:
                setattr(self, df, pd.read_pickle(os.path.join(topology_file, '%s.pkl' % df)))

        elif ext == '.xlsx':
            for df in self.dfs:
                setattr(self, df, pd.read_excel(topology_file, df, index_col=0))

        elif ext == '.raw':
            print('loading from .raw not implemented yet')
        elif ext == '.mat':
            print('loading from .mat not implemented yet')

    def prepare_network(self, year):

        """ Remove dismantled or not yet constructed equipment, check islands etc. """
        if year is True:
            year = pd.Timestamp.now().year

        # remove equipment
        if year is not None:
            for df in self.dfs:
                data = getattr(self, df)
                data.drop(data[data.uc > year].index, inplace=True)  # not yet constructed
                data.drop(data[(data.uc < 0) & (-data.uc <= year)].index, inplace=True)  # dismantled

        # remove island buses
        ibus = self.find_islands()
        if len(ibus) > 0:
            self.bus.drop(ibus, axis=0, inplace=True)
            if warnings:
                print('The following island buses were removed: %s' % str(ibus))

        # wind power
        f = self.farms
        f.drop(f[(f.status > 1) & ~f.uc].index, inplace=True)

        # existing wind farms on removed buses (iloc) -> find closest existing bus
        ind = find(np.isnan(mult_ind(f.bus, self.bus.index)))
        d = cdist(arr(f.iloc[ind, mult_ind(['x', 'y'], list(f))]), arr(self.bus.loc[:, ['x', 'y']]))
        bus = arr(self.bus.index[np.argmin(d, axis=1)])
        f.iloc[ind, list(f).index('bus')] = bus

        # update load_shares
        for b in self.bidz:
            sum_share = self.bus.loc[self.bus.bidz == b, 'load_share'].sum()
            self.bus.loc[self.bus.bidz == b, 'load_share'] *= 1 / sum_share


    def modify_network(self, year):

        """ Function to improve load distribution at buses """

        "Remove dismantled or not yet constructed equipment, check islands etc."
        if year is True:
            year = pd.Timestamp.now().year

        # remove equipment
        if year is not None:
            for df in self.dfs:
                data = getattr(self, df)
                data.drop(data[data.uc > year].index, inplace=True)  # not yet constructed
                data.drop(data[(data.uc < 0) & (-data.uc <= year)].index, inplace=True)  # dismantled

        # remove island buses
        ibus = self.find_islands()
        if len(ibus) > 0:
            self.bus.drop(ibus, axis=0, inplace=True)
            if warnings:
                print('The following island buses were removed: %s' % str(ibus))


        "Resetting the assigned loads "
        self.bus.loc[:, 'load_share'] = 0

        # Improve load distribution in Sweden

        "Read the load file for Sweden"
        Sl = pd.read_excel("Data/Loads/Sweden.xlsx", index_col=0)

        # Update load_share with new data
        for i, row1 in self.bus.iterrows():
            for j, row2 in Sl.iterrows():
                if row1['country'] == 'SE':
                    if row2['bus'] == i:
                        self.bus.loc[i, 'load_share'] += row2['Load']

        # Improved Load distribution Norway

        "Read the load file for Norway"
        No = pd.read_excel("Data/Loads/Norway.xlsx", index_col=0)

        # Update load_share with new data
        for i, row1 in self.bus.iterrows():
            for j, row2 in No.iterrows():
                if row1['country'] == 'NO':
                    if row2['bus'] == i:
                        self.bus.loc[i, 'load_share'] += row2['Load']


        # Improved Load distribution Finland

        "Read the load file for Finland"
        Fi = pd.read_excel("Data/Loads/Finland.xlsx", index_col=0)

        # Update load_share with new data
        for i, row1 in self.bus.iterrows():
            for j, row2 in Fi.iterrows():
                if row1['country'] == 'FI':
                    if row2['bus'] == i:
                        self.bus.loc[i, 'load_share'] += row2['Load']

        # Improved Load distribution Denmark

        "Read the load file for Denmark"
        Dk = pd.read_excel("Data/Loads/Denmark.xlsx", index_col=0)

        # Update load_share with new data
        for i, row1 in self.bus.iterrows():
            for j, row2 in Dk.iterrows():
                if row1['country'] == 'DK':
                    if row2['bus'] == i:
                        self.bus.loc[i, 'load_share'] += row2['Load']

        self.bus['load_share'].fillna(0, inplace=True)  # Remove NaN values

        # wind power
        f = self.farms
        f.drop(f[(f.status > 1) & ~f.uc].index, inplace=True)

        # existing wind farms on removed buses (iloc) -> find closest existing bus
        ind = find(np.isnan(mult_ind(f.bus, self.bus.index)))
        d = cdist(arr(f.iloc[ind, mult_ind(['x', 'y'], list(f))]), arr(self.bus.loc[:, ['x', 'y']]))
        bus = arr(self.bus.index[np.argmin(d, axis=1)])
        f.iloc[ind, list(f).index('bus')] = bus

        # update load_shares
        for b in self.bidz:
            sum_share = self.bus.loc[self.bus.bidz == b, 'load_share'].sum()
            self.bus.loc[self.bus.bidz == b, 'load_share'] *= 1 / sum_share

        self.bus['load_share'].fillna(0, inplace=True)  # Remove NaN values


    def branch_params(self):
        #, ohm_per_km, compensate, trafo_x
        """ Make some assumptions on branch parameters for lines and transformers.
        Note: X and B are per phase!
        """
        ohm_per_km = [0.246, 0.265, 0.301] # for [380, 300, <=220] kV lines
        #ohm_per_km = [0.33982, 0.33999, 0.38429]
        S_per_km = [13.8e-9, 13.2e-9, 12.5e-9]  # line charging susceptance [380, 300, 220] kV lines
        compensate = [0.5, 380, 200]  # long, high-voltage lines [compensation, min voltage (kV), min length (km)]
        trafo_x = [2.8e-4, 4e-4, 7e-4]  # pu reactance for [380,300,<300] kV per Sbase
        XR = [8.2, 6.625, 5.01667, 50]  # X/R for [380, 300, 220] kV lines and trafos

        line, trafo = self.line, self.trafo

        # lines
        line['X'] = ohm_per_km[2] * line['length'] / 1000 / line['Vbase'] ** 2 * self.baseMVA
        # line.loc[line.Vbase == 132 & line.circuit == 2, 'X'] = ohm_per_km[] * line['length'] / 1000 / line['Vbase'] ** 2 * self.baseMVA
        line.loc[line.Vbase == 380, 'X'] = ohm_per_km[0] * line['length'] / 1000 / line['Vbase'] ** 2 * self.baseMVA
        #line.loc[line.Vbase == 380 & line.circuit == 1, 'X'] = ohm_per_km[] * line['length'] / 1000 / line['Vbase'] ** 2 * self.baseMVA
        # line.loc[line.Vbase == 380 & line.circuit == 2, 'X'] = ohm_per_km[] * line['length'] / 1000 / line['Vbase'] ** 2 * self.baseMVA
        line.loc[line.Vbase == 300, 'X'] = ohm_per_km[1] * line['length'] / 1000 / line['Vbase'] ** 2 * self.baseMVA
        # line.loc[line.Vbase == 300 & line.circuit == 1, 'X'] = ohm_per_km[] * line['length'] / 1000 / line['Vbase'] ** 2 * self.baseMVA
        # line.loc[line.Vbase == 300 & line.circuit == 2, 'X'] = ohm_per_km[] * line['length'] / 1000 / line['Vbase'] ** 2 * self.baseMVA
        #line.loc[line.Vbase == 220, 'X'] = ohm_per_km[2] * line['length'] / 1000 / line['Vbase'] ** 2 * self.baseMVA
        # line.loc[line.Vbase == 220 & line.circuit == 1, 'X'] = ohm_per_km[] * line['length'] / 1000 / line['Vbase'] ** 2 * self.baseMVA
        # line.loc[line.Vbase == 220 & line.circuit == 2, 'X'] = ohm_per_km[] * line['length'] / 1000 / line['Vbase'] ** 2 * self.baseMVA
        line['R'] = ohm_per_km[2] * line['length'] / 1000 / line['Vbase'] ** 2 * self.baseMVA / XR[2]
        #line.loc[line.Vbase == 132 & line.circuit == 2, 'R'] = ohm_per_km[] * line['length'] / 1000 / line['Vbase'] ** 2 * self.baseMVA / XR[]
        line.loc[line.Vbase == 380, 'R'] = ohm_per_km[0] * line['length'] / 1000 / line['Vbase'] ** 2 * self.baseMVA / XR[0]
        # line.loc[line.Vbase == 380 & line.circuit == 1, 'R'] = ohm_per_km[] * line['length'] / 1000 / line['Vbase'] ** 2 * self.baseMVA / XR[]
        # line.loc[line.Vbase == 380 & line.circuit == 2, 'R'] = ohm_per_km[] * line['length'] / 1000 / line['Vbase'] ** 2 * self.baseMVA / XR[]
        line.loc[line.Vbase == 300, 'R'] = ohm_per_km[1] * line['length'] / 1000 / line['Vbase'] ** 2 * self.baseMVA / XR[1]
        # line.loc[line.Vbase == 300 & line.circuit == 1, 'R'] = ohm_per_km[] * line['length'] / 1000 / line['Vbase'] ** 2 * self.baseMVA / XR[]
        # line.loc[line.Vbase == 300 & line.circuit == 2, 'R'] = ohm_per_km[] * line['length'] / 1000 / line['Vbase'] ** 2 * self.baseMVA / XR[]
        #line.loc[line.Vbase == 220, 'R'] = ohm_per_km[2] * line['length'] / 1000 / line['Vbase'] ** 2 * self.baseMVA / XR[2]
        # line.loc[line.Vbase == 220 & line.circuit == 1, 'R'] = ohm_per_km[] * line['length'] / 1000 / line['Vbase'] ** 2 * self.baseMVA / XR[]
        # line.loc[line.Vbase == 220 & line.circuit == 2, 'R'] = ohm_per_km[] * line['length'] / 1000 / line['Vbase'] ** 2 * self.baseMVA / XR[]
        comp = (line.Vbase >= compensate[1]) & (line.length >= compensate[2] * 1000)
        line.loc[comp, 'X'] -= compensate[0] * line.loc[comp, 'X']
        line['B'] = S_per_km[2] * line['length'] / 1000 * line['Vbase'] ** 2 / self.baseMVA
        line.loc[line.Vbase == 380, 'B'] = S_per_km[0] * line['length'] / 1000 * line['Vbase'] ** 2 / self.baseMVA
        line.loc[line.Vbase == 300, 'B'] = S_per_km[1] * line['length'] / 1000 * line['Vbase'] ** 2 / self.baseMVA
        #line.loc[line.Vbase == 220, 'B'] = S_per_km[2] * line['length'] / 1000 * line['Vbase'] ** 2 / self.baseMVA
        # transformers
        v0 = self.bus.loc[trafo.bus0, 'Vbase']  # voltage at bus0
        v1 = self.bus.loc[trafo.bus1, 'Vbase']  # voltage at bus1
        v0_array = [_ for _ in v0]
        v1_array = [_ for _ in v1]
        # v = arr(np.maximum(v0, v1))  # max voltage
        v = arr(np.maximum(v0_array, v1_array))
        trafo['X'] = trafo_x[2] * self.baseMVA
        trafo.iloc[v == 380, list(trafo).index('X')] = trafo_x[0] * self.baseMVA
        trafo.iloc[v == 300, list(trafo).index('X')] = trafo_x[1] * self.baseMVA
        trafo['R'] = trafo['X'] / XR[3]
        trafo['B'] = 0

    def mpc2network(self, num=0):
        """ Extract data (flows etc) from solved case and add info to self.bus etc.
        More parameters later (e.g. after AC simulation)"""
        mpc = self.solved_mpc[num][0]
        self.bus['angle'] = mpc['bus'][:, 8]
        line = mpc['branch'][mpc['branch'][:, 8] == 0]
        self.line['flow'] = line[:, 13]

    def get_measurements(self, start, stop=None, adjust_gen=True):
        """ Import load, generation per type and HVDC from entso-e and Nordpool.
        start/stop can be strings 'yyyymmdd:hh' or pd.Timestamp (UTC+1)
        adjust_gen: True to adjust entso-e data based on Nordpool totals
        data for 2015-2018 is available (entsoe_transparency_db and nordpool_db to download more).
        """

        # Main gen type for each entso-e type
        gen_key = {'Biomass': 'Thermal', 'Gas': 'Thermal', 'Hard coal': 'Thermal', 'Hydro': 'Hydro',
                   'Hydro res': 'Hydro', 'Hydro ror': 'Hydro', 'Nuclear': 'Nuclear', 'Oil': 'Thermal',
                   'Other': 'Thermal', 'Other renew': 'Thermal', 'Peat': 'Thermal', 'Solar': 'Wind',
                   'Thermal': 'Thermal', 'Waste': 'Thermal', 'Wind': 'Wind', 'Wind offsh': 'Wind', 'Wind onsh': 'Wind'}

        # Adjust exchange areas from Nordpool, e.g. SE-PL becomes SE4-PL
        exch_fix = {'SE-PL': 'SE4-PL', 'SE-DE': 'SE4-DE', 'NO-DK': 'NO2-DK1', 'NO-NL': 'NO2-NL', 'NO-FI': 'NO4-FI'}

        def timestamp(s):
            return pd.to_datetime(s, format='%Y%m%d:%H')  # 'yyyymmdd:hh' to pd.Timestamp

        def empty_df(area, typ, time, fill=0):
            """ Make empty DataFrame with (area,type) as columns and time as index. """
            index = pd.MultiIndex.from_product([area, typ], names=['area', 'type'])
            df = pd.DataFrame(fill, index=time, columns=index)
            return df

        def fix_entsoe(dic, limit=10):
            """ Handle DST shift and interpolate remaining nans.
            Error if time starts with last sunday in october 02:00..."""

            time = dic[list(dic)[0]].index
            month, day, wd, hour = time.month, time.day, time.weekday, time.hour
            shift = find((month == 10) & (wd == 6) & (hour == 2) & (day > 24))  # DST shift hours in October
            for i, df in dic.items():
                if i in self.bidz:
                    # DST shift in October
                    for s in shift:
                        if sum(np.isnan(df.iloc[s, :])) > 0 and sum(df.iloc[s - 1, :]) / sum(df.iloc[s - 2, :]) > 1.5:
                            df.iloc[s - 1, :] /= 2
                            df.iloc[s, :] = df.iloc[s - 1, :]

                    # Remaining nans
                    df.interpolate(limit=limit, inplace=True)  # interpolate up to limit samples

                    if np.sum(arr(np.isnan(df))) > 0:
                        print('Too many nans in Entso-e data for %s, might not work properly.' % i)
            return dic

        if type(start) == type(pd.Timestamp(2018)):
            start = start.strftime('%Y%m%d:%H')
        if type(stop) == type(pd.Timestamp(2018)):
            stop = stop.strftime('%Y%m%d:%H')
        if stop is not None:
            time = pd.date_range(timestamp(start), timestamp(stop), freq='H')
        else:
            time = [timestamp(start)]
            stop = start

        # Entso-e generation
        db = entsoe.Database()
        raw = db.select_gen_per_type_wrap(starttime=start, endtime=stop)
        raw = fix_entsoe(raw)
        gen = empty_df(self.bidz, self.gen_type, time)
        for b in self.bidz:
            for t in list(raw[b]):  # iterate over gen types
                gen.loc[:, (b, gen_key[t])] += arr(raw[b][t])

        # Possibly adjust entso-e generation so sum equals nordpool
        db = nordpool.Database()
        if adjust_gen:
            gen_np = db.select_data(table='production', starttime=start, endtime=stop)
            for b in self.bidz:
                ratio = arr(gen_np[b] / gen.loc[:, b].sum(axis=1))
                for t in self.gen_type:
                    gen.loc[:, (b, t)] *= ratio

        # Nordpool load
        load = db.select_data(table='consumption', starttime=start, endtime=stop).loc[:, self.bidz]
        # Nordpool exchange (both AC and DC)
        exch_np = db.select_data(table='exchange', starttime=start, endtime=stop)
        cols = [c.replace(' ', '') for c in list(exch_np)]  # no blanks
        for k, v in exch_fix.items():
            try:
                cols[cols.index(k)] = v  # change e.g. SE-PL to SE4-PL
            except ValueError:
                pass
        exch = pd.DataFrame(index=time)
        for e, v in zip(cols, arr(exch_np).T):  # save relevant exchanges
            a0, a1 = e.split('-')
            if a0 in self.bidz or a1 in self.bidz:  # exclude e.g. LV-RU
                if not (a0 in self.country and a1 in self.country):  # exclude e.g. SE-FI
                    if a0 > a1:
                        exch.loc[:, '%s-%s' % (a1, a0)] = -v  # in alphabetic order
                    else:
                        exch.loc[:, '%s-%s' % (a0, a1)] = v

        # DC exchange
        link = pd.DataFrame(index=time)
        nn = 0  # keep track of Nordpool exchanges with counterpart in model
        for i in list(exch):
            a0, a1 = i.split('-')
            ind1 = self.link.index[(self.link.area0 == a0) & (self.link.area1 == a1)]  # find links between areas
            ind2 = self.link.index[(self.link.area0 == a1) & (self.link.area1 == a0)]  # -"- but reverse flow direction
            num = len(ind1) + len(ind2)  # number of links in model for this exchange
            if num > 0:
                nn += 1
                for i1 in ind1:
                    link.loc[:, i1] = arr(exch.loc[:, i] / num)  # later set exchange based on link capacity
                for i2 in ind2:
                    link.at[:, i2] = -arr(exch.loc[:, i] / num)

        # AC exchange between areas.
        ac_flow = pd.DataFrame(index=time)
        for i in list(exch):
            a0, a1 = i.split('-')
            ind1 = self.line.index[(self.line.area0 == a0) & (self.line.area1 == a1)]  # find lines between areas
            ind2 = self.line.index[(self.line.area0 == a1) & (self.line.area1 == a0)]  # -"- but reverse flow direction
            if len(ind1) + len(ind2) > 0:  # we have an AC connection
                nn += 1
                if self.bidz.index(a0) < self.bidz.index(a1):  # "first" bid zone should be first
                    ac_flow.loc[:, i] = exch.loc[:, i]
                else:
                    ac_flow.loc[:, '%s-%s' % (a1, a0)] = -exch.loc[:, i]

        # Check that all Nordpool exchanges has been taken into account
        if exch.shape[1] != nn and warnings:
            print('Some Nordpool exchanges are not included in model.')

        self.flow_measured = ac_flow
        self.flow_modelled = pd.DataFrame(0, index=time, columns=list(ac_flow))
        self.time = time

        return load, gen, link

    def time_series(self, start, stop):
        """ Download hourly time series between start and stop and run dc power flow for each hour."""

        print('\n*** Downloading data from Entso-E and Nordpool ***')
        load, gen, link = self.get_measurements(start, stop)
        print('\n*** Distributing power and running DCPF ***')
        day = 'yyyy-mm-dd'  # print progress each day
        for n, t in enumerate(self.time):
            self.distribute_power(load, gen, link, n)
            self.dcpf(n, save2network=False)
            if t.strftime('%Y-%m-%d') != day:
                day = t.strftime('%Y-%m-%d')
                print(day)

    def distribute_power(self, load, gen, link, time=0, gen_equals_load=True):
        """ Determine load, generation at each bus based on bid zone totals.
        time can be integer index of time series or Timestamp. """

        if type(time) is int:
            time = load.index[time]

        # Generation except wind
        neg_load = pd.DataFrame(0, index=self.bidz, columns=['load'])  # Negative load (if generators are missing)
        self.gen['P'] = 0.
        for b in self.bidz:
            for t in [x for x in self.gen_type if x != 'Wind']:
                ind = self.gen.index[(self.gen.bidz == b) & (self.gen.type == t)]
                available = np.sum(self.gen.loc[ind, 'Pmax'])
                if available == 0:
                    neg_load.at[b, 'load'] += gen.at[time, (b, t)]
                else:
                    share = gen.at[time, (b, t)] / available  # share of bid zone max
                    self.gen.loc[ind, 'P'] = self.gen.loc[ind, 'Pmax'] * share
                    if share > 1 and warnings:
                        print('Not enough %s capacity in %s (%d vs %d MW)' % (
                            t.lower(), b, available, gen.at[time, (b, t)]))

        # Wind
        self.farms['P'] = 0.
        for b in self.bidz:
            ind = self.farms.index[self.farms.bidz == b]
            available = np.sum(self.farms.loc[ind, 'Pmax'])
            if available == 0:
                neg_load.at[b, 'load'] += gen.at[time, (b, 'Wind')]
            else:
                share = gen.at[time, (b, 'Wind')] / available  # share of bid zone max
                self.farms.loc[ind, 'P'] = self.farms.loc[ind, 'Pmax'] * share
                if share > 1 and warnings:
                    print('Not enough wind capacity in %s (%d vs %d MW)' % (b, available, gen.at[time, (b, 'Wind')]))

        # Load including wind (+PV) and negative load
        self.bus['load'] = 0
        for i, row in self.farms.iterrows():  # wind as negative load
            self.bus.at[int(row.bus), 'load'] -= row.P
        for b in self.bidz:
            ind = self.bus.index[self.bus.bidz == b]
            bidz_load = load.at[time, b] - neg_load.at[b, 'load']
            self.bus.loc[ind, 'load'] += self.bus.loc[ind, 'load_share'] * bidz_load

        # DC (load or negative load at nordic bus)
        self.link['P'] = link.loc[time, :]
        self.link.P.fillna(0, inplace=True)
        for i, row in self.link.iterrows():
            try:
                self.bus.at[row.bus0, 'load'] -= row.P
            except KeyError:
                if row.area0 in self.bidz and warnings:
                    print('No bus found for link %s (%d)' % (row['name'], row.name))
            try:
                self.bus.at[row.bus1, 'load'] += row.P
            except KeyError:
                if row.area1 in self.bidz and warnings:
                    print('No bus found for link %s (%d)' % (row['name'], row.name))

        # Adjust generation so it equals load (for DC power flow)
        if gen_equals_load:
            self.gen.P *= self.bus.load.sum() / self.gen.P.sum()


    def make_mpc(self):
        """ Make matpower/pypower case (mostly DC parameters for now).
        Safer to use bus_idx etc. to find correct columns for pypower version in question?"""
        bus, line, trafo, gen = self.bus, self.line, self.trafo, self.gen
        mpc = {'version': '2', 'baseMVA': self.baseMVA}

        # bus
        mpc['bus'] = np.zeros((len(bus), 13))
        mpc['bus'][:, 0] = bus.index
        mpc['bus'][:, 1] = [2 if np.sum(gen.Pmax.loc[gen.bus == b]) > 0 else 1 for b in bus.index]  # PV/PQ
        mpc['bus'][find(mpc['bus'][:, 1] == 2)[0], 1] = 3  # ref
        mpc['bus'][:, 2] = bus.load
        mpc['bus'][:, 6] = mult_ind(bus.bidz, self.bidz) + 1  # bid zone index as area
        mpc['bus'][:, 7] = 1  # voltage (pu)
        mpc['bus'][:, 9] = bus.Vbase
        mpc['bus'][:, 10] = 1  # zone
        mpc['bus'][:, 11] = 0.9  # Vmin
        mpc['bus'][:, 12] = 1.15  # Vmax

        # gen
        mpc['gen'] = np.zeros((len(gen), 21))
        mpc['gen'][:, 0] = gen.bus
        mpc['gen'][:, 1] = gen.P
        mpc['gen'][:, 3] = -1000  # Qmin
        mpc['gen'][:, 4] = 1000  # Qmax
        mpc['gen'][:, 5] = 1  # voltage setpoint
        mpc['gen'][:, 6] = self.baseMVA
        mpc['gen'][:, 7] = 1  # 1 for in-service
        mpc['gen'][:, 8] = gen.Pmax

        # branch
        br = pd.concat([line, trafo], sort=False)
        mpc['branch'] = np.zeros((len(br), 13))
        mpc['branch'][:, 0] = br.bus0
        mpc['branch'][:, 1] = br.bus1
        mpc['branch'][:, 2] = br.R
        mpc['branch'][:, 3] = br.X
        mpc['branch'][:, 4] = br.B
        mpc['branch'][:, 8] = [1 if np.isnan(ug) else 0 for ug in br.ug]  # transformer off nominal turns ratio
        mpc['branch'][:, 10] = 1  # 1 for in-service
        mpc['branch'][:, 11] = -360  # minimum angle difference
        mpc['branch'][:, 12] = 360  # maximum angle difference

        return mpc

    def dcpf(self, time=0, mpc=None, save2network=True):
        """ Run DC power flow """

        if type(time) is int:
            time = self.flow_modelled.index[time]
        if mpc is None:
            mpc = self.make_mpc()
        mpc = rundcpf(mpc, ppoption(VERBOSE=0, OUT_ALL=-1, OUT_BUS=0, OUT_ALL_LIM=0, OUT_BRANCH=0, OUT_SYS_SUM=0))
        self.solved_mpc.append(mpc)

        # AC exchange between areas.
        br = mpc[0]['branch']
        ac_flow = pd.DataFrame(0, index=[time], columns=list(self.flow_modelled))
        bid0 = arr(self.bus.loc[br[:, 0], 'bidz'])
        bid1 = arr(self.bus.loc[br[:, 1], 'bidz'])
        for n, a0 in enumerate(self.bidz):
            for a1 in self.bidz[n + 1:]:
                ind0 = (bid0 == a0) & (bid1 == a1)  # find lines between areas
                ind1 = (bid0 == a1) & (bid1 == a0)  # -"- but reverse flow direction
                if np.sum(ind0) + np.sum(ind1) > 0:
                    exch = - np.sum(br[ind0, 13]) + np.sum(br[ind1, 13])
                    ac_flow.loc[time, '%s-%s' % (a0, a1)] = exch
        for i in list(ac_flow):
            self.flow_modelled.at[time, i] = ac_flow.at[time, i]
        if save2network:
            self.mpc2network()  # add parameters to network (flows, voltage angle etc.)


    def compare_flows(self, n=None, plot=True):
        """Compare flow_measured with flow_modelled
        Specify n to look at timestep n, otherwise all timesteps
        plot False -> return results dataframe"""

        meas = self.flow_measured
        sim = self.flow_modelled
        if len(meas) == 1:
            n = 0

        if type(n) is int:
            res = pd.DataFrame(index=list(sim), columns=['Measured', 'Modelled'])
            res['Measured'] = meas.iloc[n, :].values
            res['Modelled'] = sim.iloc[n, :].values
            if plot:
                res.plot.bar(rot=45, title='AC exchange %s' % sim.index[n])
                plt.show()
        else:
            col = pd.MultiIndex.from_product([list(sim), ['Measured', 'Modelled']])
            res = pd.DataFrame(index=sim.index, columns=col)
            for c in list(sim):
                res.loc[:, (c, 'Measured')] = meas[c]
                res.loc[:, (c, 'Modelled')] = sim[c]
            if plot:
                fig, ax = plt.subplots(4, 4)
                for n, i in enumerate(list(sim)):
                    res[i].plot(ax=ax[int(n / 4), n % 4], legend=False, title=i)
                    y = ax[int(n / 4), n % 4].get_ylim()
                    ax[int(n / 4), n % 4].set_ylim(min(-1000, y[0]), max(1000, y[1]))
                plt.show()
        if not plot:
            return res

    def calculate_errors(self, n=None):
        """ Compare flow_measured with flow_modelled and calculate the error values """
        "Specify n to look at timestep n, otherwise all returns for all timesteps"

        meas = self.flow_measured
        sim = self.flow_modelled

        if len(meas) == 1:
            n = 0

        if type(n) is int:
            err = pd.DataFrame(index=list(sim), columns=['MAE', 'MAPE', 'RMSE'])
            err['MAE'] = np.abs(meas.iloc[n, :].values - sim.iloc[n, :].values)
            err['MAPE'] = np.abs((meas.iloc[n, :].values - sim.iloc[n, :].values)/meas.iloc[n, :].values*100)
            err['RMSE'] = np.sqrt(np.square(meas.iloc[n, :].values - sim.iloc[n, :].values))

        else:
            err = pd.DataFrame(index=list(sim), columns=['MAE', 'MAPE', 'RMSE'])
            err['MAE'] = np.abs(meas - sim).mean()
            err['MAPE'] = np.abs((meas - sim) / meas*100).mean()
            err['RMSE'] = np.sqrt(np.square(meas - sim).mean())

        return err

    def save_xlsx(self, file_name):
        """ Export data to Excel (different sheets for bus,gen,line...). """

        writer = pd.ExcelWriter(file_name)
        for df in self.dfs:
            getattr(self, df).to_excel(writer, sheet_name=df)
        writer.save()

    def save_mat(self, path):
        """ Export to Matpower format (Matlab). """
        # TODO Should save a struct instead since this is expected by Matpower
        mpc = self.make_mpc()
        savemat(path, mpc)

    def save_raw(self, file_name):
        """ Export to raw format. """
        print('save_raw not implemented yet')

    def pickle(self, path):
        """ Pickle whole model. """
        pickle.dump(self, open(path, "wb"))

    def save_shp(self, file_name):
        """ Export to GIS shape files.
        Separate files will be saved for buses, lines etc."""
        print('save_shp not implemented yet')

    def plot(self):
        """ Make a interactive network plot (using modified SPINE class).
        For more advanced features, see example at top. """
        m = Map(self)
        m.init_plot()
        m.add_topo()
        m.add_legend()
        #m.show()

    def simple_plot(self, bus=None, line=None, link=None):
        """ Simple plot, possibly highlight certain buses, lines and/or links. """
        data_path = os.path.join('Data', 'raw', 'map_with_bidz2018.npz')
        temp = np.load(data_path, allow_pickle=True)
        x_map, y_map = temp['x'], temp['y']
        fig, ax = plt.subplots(1, 1, figsize=[6, 8])
        plt.fill([-1e6, -1e6, 2e6, 2e6, -1e6], [5e6, 9e6, 9e6, 5e6, 5e6], facecolor=(220. / 255, 238. / 255, 1))
        for x, y in zip(x_map, y_map):
            plt.fill(x, y, 'w')
        for x, y in zip(x_map, y_map):
            plt.plot(x, y, 'k', lw=0.5)

        plt.scatter(self.bus.x, self.bus.y, s=3, c='k', zorder=10)
        if bus is not None:
            plt.scatter(self.bus.x.loc[bus], self.bus.y.loc[bus], s=15, c='r', zorder=10)
        for i, row in self.line.iterrows():
            if line is not None and i in line:
                plt.plot(row.x, row.y, c='r', lw=2)
            else:
                plt.plot(row.x, row.y, c='k', lw=0.5)
        for i, row in self.link.iterrows():
            if link is not None and i in line:
                plt.plot(row.x, row.y, c='r', lw=2)
            else:
                plt.plot(row.x, row.y, c='b', lw=0.5)

        plt.xlim([-1.2e5, 1.35e6])
        plt.ylim([5.9e6, 7.95e6])
        fig.subplots_adjust(bottom=0.01, top=0.99, left=0.01, right=0.99)
        ax.tick_params(axis='both', which='both', bottom=False, labelbottom=False,
                       top=False, labeltop=False, left=False, labelleft=False, right=False, labelright=False)
        #plt.show()

    def find_islands(self):
        """ Return island buses. """
        G = nx.Graph()
        for b1, b2 in zip(self.line.bus0, self.line.bus1):
            G.add_edge(b1, b2)
        for b1, b2 in zip(self.trafo.bus0, self.trafo.bus1):
            G.add_edge(b1, b2)
        for b in self.bus.index:
            G.add_node(b)
        SG = [list(c) for c in nx.connected_components(G)]  # sub-graphs
        if len(SG) > 1:
            return [b for s in SG[1:] for b in s]
        else:
            return []

    def balance(self, time=0):

        if type(time) is int:
            time = self.flow_modelled.index[time]

        # Check balance per area
        bal = pd.DataFrame(0, index=self.bidz, columns=['load', 'gen', 'ac', 'balance'])
        for b in self.bidz:
            ind = self.bus.index[self.bus.bidz == b]
            bal.at[b, 'load'] = -np.sum(self.bus.loc[ind, 'load'])
            ind = self.gen.index[self.gen.bidz == b]
            bal.at[b, 'gen'] = np.sum(self.gen.loc[ind, 'P'])
            bal.at[b, 'ac'] += np.sum(np.matrix(self.flow_modelled.loc[time, self.flow_modelled.columns.str.contains(b+'-')]))
            bal.at[b, 'ac'] -= np.sum(np.matrix(self.flow_modelled.loc[time, self.flow_modelled.columns.str.contains('-'+b)]))
            #bal.at[b, 'ac'] -= np.sum(np.matrix(ac_flow.loc[:, ac_flow.columns.str.contains('-'+b)]))
        bal['balance'] = np.sum(bal, axis=1)
        print(bal)
        #return bal


if __name__ == '__main__':
    m = N490(year=2018)
    self = m
    m.branch_params()
    start = '20181028:00'
    stop = '20181028:15'
    if 0:
        m.time_series(start, stop)
        results = m.compare_flows(plot=False)
    if 0:
        load, gen, link = m.get_measurements(start)
        m.distribute_power(load, gen, link)
        m.dcpf()

    if 0:  # test ac
        load, gen, link = m.get_measurements(start)
        m.distribute_power(load, gen, link, gen_equals_load=False)
        mpc = m.make_mpc()
        mpc = rundcpf(mpc, ppoption(VERBOSE=0, OUT_ALL=-1, OUT_BUS=0, OUT_ALL_LIM=0, OUT_BRANCH=0))
        # mpc = runpf(mpc,ppoption(VERBOSE=0,OUT_ALL=-1,OUT_BUS=0,OUT_ALL_LIM=0,OUT_BRANCH=0,PF_ALG='NR'))

"""
        # Check balance per area
        bal = pd.DataFrame(0,index=self.bidz,columns=['load','gen','ac','balance'])
        for b in self.bidz:
            ind = self.bus.index[self.bus.bidz == b]
            bal.at[b,'load'] = -np.sum(self.bus.loc[ind,'load'])
            ind = self.gen.index[self.gen.bidz == b]
            bal.at[b,'gen'] = np.sum(self.gen.loc[ind,'P'])
            ind = ac_flow.index[ac_flow.area0 == b]
            bal.at[b,'ac'] = np.sum(ac_flow.loc[ind,'exch'])
            ind = ac_flow.index[ac_flow.area1 == b]
            bal.at[b,'ac'] -= np.sum(ac_flow.loc[ind,'exch'])
        bal['balance'] = np.sum(bal,axis=1)

  """
