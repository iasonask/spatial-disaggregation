# -*- coding: utf-8 -*-
""" Build sqlite database 'nordpool.db' from nordpool csv data. The tables are:

    consumption(TEXT time, TEXT area, FLOAT MWh)
        - consumption in each area

    production(TEXT time, TEXT area, FLOAT MWh)
        - production in each area

    wind(TEXT time, TEXT area, FLOAT MWh)
        - wind production in each area, not for NO or FI

    spotprice(TEXT time, TEXT area, FLOAT EUR)
        - spot price in each area, also has system price under SYS

    exchange(TEXT time, TEXT transfer, FLOAT MWh)
        - transfer between areas, transfer is e.g. 'SE1 - SE2'

    reservoir(TEXT time, TEXT area, FLOAT GWh)
        - reservoir content, with area = country, SE, FI, NO, time: 'YYYY:WW'

    inflow(time TEXT, area TEXT, GWh FLOAT)
        - hydro reservoir inflow, calculated from reservoir content
          and hydro production, time: 'YYYY:WW'

    time: 'YYYYMMDD:HH'


    Note: For nordpool data for hour 00-01 we have used time stamp YYYYMMDD:00
    ENTSO-E data in UTC, Nordpool data in CET=UTC+1:
    Nordpool  -> Entsoe
    20180101:00  20171231:23
    20180101:01  20180101:00

Created on Wed Jan 23 14:09:59 2019

@author: elisn
"""

import csv
import sqlite3
from pathlib import Path
import datetime
import pandas as pd
import numpy as np
import openpyxl
from help_functions import week_to_date, date_to_week, weekdd, str_to_date
import matplotlib.pyplot as plt

tables = ['consumption', 'exchange', 'wind', 'spotprice', 'reservoir', 'inflow', 'production']


class Database():

    def __init__(self, db='Data/nordpool.db'):

        self.db = db
        self.tables = tables
        self.cat = {}
        # if database file exists, query tables to find valid categories
        if Path(self.db).exists():
            # query categories
            for tab in self.tables:
                self.cat[tab] = self.query_categories(tab)
        else:
            for tab in self.tables:
                self.cat[tab] = []

    def create_database(self):

        # make sqlite database
        conn = sqlite3.connect(self.db)
        c = conn.cursor()

        area_codes = ['SE1', 'SE2', 'SE3', 'SE4', 'DK1', 'DK2', 'EE', 'LT', 'LV', \
                      'FI', 'NO1', 'NO2', 'NO3', 'NO4', 'NO5']

        ## PRODUCTION
        if 1:
            c.execute('DROP TABLE IF EXISTS production')
            c.execute('CREATE TABLE production (' + \
                      'time TEXT NOT NULL,' + \
                      'area TEXT NOT NULL,' + \
                      'MWh REAL' + \
                      ')')

            data_path = 'C:/Users/elisn/Box Sync/Data/Nordpool/Production/'

            files = []
            for area in ['dk', 'se', 'no']:
                files = files + ['production-{0}-areas_{1}_hourly.csv'.format(area, year) for year in range(2013, 2019)]
            files = files + ['production-per-country_{0}_hourly.csv'.format(year) for year in range(2013, 2019)]

            for data_file in files:
                print("Reading {0}".format(data_file))
                with open(data_path + data_file) as f:
                    csv_reader = csv.reader(f, delimiter=';')
                    ridx = 0
                    for row in csv_reader:
                        if ridx == 0:
                            pass
                        elif ridx == 1:
                            pass
                        elif ridx == 2:
                            # get headers
                            all_cols = row[2:]
                            rel_cols = []
                            for idx, col in enumerate(all_cols):
                                if col in area_codes:
                                    rel_cols.append(idx)
                        else:
                            # reformat date
                            dmy = row[0].split('-')
                            date = dmy[2] + dmy[1] + dmy[0]
                            # reformat hour
                            hour = row[1][0:2]
                            time = date + ':' + hour

                            # loop over values in row
                            for idx in rel_cols:
                                area = all_cols[idx]
                                val = row[2 + idx]
                                if val == '':  # repliace missing data with NULL
                                    val = 'NULL'
                                # insert into database
                                cmd = 'INSERT INTO production (time,area,MWh) values("{0}","{1}",{2})'.format(time,
                                                                                                              area, val)
                                c.execute(cmd)
                        ridx += 1

        ## CONSUMPTION
        if 0:
            c.execute('DROP TABLE IF EXISTS consumption')
            c.execute('CREATE TABLE consumption (' + \
                      'time TEXT NOT NULL,' + \
                      'area TEXT NOT NULL,' + \
                      'MWh REAL' + \
                      ')')

            data_path = 'C:/Users/elisn/Box Sync/Data/Nordpool/Consumption/'
            files = [ \
                'consumption-dk-areas_2014_hourly.csv', \
                'consumption-dk-areas_2015_hourly.csv', \
                'consumption-dk-areas_2016_hourly.csv', \
                'consumption-dk-areas_2017_hourly.csv', \
                'consumption-dk-areas_2018_hourly.csv', \
                'consumption-no-areas_2014_hourly.csv', \
                'consumption-no-areas_2015_hourly.csv', \
                'consumption-no-areas_2016_hourly.csv', \
                'consumption-no-areas_2017_hourly.csv', \
                'consumption-no-areas_2018_hourly.csv', \
                'consumption-se-areas_2014_hourly.csv', \
                'consumption-se-areas_2015_hourly.csv', \
                'consumption-se-areas_2016_hourly.csv', \
                'consumption-se-areas_2017_hourly.csv', \
                'consumption-se-areas_2018_hourly.csv', \
                'consumption-per-country_2013_hourly.csv', \
                'consumption-per-country_2014_hourly.csv', \
                'consumption-per-country_2015_hourly.csv', \
                'consumption-per-country_2016_hourly.csv', \
                'consumption-per-country_2017_hourly.csv', \
                'consumption-per-country_2018_hourly.csv', \
                ]

            for data_file in files:
                print("Reading {0}".format(data_file))
                with open(data_path + data_file) as f:
                    csv_reader = csv.reader(f, delimiter=';')
                    ridx = 0
                    for row in csv_reader:
                        # print(row)

                        if ridx == 0:
                            pass
                        elif ridx == 1:
                            pass
                        elif ridx == 2:
                            # get headers
                            all_cols = row[2:]
                            rel_cols = []
                            for idx, col in enumerate(all_cols):
                                if col in area_codes:
                                    rel_cols.append(idx)
                        else:
                            # reformat date
                            dmy = row[0].split('-')
                            date = dmy[2] + dmy[1] + dmy[0]
                            # reformat hour
                            hour = row[1][0:2]
                            time = date + ':' + hour
                            # print(time)

                            # loop over values in row
                            for idx in rel_cols:
                                area = all_cols[idx]
                                val = row[2 + idx]
                                if val == '':  # repliace missing data with NULL
                                    val = 'NULL'
                                # insert into database
                                cmd = 'INSERT INTO consumption (time,area,MWh) values("{0}","{1}",{2})'.format(time,
                                                                                                               area,
                                                                                                               val)
                                c.execute(cmd)
                        ridx += 1

        ## WIND
        if 0:
            c.execute('DROP TABLE IF EXISTS wind')
            c.execute('CREATE TABLE wind (' + \
                      'time TEXT NOT NULL,' + \
                      'area TEXT NOT NULL,' + \
                      'MWh REAL' + \
                      ')')

            data_path = 'C:/Users/elisn/Box Sync/Data/Nordpool/Wind/'

            files = [ \
                'wind-power-dk_2013_hourly.csv', \
                'wind-power-dk_2014_hourly.csv', \
                'wind-power-dk_2015_hourly.csv', \
                'wind-power-dk_2016_hourly.csv', \
                'wind-power-dk_2017_hourly.csv', \
                'wind-power-dk_2018_hourly.csv', \
                'wind-power-ee_2013_hourly.csv', \
                'wind-power-ee_2014_hourly.csv', \
                'wind-power-ee_2015_hourly.csv', \
                'wind-power-ee_2016_hourly.csv', \
                'wind-power-ee_2017_hourly.csv', \
                'wind-power-ee_2018_hourly.csv', \
                'wind-power-lt_2013_hourly.csv', \
                'wind-power-lt_2014_hourly.csv', \
                'wind-power-lt_2015_hourly.csv', \
                'wind-power-lt_2016_hourly.csv', \
                'wind-power-lt_2017_hourly.csv', \
                'wind-power-lt_2018_hourly.csv', \
                'wind-power-lv_2013_hourly.csv', \
                'wind-power-lv_2014_hourly.csv', \
                'wind-power-lv_2015_hourly.csv', \
                'wind-power-lv_2016_hourly.csv', \
                'wind-power-lv_2017_hourly.csv', \
                'wind-power-lv_2018_hourly.csv', \
                'wind-power-se_2013_hourly.csv', \
                'wind-power-se_2014_hourly.csv', \
                'wind-power-se_2015_hourly.csv', \
                'wind-power-se_2016_hourly.csv', \
                'wind-power-se_2017_hourly.csv', \
                'wind-power-se_2018_hourly.csv', \
                ]

            for data_file in files:
                print("Reading {0}".format(data_file))
                with open(data_path + data_file) as f:
                    csv_reader = csv.reader(f, delimiter=';')
                    ridx = 0
                    for row in csv_reader:
                        if ridx == 0:
                            pass
                        elif ridx == 1:
                            pass
                        elif ridx == 2:
                            # get headers
                            all_cols = row[2:]
                            rel_cols = []
                            for idx, col in enumerate(all_cols):
                                if col in area_codes:
                                    rel_cols.append(idx)
                        else:
                            # reformat date
                            dmy = row[0].split('-')
                            date = dmy[2] + dmy[1] + dmy[0]
                            # reformat hour
                            hour = row[1][0:2]
                            time = date + ':' + hour
                            # print(time)

                            # loop over values in row
                            for idx in rel_cols:
                                area = all_cols[idx]
                                val = row[2 + idx]
                                if val == '':  # repliace missing data with NULL
                                    val = 'NULL'
                                # insert into database
                                cmd = 'INSERT INTO wind (time,area,MWh) values("{0}","{1}",{2})'.format(time, area, val)
                                c.execute(cmd)
                        ridx += 1

        ### EXCHANGE
        if 0:

            c.execute('DROP TABLE IF EXISTS exchange')
            c.execute('CREATE TABLE exchange (' + \
                      'time TEXT NOT NULL,' + \
                      'transfer TEXT NOT NULL,' + \
                      'MWh REAL' + \
                      ')')

            data_path = 'C:/Users/elisn/Box Sync/Data/Nordpool/Exchange/'

            #        files = []
            #        for cc in ['no','se','lv','lt','ee','dk']:
            #            for yy in [str(y) for y in range(2016,2019)]:
            #                files.append('exchange-{0}-connections_{1}_hourly.csv'.format(cc,yy))
            #
            files = [ \
                'exchange-se-connections_2014_hourly.csv', \
                'exchange-se-connections_2015_hourly.csv', \
                'exchange-se-connections_2016_hourly.csv', \
                'exchange-se-connections_2017_hourly.csv', \
                'exchange-se-connections_2018_hourly.csv', \
                'exchange-no-connections_2013_hourly.csv', \
                'exchange-no-connections_2014_hourly.csv', \
                'exchange-no-connections_2015_hourly.csv', \
                'exchange-no-connections_2016_hourly.csv', \
                'exchange-no-connections_2017_hourly.csv', \
                'exchange-no-connections_2018_hourly.csv', \
                'exchange-dk-connections_2013_hourly.csv', \
                'exchange-dk-connections_2014_hourly.csv', \
                'exchange-dk-connections_2015_hourly.csv', \
                'exchange-dk-connections_2016_hourly.csv', \
                'exchange-dk-connections_2017_hourly.csv', \
                'exchange-dk-connections_2018_hourly.csv', \
                'exchange-ee-connections_2015_hourly.csv', \
                'exchange-ee-connections_2016_hourly.csv', \
                'exchange-ee-connections_2017_hourly.csv', \
                'exchange-ee-connections_2018_hourly.csv', \
                'exchange-fi-connections_2013_hourly.csv', \
                'exchange-fi-connections_2014_hourly.csv', \
                'exchange-fi-connections_2015_hourly.csv', \
                'exchange-fi-connections_2016_hourly.csv', \
                'exchange-fi-connections_2017_hourly.csv', \
                'exchange-fi-connections_2018_hourly.csv', \
                'exchange-lt-connections_2016_hourly.csv', \
                'exchange-lt-connections_2017_hourly.csv', \
                'exchange-lt-connections_2018_hourly.csv', \
                'exchange-lv-connections_2016_hourly.csv', \
                'exchange-lv-connections_2017_hourly.csv', \
                'exchange-lv-connections_2018_hourly.csv', \
                ]

            for data_file in files:
                print("Reading {0}".format(data_file))
                with open(data_path + data_file) as f:
                    csv_reader = csv.reader(f, delimiter=';')
                    ridx = 0
                    for row in csv_reader:
                        if ridx == 0:
                            pass
                        elif ridx == 1:
                            pass
                        elif ridx == 2:
                            # get headers
                            all_cols = row[2:]
                            rel_cols = []
                            for idx, col in enumerate(all_cols):
                                if col.split('-').__len__() > 1:
                                    rel_cols.append(idx)  # keep all columns
                        else:
                            # reformat date
                            dmy = row[0].split('-')
                            date = dmy[2] + dmy[1] + dmy[0]
                            # reformat hour
                            hour = row[1][0:2]
                            time = date + ':' + hour
                            # print(time)

                            # loop over values in row
                            for idx in rel_cols:
                                transfer = all_cols[idx]
                                val = row[2 + idx]
                                if val == '':  # repliace missing data with NULL
                                    val = 'NULL'
                                # insert into database
                                cmd = 'INSERT INTO exchange (time,transfer,MWh) values("{0}","{1}",{2})'.format(time,
                                                                                                                transfer,
                                                                                                                val)
                                c.execute(cmd)
                        ridx += 1

        # PRICES

        if 0:

            # norwegian areas are saved with different names
            no_areas = {  # 'NO4':'Molde',
                'NO1': 'Oslo',
                'NO2': 'Kr.sand',
                'NO5': 'Bergen',
                'NO3': 'Tr.heim',
                'NO4': 'Tromsø',
            }

            no_rareas = {}
            for f in no_areas.keys():
                no_rareas[no_areas[f]] = f

            c.execute('DROP TABLE IF EXISTS spotprice')
            c.execute('CREATE TABLE spotprice (' + \
                      'time TEXT NOT NULL,' + \
                      'area TEXT NOT NULL,' + \
                      'EUR REAL' + \
                      ')')

            data_path = 'C:/Users/elisn/Box Sync/Data/Nordpool/Prices/'

            files = [ \
                'elspot-prices_2013_hourly_eur.csv', \
                'elspot-prices_2014_hourly_eur.csv', \
                'elspot-prices_2015_hourly_eur.csv', \
                'elspot-prices_2016_hourly_eur.csv', \
                'elspot-prices_2017_hourly_eur.csv', \
                'elspot-prices_2018_hourly_eur.csv', \
                ]

            for data_file in files:
                print("Reading {0}".format(data_file))
                with open(data_path + data_file) as f:
                    csv_reader = csv.reader(f, delimiter=';')
                    ridx = 0
                    for row in csv_reader:
                        if ridx == 0:
                            pass
                        elif ridx == 1:
                            pass
                        elif ridx == 2:
                            # get headers
                            all_cols = row[2:]
                            rel_cols = []
                            rel_col_names = []
                            for idx, col in enumerate(all_cols):
                                if col in area_codes + ['SYS']:
                                    rel_cols.append(idx)
                                    rel_col_names.append(col)
                                elif col in no_rareas:
                                    rel_cols.append(idx)
                                    rel_col_names.append(no_rareas[col])
                        else:
                            # reformat date
                            dmy = row[0].split('-')
                            date = dmy[2] + dmy[1] + dmy[0]
                            # reformat hour
                            hour = row[1][0:2]
                            time = date + ':' + hour
                            # print(time)

                            # loop over values in row
                            for i, idx in enumerate(rel_cols):
                                area = rel_col_names[i]
                                val = row[2 + idx].replace(',', '.')
                                if val == '':  # repliace missing data with NULL
                                    val = 'NULL'
                                # insert into database
                                cmd = 'INSERT INTO spotprice (time,area,EUR) values("{0}","{1}",{2})'.format(time, area,
                                                                                                             val)
                                c.execute(cmd)
                        ridx += 1

        ## HYDRO RESERVIOR
        if 0:
            c.execute('DROP TABLE IF EXISTS reservoir')
            c.execute('CREATE TABLE reservoir (' + \
                      'time TEXT NOT NULL,' + \
                      'area TEXT NOT NULL,' + \
                      'GWh REAL' + \
                      ')')

            data_path = 'C:/Users/elisn/Box Sync/Data/Nordpool/Hydro/'
            files = [
                'hydro-reservoir_2013_weekly.csv',
                'hydro-reservoir_2014_weekly.csv',
                'hydro-reservoir_2015_weekly.csv',
                'hydro-reservoir_2016_weekly.csv',
            ]

            for data_file in files:
                print("Reading {0}".format(data_file))
                with open(data_path + data_file) as f:
                    csv_reader = csv.reader(f, delimiter=';')
                    ridx = 0
                    for row in csv_reader:
                        # print(row)

                        if ridx == 0:
                            pass
                        elif ridx == 1:
                            pass
                        elif ridx == 2:
                            # get headers
                            all_cols = row[1:]
                        else:
                            # reformat date
                            wy = row[0].split('-')
                            week = wy[0].strip()
                            year = wy[1].strip()
                            time = '20{0}'.format(year) + ':' + week

                            # loop over values in row
                            for idx, area in enumerate(all_cols):
                                val = row[1 + idx]
                                if val == '':  # repliace missing data with NULL
                                    val = 'NULL'
                                # insert into database
                                cmd = 'INSERT INTO reservoir (time,area,GWh) values("{0}","{1}",{2})'.format(time, area,
                                                                                                             val)
                                c.execute(cmd)
                        ridx += 1

            data_path = 'C:/Users/elisn/Box Sync/Data/Nordpool/Hydro/'
            files = [
                'hydro-reservoir-no-1.csv',
                'hydro-reservoir-no-2.csv',
                'hydro-reservoir-se-1.csv',
                'hydro-reservoir-se-2.csv',
            ]

            for data_file in files:
                print("Reading {0}".format(data_file))
                with open(data_path + data_file) as f:
                    csv_reader = csv.reader(f, delimiter=';')
                    ridx = 0
                    for row in csv_reader:
                        # print(row)

                        if ridx == 0:
                            pass
                        elif ridx == 1:
                            # get headers
                            all_cols = row[1:]
                        else:
                            # reformat date
                            wy = row[0].split('-')
                            week = wy[0].strip()
                            year = wy[1].strip()
                            time = '20{0}'.format(year) + ':' + week

                            # loop over values in row
                            for idx, area in enumerate(all_cols):
                                val = row[1 + idx]
                                if val == '':  # repliace missing data with NULL
                                    val = 'NULL'
                                # insert into database
                                cmd = 'INSERT INTO reservoir (time,area,GWh) values("{0}","{1}",{2})'.format(time, area,
                                                                                                             val)
                                c.execute(cmd)
                        ridx += 1

            # hydro reservoir for finland
            data_path = 'C:/Users/elisn/Box Sync/Data/Nordpool/Hydro/'
            files = [
                'hydro-reservoir-fi-1.csv',
            ]

            for data_file in files:
                print("Reading {0}".format(data_file))
                with open(data_path + data_file) as f:
                    csv_reader = csv.reader(f, delimiter=';')
                    ridx = 0
                    for row in csv_reader:

                        if ridx == 0:
                            pass
                        elif ridx == 1:
                            # get headers
                            all_cols = row[1:]
                        else:
                            # reformat date
                            week = str(row[0])
                            if week.__len__() < 2:
                                week = '0' + week

                            # loop over values in row
                            for idx, year in enumerate(all_cols):
                                val = row[1 + idx]
                                if val == '':  # repliace missing data with NULL
                                    val = 'NULL'
                                time = year + ':' + week
                                # insert into database
                                cmd = 'INSERT INTO reservoir (time,area,GWh) values("{0}","{1}",{2})'.format(time, 'FI',
                                                                                                             val)
                                c.execute(cmd)
                        ridx += 1

        conn.commit()
        conn.close()

    def add_uk_prices(self):

        # make sqlite database
        conn = sqlite3.connect(self.db)
        c = conn.cursor()

        data_path = 'C:/Users/elisn/Box Sync/Data/Nordpool/Prices/'

        files = [ \
            'n2ex-day-ahead-auction-prices_2015_hourly_eur.csv', \
            'n2ex-day-ahead-auction-prices_2016_hourly_eur.csv', \
            'n2ex-day-ahead-auction-prices_2017_hourly_eur.csv', \
            'n2ex-day-ahead-auction-prices_2018_hourly_eur.csv', \
            'n2ex-day-ahead-auction-prices_2019_hourly_eur.csv', \
            ]

        for data_file in files:
            print("Reading {0}".format(data_file))
            with open(data_path + data_file) as f:
                csv_reader = csv.reader(f, delimiter=';')
                ridx = 0
                for row in csv_reader:
                    if ridx == 0:
                        pass
                    elif ridx == 1:
                        pass
                    elif ridx == 2:
                        pass

                    else:
                        # reformat date
                        dmy = row[0].split('-')
                        date = dmy[2] + dmy[1] + dmy[0]
                        # reformat hour
                        # Note: first get UK time
                        hour = row[2][0:2]
                        uk_time = date + ':' + hour
                        t = str_to_date(uk_time)
                        cet_time = (t + datetime.timedelta(seconds=-3600)).strftime('%Y%m%d:%H')

                        area = 'UK'
                        val = row[3].replace(',', '.')
                        if val == '':  # repliace missing data with NULL
                            val = 'NULL'
                        # insert into database
                        cmd = 'INSERT INTO spotprice (time,area,EUR) values("{0}","{1}",{2})'.format(cet_time, area,
                                                                                                     val)
                        c.execute(cmd)
                    ridx += 1

        conn.commit()
        conn.close()

    def calculate_inflow_data(self):
        """ Calculate weekly series for hydro reservoir inflow. This is done
        by using the hydro production time series (from ENTSO-E and SCB for SE)
        and reservoir storage levels (from Nordpool).
        New table:

            inflow(time TEXT, area TEXT, GWh FLOAT)
                - week in format 'YYYY:WW'

        """
        pd_reservoir = self.select_data(table='reservoir')

        # get hydro production from entsoe database
        import entsoe_transparency_db as entsoe
        entsoe_db = entsoe.Database()

        types = ['Hydro pump', 'Hydro ror', 'Hydro res']
        pd_generation = entsoe_db.select_gen_per_type_data(types=types, starttime='20150101', endtime='20190101')
        pd_se_generation = entsoe_db.select_se_gen_per_type_data(types=['Hydro'], starttime='20150101',
                                                                 endtime='20190101')
        # load monthly SE hydro data from SCB (GWh)
        wb = openpyxl.load_workbook('Data/SCB_elproduktion_per_manad.xlsx')
        ws = wb['EN0108A3']

        dstart = ws.cell(3, 2).value

        dates = pd.date_range(start='{0}-{1}-01'.format(dstart[0:4], dstart[5:7]), \
                              periods=ws.max_column - 2, freq='M')

        se_hydro = pd.Series(index=dates, dtype=float)
        for cidx in range(0, ws.max_column - 2):
            se_hydro.iloc[cidx] = ws.cell(5, cidx + 2).value

        # Aggregate hydro production for different hydro types
        cols = ['NO1', 'NO2', 'NO3', 'NO4', 'NO5', 'FI', 'SE']
        hydro_prod = pd.DataFrame(index=pd_generation['NO1'].index, columns=cols)
        for area in ['NO1', 'NO2', 'NO3', 'NO4', 'NO5', 'FI']:
            hydro_prod.loc[:, area] = pd_generation[area].sum(axis=1)
        for area in ['SE1', 'SE2', 'SE3', 'SE4']:
            hydro_prod.loc[:, area] = pd_se_generation[area].sum(axis=1)

        ## CALCULATE WEEKLY HYDRO PRODUCTION ##

        hydro_weekly = pd.DataFrame(index=pd_reservoir.index,
                                    columns=['NO1', 'NO2', 'NO3', 'NO4', 'NO5', 'FI', 'SE', 'SE1', 'SE2', 'SE3', 'SE4'],
                                    dtype=float)
        for area in ['NO1', 'NO2', 'NO3', 'NO4', 'NO5', 'FI', 'SE1', 'SE2', 'SE3', 'SE4']:
            for idx in range(hydro_weekly.index.__len__() - 1):
                # get production for current week
                sdate = week_to_date(hydro_weekly.index[idx])
                edate = week_to_date(hydro_weekly.index[idx + 1])
                drange = hydro_prod.loc[(hydro_prod.index >= sdate) * (hydro_prod.index < edate), area]
                if drange.__len__() > 0:
                    hydro_weekly.at[hydro_weekly.index[idx], area] = drange.sum()  # note: summing over nan gives 0

        #    # for SE1-SE4 use svk data
        #    for area in ['SE1','SE2','SE3','SE4']:
        #        for idx in range(hydro_weekly.index.__len__()-1):
        #             # get production for current week
        #            sdate = week_to_date(hydro_weekly.index[idx])
        #            edate = week_to_date(hydro_weekly.index[idx+1])
        #            drange = hydro_prod.loc[(hydro_prod.index >= sdate) * (hydro_prod.index < edate),area]
        #            if drange.__len__() > 0:
        #                hydro_weekly.loc[hydro_weekly.index[idx],area] = drange.sum() # note: summing over nan gives 0
        #
        # for SE impute weekly production from monthly data
        for idx in range(hydro_weekly.index.__len__() - 1):
            sdate = week_to_date(hydro_weekly.index[idx])
            edate = week_to_date(hydro_weekly.index[idx + 1])
            if sdate.month == edate.month:  # if all days are within the same month
                # find number of days in month
                if sdate.month == 12:
                    ndays = (datetime.date(sdate.year + 1, 1, 1) - datetime.date(sdate.year, 12, 1)).days
                else:
                    ndays = (datetime.date(sdate.year, sdate.month + 1, 1) - datetime.date(sdate.year, sdate.month,
                                                                                           1)).days
                try:
                    hydro_weekly.at[hydro_weekly.index[idx], 'SE'] = se_hydro.at[sdate.strftime('%Y-%m')][0] * 7 / ndays
                except KeyError as err:  # SE monthly hydro data does not cover whole range for hydro_weekly, accept key errors when accessing se_hydro
                    print(str(err))
            else:  # week overlapping two months
                ndays1 = (datetime.date(edate.year, edate.month, 1) - datetime.date(sdate.year, sdate.month, 1)).days
                if edate.month == 12:
                    ndays2 = (datetime.date(edate.year + 1, 1, 1) - datetime.date(edate.year, 12, 1)).days
                else:
                    ndays2 = (datetime.date(edate.year, edate.month + 1, 1) - datetime.date(edate.year, edate.month,
                                                                                            1)).days
                # find number of days in each month
                m1days = 0
                while sdate.month == (sdate + datetime.timedelta(days=m1days + 1)).month:
                    m1days += 1
                try:
                    # calculate production this week by weighting contributions from months
                    hydro_weekly.at[hydro_weekly.index[idx], 'SE'] = \
                        m1days / ndays1 * se_hydro.loc[sdate.strftime('%Y-%m')][0] \
                        + (7 - m1days) / ndays2 * se_hydro.loc[edate.strftime('%Y-%m')][0]
                except KeyError as err:  # accept key errors when accessing se_hydro
                    print(str(err))
        # convert to GWh
        for area in ['NO1', 'NO2', 'NO3', 'NO4', 'NO5', 'FI', 'SE1', 'SE2', 'SE3', 'SE4']:
            hydro_weekly.loc[:, area] = hydro_weekly.loc[:, area] / 1000

        hydro_weekly.index = [week_to_date(w) for w in hydro_weekly.index]
        plt.plot(hydro_weekly)
        plt.legend(hydro_weekly.columns)
        plt.grid()
        plt.ylabel('GWh')
        plt.title('Weekly Hydro Production')

        #        plt.figure()
        #        plt.plot(hydro_weekly['SE'])
        #        plt.plot(hydro_weekly.loc[:,['SE1','SE2','SE3','SE4']].sum(axis=1))

        fig = plt.gcf()
        cm_per_inch = 2.5
        fig.set_size_inches(20 / cm_per_inch, 10 / cm_per_inch)

        plt.savefig('Figures/hydro_weekly.png')
        plt.savefig('Figures/hydro_weekly.pdf')
        plt.savefig('Figures/hydro_weekly.eps')
        plt.show()

        hydro_weekly.index = [date_to_week(w) for w in hydro_weekly.index]

        # Calculate inflow (in GWh) as
        # V(t)=P(t)+M(t)-M(t-1)
        # P - hydro_weekly, M - pd_reservoir
        # Note: P(t) is the production during a week, M(t) from Nordpool data is the
        # reservoir content at the END of a week
        Vweekly = pd.DataFrame(columns=hydro_weekly.columns, index=hydro_weekly.index, dtype=float)
        for area in ['NO1', 'NO2', 'NO3', 'NO4', 'NO5', 'FI', 'SE', 'SE1', 'SE2', 'SE3', 'SE4']:
            for idx in range(1, hydro_weekly.__len__()):
                Vweekly.at[Vweekly.index[idx], area] = \
                    hydro_weekly.at[Vweekly.index[idx], area] \
                    + pd_reservoir.at[Vweekly.index[idx], area] \
                    - pd_reservoir.at[Vweekly.index[idx - 1], area]

        # remove few negative values
        Vweekly[Vweekly < 0] = 0

        # Load SE weekly inflow data - from Energiföretagen
        wb = openpyxl.load_workbook('Data/Inflow_SE_2000-2018.xlsx')
        ws = wb['Inflow - Sweden']

        years = range(2000, 2019)
        cidxs = range(3, 22)

        index = []
        for year in years:
            index = index + ['{0}:{1}'.format(year, week) for week in weekdd]

        # inflow_SE = pd.DataFrame(dtype=float,index=range(1,53),columns=years)
        inflow_SE = pd.DataFrame(dtype=float, index=index, columns=['SE'])
        hrow = 8  # row with headers
        for cidx in cidxs:
            year = ws.cell(hrow, cidx).value
            for ridx in range(hrow + 1, hrow + 54):
                week = ws.cell(ridx, 2).value
                val = ws.cell(ridx, cidx).value
                if not val is None:
                    if week < 53:
                        # inflow_SE.loc[week,year] = val
                        inflow_SE.at['{0}:{1}'.format(year, weekdd[week - 1]), 'SE'] = val
                    else:  # add week 53 to week 52
                        # inflow_SE.loc[52,year] = inflow_SE.loc[52,year] + val
                        inflow_SE.at['{0}:52'.format(year), 'SE'] = inflow_SE.at['{0}:52'.format(year), 'SE'] + val

        # Compare calculated inflow with measured values

        # Convert scales to datetime
        inflow_SE.index = [week_to_date(w) for w in inflow_SE.index]
        Vweekly.index = [week_to_date(w) for w in Vweekly.index]

        # find dates for which there is data
        xdates = Vweekly.index[pd.isna(Vweekly.loc[:, 'SE1']) == False]
        # compure rmse
        rmse1 = np.sqrt(np.mean(np.square(inflow_SE.loc[xdates, 'SE'] - Vweekly.loc[xdates, 'SE'])))
        rmse2 = np.sqrt(np.mean(
            np.square(inflow_SE.loc[xdates, 'SE'] - Vweekly.loc[xdates, ['SE1', 'SE2', 'SE3', 'SE4']].sum(axis=1))))
        print('Monthly rmse: {0:.4}'.format(rmse1))
        print('Hourly rmse: {0:.4}'.format(rmse2))
        rmse = np.sqrt(np.mean(np.square(inflow_SE.loc[Vweekly.index, 'SE'] - Vweekly.loc[:, 'SE'])))
        print('Inflow rmse: {}'.format(rmse))

        # plt.plot(Vweekly.loc[xdates,'SE'])
        plt.plot(Vweekly.loc[xdates, ['SE1', 'SE2', 'SE3', 'SE4']].sum(axis=1))
        plt.plot(inflow_SE.loc[xdates, 'SE'])
        plt.legend(['Reconstructed', 'Data from Energiföretagen'])
        plt.grid()
        plt.ylabel('GWh')
        plt.title('SE reservoir inflow, RMSE = {0:.4}'.format(rmse2))
        fig = plt.gcf()
        cm_per_inch = 2.5
        fig.set_size_inches(20 / cm_per_inch, 10 / cm_per_inch)

        plt.savefig('Figures/reservoir_inflow_se_hourly.png')
        plt.savefig('Figures/reservoir_inflow_se_hourly.eps')
        plt.savefig('Figures/reservoir_inflow_se_hourly.pdf')
        plt.show()

        ## PUT INFLOW DATA INTO DATABASE

        # make sqlite database
        conn = sqlite3.connect(self.db)
        c = conn.cursor()

        c.execute('DROP TABLE IF EXISTS inflow')
        c.execute('CREATE TABLE inflow (' + \
                  'time TEXT NOT NULL,' + \
                  'area TEXT NOT NULL,' + \
                  'GWh REAL' + \
                  ')')

        # convert index back to weeks
        Vweekly.index = [date_to_week(date) for date in Vweekly.index]

        for area in Vweekly.columns:
            for week in Vweekly.index:
                val = Vweekly.at[week, area]
                if not pd.isna(val):
                    cmd = "INSERT INTO inflow (time,area,GWh) values ('{0}','{1}',{2})".format(week, area, val)
                    c.execute(cmd)

        conn.commit()
        conn.close()

        # !!! NO COMPENSATION FOR UTC/CET -> wrong data !!!

    def update_wind_data(self):  # Note: Bad style, merge data sets in model instead,

        conn = sqlite3.connect(self.db)
        c = conn.cursor()
        # Nordpool data is missing wind production for FI, NO, take entso-e data instead
        import entsoe_transparency_db as entsoe
        entsoe_db = entsoe.Database()

        data = entsoe_db.select_gen_per_type_data(areas=['FI', 'NO1', 'NO2', 'NO3', 'NO4', 'NO5'],
                                                  types=['Wind onsh', 'Wind offsh'])
        # sum onshore and offshore
        for area in data:
            data[area].loc[:, 'Wind Total'] = data[area].sum(axis=1)

        print('Reading wind production data from ENTSO-E transparency database')
        # put data into nordpool database
        for area in data:
            for idx in data[area].index:
                cmd = "INSERT INTO wind (time,area,MWh) values ('{0}','{1}',{2})".format(idx.strftime('%Y%m%d:%H'),
                                                                                         area, data[area].at[
                                                                                             idx, 'Wind Total'])
                c.execute(cmd)

        conn.commit()
        conn.close()

    def query_categories(self, table):
        """ Query database to find values for 'area' and 'transfer'
        """

        if table not in self.tables:
            print('Could not find table ''{0}'' in database'.format(table))
            return None
        else:

            conn = sqlite3.connect(self.db)
            c = conn.cursor()

            if table == 'exchange':
                category = 'transfer'
            else:
                category = 'area'

            cmd = 'SELECT DISTINCT {1} FROM {0}'.format(table, category)
            c.execute(cmd)

            values = []
            for res in c:
                values.append(res[0])
            return values

    def select_data(self, table, categories=[], starttime='', endtime='', excelfile=''):
        """ Select time series from sqlite database with nordpool data. Data
        is returned as a pandas dataframe, and optionally exported to excel file.

        Input:
            areas - list of areas to choose, or in the case of table 'exchange'
                    the list of transfers, by default all categories are selected
            start - string with starting date in format "YYYYMMDD:HH"
            end - string with ending date in format "YYYYMMDD:HH"
            excelfile - path to excel file where data should be stored

        Output:
            pd_data - pandas dataframe with one column for each time series
        """

        if not type(starttime) is str:
            starttime = str(starttime)
        if not type(endtime) is str:
            endtime = str(endtime)

        conn = sqlite3.connect(self.db)
        c = conn.cursor()

        if table == 'exchange':
            cmd = "SELECT MWh,time,transfer FROM exchange"
        elif table == 'consumption':
            cmd = "SELECT MWh,time,area FROM consumption"
        elif table == 'wind':
            cmd = "SELECT DISTINCT MWh,time,area FROM wind"
        elif table == 'spotprice':
            cmd = "SELECT EUR,time,area FROM spotprice"
        elif table == 'reservoir':
            cmd = "SELECT DISTINCT GWh,time,area FROM reservoir"
        elif table == 'inflow':
            cmd = "SELECT GWh,time,area FROM inflow"
        elif table == 'production':
            cmd = "SELECT MWh,time,area FROM production"
        else:
            print('Table ''{0}'' does not exist'.format(table))
            return None

        cmd_max = "SELECT max(time) FROM {0}".format(table)
        cmd_min = "SELECT min(time) FROM {0}".format(table)

        # check if some categories don't exist in table:
        for cat in categories:
            if cat not in self.cat[table]:
                print('Category ''{0}'' does not exist in table ''{1}'''.format(cat, table))

        if categories != []:
            str_categories = '('
            for idx, cat in enumerate(categories):
                if idx > 0:
                    str_categories += ",'{0}'".format(cat)
                else:
                    str_categories += "'{0}'".format(cat)
            str_categories += ')'

        conditions = []
        if categories != []:
            if table == 'exchange':
                cat_cnd = 'transfer in ' + str_categories
            else:
                cat_cnd = 'area in ' + str_categories
            conditions.append('category')
        if starttime != '':
            start_cnd = 'time >= ' + "'{0}'".format(starttime)
            conditions.append('start')
        if endtime != '':
            end_cnd = 'time <= ' + "'{0}'".format(endtime)
            conditions.append('end')

        n = conditions.__len__()
        if n > 0:
            cmd += ' WHERE '
            cmd_max += ' WHERE '
            cmd_min += ' WHERE '
            for idx, cnd in enumerate(conditions):
                if idx > 0:
                    cmd += ' AND '
                    cmd_max += ' AND '
                    cmd_min += ' AND '
                if cnd == 'category':
                    cmd += cat_cnd
                    cmd_max += cat_cnd
                    cmd_min += cat_cnd
                elif cnd == 'start':
                    cmd += start_cnd
                    cmd_max += start_cnd
                    cmd_min += start_cnd
                elif cnd == 'end':
                    cmd += end_cnd
                    cmd_max += end_cnd
                    cmd_min += end_cnd
                else:
                    print('Unknown condition type: {0}'.format(c))

        c.execute(cmd_min)
        for row in c:
            start = row[0]
        c.execute(cmd_max)
        for row in c:
            end = row[0]

        if start is None:  # if query gave no results then return None
            print("Queries for time range gave no result, returning None")
            return None

        if table == 'reservoir' or table == 'inflow':  # different time format
            # sdate = datetime.datetime(int(start[0:4]),1,1) + datetime.timedelta(days=7*(int(start[5:7])-1))
            # edate = datetime.datetime(int(end[0:4]),1,1) + datetime.timedelta(days=7*(int(end[5:7])-1))
            # dates = pd.date_range(start=sdate,end=edate,freq='7D')
            start_year = int(start[0:4])
            start_week = int(start[5:])
            end_year = int(end[0:4])
            end_week = int(end[5:])
            dd_numbers = ['0{0}'.format(i) for i in range(1, 10)] + [str(i) for i in range(10, 53)]
            if start_year != end_year:
                firstperiod = ['{0}:{1}'.format(start_year, dd_numbers[w]) for w in range(start_week - 1, 52)]
                lastperiod = ['{0}:{1}'.format(end_year, dd_numbers[w]) for w in range(0, end_week)]
                middleperiod = []
                for y in range(start_year + 1, end_year):
                    middleperiod += ['{0}:{1}'.format(y, dd_numbers[w]) for w in range(0, 52)]
                dates = firstperiod + middleperiod + lastperiod
            else:
                dates = ['{0}:{1}'.format(start_year, dd_numbers[w]) for w in range(start_week - 1, end_week)]
        else:
            # create index for data frame
            sdate = datetime.datetime(int(start[0:4]), int(start[4:6]), int(start[6:8]), int(start[9:11]))
            edate = datetime.datetime(int(end[0:4]), int(end[4:6]), int(end[6:8]), int(end[9:11]))
            dates = pd.date_range(start=sdate, end=edate, freq='H')

        # find columns for data frame
        if categories == []:  # all areas selected by default
            categories = self.cat[table]

        # allocate panda data frame for data
        pd_data = pd.DataFrame( \
            dtype=float, \
            index=dates, \
            columns=categories)

        # get data
        # print(cmd)
        c.execute(cmd)
        for row in c:
            if table == 'reservoir' or table == 'inflow':  # different time format
                date = date = row[1]
            else:
                date = datetime.datetime(int(row[1][0:4]), int(row[1][4:6]), int(row[1][6:8]), int(row[1][9:11]))
            pd_data.at[date, row[2]] = row[0]

        conn.close()

        return pd_data


if __name__ == "__main__":
    db = Database()

    #    db.add_uk_prices()

    data = db.select_data(table='spotprice', categories=['SYS', 'UK'], starttime='20180101:00', endtime='20180102:23')

    # db.calculate_inflow_data()

#    # make sqlite database
#    conn = sqlite3.connect(db.db)
#    c = conn.cursor()
#
#
#    data_path = 'C:/Users/elisn/Box Sync/Data/Nordpool/Prices/'
#
#    files = [ \
#             'n2ex-day-ahead-auction-prices_2015_hourly_eur.csv', \
#             'n2ex-day-ahead-auction-prices_2016_hourly_eur.csv', \
#             'n2ex-day-ahead-auction-prices_2017_hourly_eur.csv', \
#             'n2ex-day-ahead-auction-prices_2018_hourly_eur.csv', \
#             'n2ex-day-ahead-auction-prices_2019_hourly_eur.csv', \
#             ]
#
#    for data_file in files:
#        print("Reading {0}".format(data_file))
#        with open(data_path+data_file) as f:
#            csv_reader = csv.reader(f,delimiter=';')
#            ridx = 0
#            for row in csv_reader:
#                if ridx == 0:
#                    pass
#                elif ridx == 1:
#                    pass
#                elif ridx == 2:
#                    pass
#
#                else:
#                    # reformat date
#                    dmy = row[0].split('-')
#                    date = dmy[2] + dmy[1] + dmy[0]
#                    # reformat hour
#                    # Note: first get UK time
#                    hour = row[2][0:2]
#                    uk_time = date + ':' + hour
#                    t = str_to_date(uk_time)
#                    cet_time = (t + datetime.timedelta(seconds=-3600)).strftime('%Y%m%d:%H')
#
#                    area = 'UK'
#                    val = row[3].replace(',','.')
#                    if val == '': # repliace missing data with NULL
#                        val = 'NULL'
#                    # insert into database
#                    cmd = 'INSERT INTO spotprice (time,area,EUR) values("{0}","{1}",{2})'.format(cet_time,area,val)
#                    print(cmd)
#                    c.execute(cmd)
#                ridx += 1
#
#
#    conn.commit()
#    conn.close()

# -*- coding: utf-8 -*-
""" Build sqlite database 'nordpool.db' from nordpool csv data. The tables are:

    consumption(TEXT time, TEXT area, FLOAT MWh)
        - consumption in each area

    production(TEXT time, TEXT area, FLOAT MWh)
        - production in each area

    wind(TEXT time, TEXT area, FLOAT MWh) 
        - wind production in each area, not for NO or FI

    spotprice(TEXT time, TEXT area, FLOAT EUR) 
        - spot price in each area, also has system price under SYS

    exchange(TEXT time, TEXT transfer, FLOAT MWh)
        - transfer between areas, transfer is e.g. 'SE1 - SE2'

    reservoir(TEXT time, TEXT area, FLOAT GWh)
        - reservoir content, with area = country, SE, FI, NO, time: 'YYYY:WW'

    inflow(time TEXT, area TEXT, GWh FLOAT)
        - hydro reservoir inflow, calculated from reservoir content
          and hydro production, time: 'YYYY:WW'

    time: 'YYYYMMDD:HH'


    Note: For nordpool data for hour 00-01 we have used time stamp YYYYMMDD:00
    ENTSO-E data in UTC, Nordpool data in CET=UTC+1:
    Nordpool  -> Entsoe
    20180101:00  20171231:23
    20180101:01  20180101:00

Created on Wed Jan 23 14:09:59 2019

@author: elisn
"""

import csv
import sqlite3
from pathlib import Path
import datetime
import pandas as pd
import numpy as np
import openpyxl
from help_functions import week_to_date, date_to_week, weekdd, str_to_date
import matplotlib.pyplot as plt

tables = ['consumption', 'exchange', 'wind', 'spotprice', 'reservoir', 'inflow', 'production']


class Database():

    def __init__(self, db='Data/nordpool.db'):

        self.db = db
        self.tables = tables
        self.cat = {}
        # if database file exists, query tables to find valid categories
        if Path(self.db).exists():
            # query categories
            for tab in self.tables:
                self.cat[tab] = self.query_categories(tab)
        else:
            for tab in self.tables:
                self.cat[tab] = []

    def create_database(self):

        # make sqlite database
        conn = sqlite3.connect(self.db)
        c = conn.cursor()

        area_codes = ['SE1', 'SE2', 'SE3', 'SE4', 'DK1', 'DK2', 'EE', 'LT', 'LV', \
                      'FI', 'NO1', 'NO2', 'NO3', 'NO4', 'NO5']

        ## PRODUCTION
        if 1:
            c.execute('DROP TABLE IF EXISTS production')
            c.execute('CREATE TABLE production (' + \
                      'time TEXT NOT NULL,' + \
                      'area TEXT NOT NULL,' + \
                      'MWh REAL' + \
                      ')')

            data_path = 'C:/Users/elisn/Box Sync/Data/Nordpool/Production/'

            files = []
            for area in ['dk', 'se', 'no']:
                files = files + ['production-{0}-areas_{1}_hourly.csv'.format(area, year) for year in range(2013, 2019)]
            files = files + ['production-per-country_{0}_hourly.csv'.format(year) for year in range(2013, 2019)]

            for data_file in files:
                print("Reading {0}".format(data_file))
                with open(data_path + data_file) as f:
                    csv_reader = csv.reader(f, delimiter=';')
                    ridx = 0
                    for row in csv_reader:
                        if ridx == 0:
                            pass
                        elif ridx == 1:
                            pass
                        elif ridx == 2:
                            # get headers
                            all_cols = row[2:]
                            rel_cols = []
                            for idx, col in enumerate(all_cols):
                                if col in area_codes:
                                    rel_cols.append(idx)
                        else:
                            # reformat date
                            dmy = row[0].split('-')
                            date = dmy[2] + dmy[1] + dmy[0]
                            # reformat hour
                            hour = row[1][0:2]
                            time = date + ':' + hour

                            # loop over values in row
                            for idx in rel_cols:
                                area = all_cols[idx]
                                val = row[2 + idx]
                                if val == '':  # repliace missing data with NULL
                                    val = 'NULL'
                                # insert into database
                                cmd = 'INSERT INTO production (time,area,MWh) values("{0}","{1}",{2})'.format(time,
                                                                                                              area, val)
                                c.execute(cmd)
                        ridx += 1

        ## CONSUMPTION
        if 0:
            c.execute('DROP TABLE IF EXISTS consumption')
            c.execute('CREATE TABLE consumption (' + \
                      'time TEXT NOT NULL,' + \
                      'area TEXT NOT NULL,' + \
                      'MWh REAL' + \
                      ')')

            data_path = 'C:/Users/elisn/Box Sync/Data/Nordpool/Consumption/'
            files = [ \
                'consumption-dk-areas_2014_hourly.csv', \
                'consumption-dk-areas_2015_hourly.csv', \
                'consumption-dk-areas_2016_hourly.csv', \
                'consumption-dk-areas_2017_hourly.csv', \
                'consumption-dk-areas_2018_hourly.csv', \
                'consumption-no-areas_2014_hourly.csv', \
                'consumption-no-areas_2015_hourly.csv', \
                'consumption-no-areas_2016_hourly.csv', \
                'consumption-no-areas_2017_hourly.csv', \
                'consumption-no-areas_2018_hourly.csv', \
                'consumption-se-areas_2014_hourly.csv', \
                'consumption-se-areas_2015_hourly.csv', \
                'consumption-se-areas_2016_hourly.csv', \
                'consumption-se-areas_2017_hourly.csv', \
                'consumption-se-areas_2018_hourly.csv', \
                'consumption-per-country_2013_hourly.csv', \
                'consumption-per-country_2014_hourly.csv', \
                'consumption-per-country_2015_hourly.csv', \
                'consumption-per-country_2016_hourly.csv', \
                'consumption-per-country_2017_hourly.csv', \
                'consumption-per-country_2018_hourly.csv', \
                ]

            for data_file in files:
                print("Reading {0}".format(data_file))
                with open(data_path + data_file) as f:
                    csv_reader = csv.reader(f, delimiter=';')
                    ridx = 0
                    for row in csv_reader:
                        # print(row)

                        if ridx == 0:
                            pass
                        elif ridx == 1:
                            pass
                        elif ridx == 2:
                            # get headers
                            all_cols = row[2:]
                            rel_cols = []
                            for idx, col in enumerate(all_cols):
                                if col in area_codes:
                                    rel_cols.append(idx)
                        else:
                            # reformat date
                            dmy = row[0].split('-')
                            date = dmy[2] + dmy[1] + dmy[0]
                            # reformat hour
                            hour = row[1][0:2]
                            time = date + ':' + hour
                            # print(time)

                            # loop over values in row
                            for idx in rel_cols:
                                area = all_cols[idx]
                                val = row[2 + idx]
                                if val == '':  # repliace missing data with NULL
                                    val = 'NULL'
                                # insert into database
                                cmd = 'INSERT INTO consumption (time,area,MWh) values("{0}","{1}",{2})'.format(time,
                                                                                                               area,
                                                                                                               val)
                                c.execute(cmd)
                        ridx += 1

        ## WIND
        if 0:
            c.execute('DROP TABLE IF EXISTS wind')
            c.execute('CREATE TABLE wind (' + \
                      'time TEXT NOT NULL,' + \
                      'area TEXT NOT NULL,' + \
                      'MWh REAL' + \
                      ')')

            data_path = 'C:/Users/elisn/Box Sync/Data/Nordpool/Wind/'

            files = [ \
                'wind-power-dk_2013_hourly.csv', \
                'wind-power-dk_2014_hourly.csv', \
                'wind-power-dk_2015_hourly.csv', \
                'wind-power-dk_2016_hourly.csv', \
                'wind-power-dk_2017_hourly.csv', \
                'wind-power-dk_2018_hourly.csv', \
                'wind-power-ee_2013_hourly.csv', \
                'wind-power-ee_2014_hourly.csv', \
                'wind-power-ee_2015_hourly.csv', \
                'wind-power-ee_2016_hourly.csv', \
                'wind-power-ee_2017_hourly.csv', \
                'wind-power-ee_2018_hourly.csv', \
                'wind-power-lt_2013_hourly.csv', \
                'wind-power-lt_2014_hourly.csv', \
                'wind-power-lt_2015_hourly.csv', \
                'wind-power-lt_2016_hourly.csv', \
                'wind-power-lt_2017_hourly.csv', \
                'wind-power-lt_2018_hourly.csv', \
                'wind-power-lv_2013_hourly.csv', \
                'wind-power-lv_2014_hourly.csv', \
                'wind-power-lv_2015_hourly.csv', \
                'wind-power-lv_2016_hourly.csv', \
                'wind-power-lv_2017_hourly.csv', \
                'wind-power-lv_2018_hourly.csv', \
                'wind-power-se_2013_hourly.csv', \
                'wind-power-se_2014_hourly.csv', \
                'wind-power-se_2015_hourly.csv', \
                'wind-power-se_2016_hourly.csv', \
                'wind-power-se_2017_hourly.csv', \
                'wind-power-se_2018_hourly.csv', \
                ]

            for data_file in files:
                print("Reading {0}".format(data_file))
                with open(data_path + data_file) as f:
                    csv_reader = csv.reader(f, delimiter=';')
                    ridx = 0
                    for row in csv_reader:
                        if ridx == 0:
                            pass
                        elif ridx == 1:
                            pass
                        elif ridx == 2:
                            # get headers
                            all_cols = row[2:]
                            rel_cols = []
                            for idx, col in enumerate(all_cols):
                                if col in area_codes:
                                    rel_cols.append(idx)
                        else:
                            # reformat date
                            dmy = row[0].split('-')
                            date = dmy[2] + dmy[1] + dmy[0]
                            # reformat hour
                            hour = row[1][0:2]
                            time = date + ':' + hour
                            # print(time)

                            # loop over values in row
                            for idx in rel_cols:
                                area = all_cols[idx]
                                val = row[2 + idx]
                                if val == '':  # repliace missing data with NULL
                                    val = 'NULL'
                                # insert into database
                                cmd = 'INSERT INTO wind (time,area,MWh) values("{0}","{1}",{2})'.format(time, area, val)
                                c.execute(cmd)
                        ridx += 1

        ### EXCHANGE
        if 0:

            c.execute('DROP TABLE IF EXISTS exchange')
            c.execute('CREATE TABLE exchange (' + \
                      'time TEXT NOT NULL,' + \
                      'transfer TEXT NOT NULL,' + \
                      'MWh REAL' + \
                      ')')

            data_path = 'C:/Users/elisn/Box Sync/Data/Nordpool/Exchange/'

            #        files = []
            #        for cc in ['no','se','lv','lt','ee','dk']:
            #            for yy in [str(y) for y in range(2016,2019)]:
            #                files.append('exchange-{0}-connections_{1}_hourly.csv'.format(cc,yy))
            #
            files = [ \
                'exchange-se-connections_2014_hourly.csv', \
                'exchange-se-connections_2015_hourly.csv', \
                'exchange-se-connections_2016_hourly.csv', \
                'exchange-se-connections_2017_hourly.csv', \
                'exchange-se-connections_2018_hourly.csv', \
                'exchange-no-connections_2013_hourly.csv', \
                'exchange-no-connections_2014_hourly.csv', \
                'exchange-no-connections_2015_hourly.csv', \
                'exchange-no-connections_2016_hourly.csv', \
                'exchange-no-connections_2017_hourly.csv', \
                'exchange-no-connections_2018_hourly.csv', \
                'exchange-dk-connections_2013_hourly.csv', \
                'exchange-dk-connections_2014_hourly.csv', \
                'exchange-dk-connections_2015_hourly.csv', \
                'exchange-dk-connections_2016_hourly.csv', \
                'exchange-dk-connections_2017_hourly.csv', \
                'exchange-dk-connections_2018_hourly.csv', \
                'exchange-ee-connections_2015_hourly.csv', \
                'exchange-ee-connections_2016_hourly.csv', \
                'exchange-ee-connections_2017_hourly.csv', \
                'exchange-ee-connections_2018_hourly.csv', \
                'exchange-fi-connections_2013_hourly.csv', \
                'exchange-fi-connections_2014_hourly.csv', \
                'exchange-fi-connections_2015_hourly.csv', \
                'exchange-fi-connections_2016_hourly.csv', \
                'exchange-fi-connections_2017_hourly.csv', \
                'exchange-fi-connections_2018_hourly.csv', \
                'exchange-lt-connections_2016_hourly.csv', \
                'exchange-lt-connections_2017_hourly.csv', \
                'exchange-lt-connections_2018_hourly.csv', \
                'exchange-lv-connections_2016_hourly.csv', \
                'exchange-lv-connections_2017_hourly.csv', \
                'exchange-lv-connections_2018_hourly.csv', \
                ]

            for data_file in files:
                print("Reading {0}".format(data_file))
                with open(data_path + data_file) as f:
                    csv_reader = csv.reader(f, delimiter=';')
                    ridx = 0
                    for row in csv_reader:
                        if ridx == 0:
                            pass
                        elif ridx == 1:
                            pass
                        elif ridx == 2:
                            # get headers
                            all_cols = row[2:]
                            rel_cols = []
                            for idx, col in enumerate(all_cols):
                                if col.split('-').__len__() > 1:
                                    rel_cols.append(idx)  # keep all columns
                        else:
                            # reformat date
                            dmy = row[0].split('-')
                            date = dmy[2] + dmy[1] + dmy[0]
                            # reformat hour
                            hour = row[1][0:2]
                            time = date + ':' + hour
                            # print(time)

                            # loop over values in row
                            for idx in rel_cols:
                                transfer = all_cols[idx]
                                val = row[2 + idx]
                                if val == '':  # repliace missing data with NULL
                                    val = 'NULL'
                                # insert into database
                                cmd = 'INSERT INTO exchange (time,transfer,MWh) values("{0}","{1}",{2})'.format(time,
                                                                                                                transfer,
                                                                                                                val)
                                c.execute(cmd)
                        ridx += 1

        # PRICES

        if 0:

            # norwegian areas are saved with different names
            no_areas = {  # 'NO4':'Molde',
                'NO1': 'Oslo',
                'NO2': 'Kr.sand',
                'NO5': 'Bergen',
                'NO3': 'Tr.heim',
                'NO4': 'Tromsø',
            }

            no_rareas = {}
            for f in no_areas.keys():
                no_rareas[no_areas[f]] = f

            c.execute('DROP TABLE IF EXISTS spotprice')
            c.execute('CREATE TABLE spotprice (' + \
                      'time TEXT NOT NULL,' + \
                      'area TEXT NOT NULL,' + \
                      'EUR REAL' + \
                      ')')

            data_path = 'C:/Users/elisn/Box Sync/Data/Nordpool/Prices/'

            files = [ \
                'elspot-prices_2013_hourly_eur.csv', \
                'elspot-prices_2014_hourly_eur.csv', \
                'elspot-prices_2015_hourly_eur.csv', \
                'elspot-prices_2016_hourly_eur.csv', \
                'elspot-prices_2017_hourly_eur.csv', \
                'elspot-prices_2018_hourly_eur.csv', \
                ]

            for data_file in files:
                print("Reading {0}".format(data_file))
                with open(data_path + data_file) as f:
                    csv_reader = csv.reader(f, delimiter=';')
                    ridx = 0
                    for row in csv_reader:
                        if ridx == 0:
                            pass
                        elif ridx == 1:
                            pass
                        elif ridx == 2:
                            # get headers
                            all_cols = row[2:]
                            rel_cols = []
                            rel_col_names = []
                            for idx, col in enumerate(all_cols):
                                if col in area_codes + ['SYS']:
                                    rel_cols.append(idx)
                                    rel_col_names.append(col)
                                elif col in no_rareas:
                                    rel_cols.append(idx)
                                    rel_col_names.append(no_rareas[col])
                        else:
                            # reformat date
                            dmy = row[0].split('-')
                            date = dmy[2] + dmy[1] + dmy[0]
                            # reformat hour
                            hour = row[1][0:2]
                            time = date + ':' + hour
                            # print(time)

                            # loop over values in row
                            for i, idx in enumerate(rel_cols):
                                area = rel_col_names[i]
                                val = row[2 + idx].replace(',', '.')
                                if val == '':  # repliace missing data with NULL
                                    val = 'NULL'
                                # insert into database
                                cmd = 'INSERT INTO spotprice (time,area,EUR) values("{0}","{1}",{2})'.format(time, area,
                                                                                                             val)
                                c.execute(cmd)
                        ridx += 1

        ## HYDRO RESERVIOR
        if 0:
            c.execute('DROP TABLE IF EXISTS reservoir')
            c.execute('CREATE TABLE reservoir (' + \
                      'time TEXT NOT NULL,' + \
                      'area TEXT NOT NULL,' + \
                      'GWh REAL' + \
                      ')')

            data_path = 'C:/Users/elisn/Box Sync/Data/Nordpool/Hydro/'
            files = [
                'hydro-reservoir_2013_weekly.csv',
                'hydro-reservoir_2014_weekly.csv',
                'hydro-reservoir_2015_weekly.csv',
                'hydro-reservoir_2016_weekly.csv',
            ]

            for data_file in files:
                print("Reading {0}".format(data_file))
                with open(data_path + data_file) as f:
                    csv_reader = csv.reader(f, delimiter=';')
                    ridx = 0
                    for row in csv_reader:
                        # print(row)

                        if ridx == 0:
                            pass
                        elif ridx == 1:
                            pass
                        elif ridx == 2:
                            # get headers
                            all_cols = row[1:]
                        else:
                            # reformat date
                            wy = row[0].split('-')
                            week = wy[0].strip()
                            year = wy[1].strip()
                            time = '20{0}'.format(year) + ':' + week

                            # loop over values in row
                            for idx, area in enumerate(all_cols):
                                val = row[1 + idx]
                                if val == '':  # repliace missing data with NULL
                                    val = 'NULL'
                                # insert into database
                                cmd = 'INSERT INTO reservoir (time,area,GWh) values("{0}","{1}",{2})'.format(time, area,
                                                                                                             val)
                                c.execute(cmd)
                        ridx += 1

            data_path = 'C:/Users/elisn/Box Sync/Data/Nordpool/Hydro/'
            files = [
                'hydro-reservoir-no-1.csv',
                'hydro-reservoir-no-2.csv',
                'hydro-reservoir-se-1.csv',
                'hydro-reservoir-se-2.csv',
            ]

            for data_file in files:
                print("Reading {0}".format(data_file))
                with open(data_path + data_file) as f:
                    csv_reader = csv.reader(f, delimiter=';')
                    ridx = 0
                    for row in csv_reader:
                        # print(row)

                        if ridx == 0:
                            pass
                        elif ridx == 1:
                            # get headers
                            all_cols = row[1:]
                        else:
                            # reformat date
                            wy = row[0].split('-')
                            week = wy[0].strip()
                            year = wy[1].strip()
                            time = '20{0}'.format(year) + ':' + week

                            # loop over values in row
                            for idx, area in enumerate(all_cols):
                                val = row[1 + idx]
                                if val == '':  # repliace missing data with NULL
                                    val = 'NULL'
                                # insert into database
                                cmd = 'INSERT INTO reservoir (time,area,GWh) values("{0}","{1}",{2})'.format(time, area,
                                                                                                             val)
                                c.execute(cmd)
                        ridx += 1

            # hydro reservoir for finland
            data_path = 'C:/Users/elisn/Box Sync/Data/Nordpool/Hydro/'
            files = [
                'hydro-reservoir-fi-1.csv',
            ]

            for data_file in files:
                print("Reading {0}".format(data_file))
                with open(data_path + data_file) as f:
                    csv_reader = csv.reader(f, delimiter=';')
                    ridx = 0
                    for row in csv_reader:

                        if ridx == 0:
                            pass
                        elif ridx == 1:
                            # get headers
                            all_cols = row[1:]
                        else:
                            # reformat date
                            week = str(row[0])
                            if week.__len__() < 2:
                                week = '0' + week

                            # loop over values in row
                            for idx, year in enumerate(all_cols):
                                val = row[1 + idx]
                                if val == '':  # repliace missing data with NULL
                                    val = 'NULL'
                                time = year + ':' + week
                                # insert into database
                                cmd = 'INSERT INTO reservoir (time,area,GWh) values("{0}","{1}",{2})'.format(time, 'FI',
                                                                                                             val)
                                c.execute(cmd)
                        ridx += 1

        conn.commit()
        conn.close()

    def add_uk_prices(self):

        # make sqlite database
        conn = sqlite3.connect(self.db)
        c = conn.cursor()

        data_path = 'C:/Users/elisn/Box Sync/Data/Nordpool/Prices/'

        files = [ \
            'n2ex-day-ahead-auction-prices_2015_hourly_eur.csv', \
            'n2ex-day-ahead-auction-prices_2016_hourly_eur.csv', \
            'n2ex-day-ahead-auction-prices_2017_hourly_eur.csv', \
            'n2ex-day-ahead-auction-prices_2018_hourly_eur.csv', \
            'n2ex-day-ahead-auction-prices_2019_hourly_eur.csv', \
            ]

        for data_file in files:
            print("Reading {0}".format(data_file))
            with open(data_path + data_file) as f:
                csv_reader = csv.reader(f, delimiter=';')
                ridx = 0
                for row in csv_reader:
                    if ridx == 0:
                        pass
                    elif ridx == 1:
                        pass
                    elif ridx == 2:
                        pass

                    else:
                        # reformat date
                        dmy = row[0].split('-')
                        date = dmy[2] + dmy[1] + dmy[0]
                        # reformat hour
                        # Note: first get UK time
                        hour = row[2][0:2]
                        uk_time = date + ':' + hour
                        t = str_to_date(uk_time)
                        cet_time = (t + datetime.timedelta(seconds=-3600)).strftime('%Y%m%d:%H')

                        area = 'UK'
                        val = row[3].replace(',', '.')
                        if val == '':  # repliace missing data with NULL
                            val = 'NULL'
                        # insert into database
                        cmd = 'INSERT INTO spotprice (time,area,EUR) values("{0}","{1}",{2})'.format(cet_time, area,
                                                                                                     val)
                        c.execute(cmd)
                    ridx += 1

        conn.commit()
        conn.close()

    def calculate_inflow_data(self):
        """ Calculate weekly series for hydro reservoir inflow. This is done
        by using the hydro production time series (from ENTSO-E and SCB for SE)
        and reservoir storage levels (from Nordpool).
        New table:

            inflow(time TEXT, area TEXT, GWh FLOAT)
                - week in format 'YYYY:WW'

        """
        pd_reservoir = self.select_data(table='reservoir')

        # get hydro production from entsoe database
        import entsoe_transparency_db as entsoe
        entsoe_db = entsoe.Database()

        types = ['Hydro pump', 'Hydro ror', 'Hydro res']
        pd_generation = entsoe_db.select_gen_per_type_data(types=types, starttime='20150101', endtime='20190101')
        pd_se_generation = entsoe_db.select_se_gen_per_type_data(types=['Hydro'], starttime='20150101',
                                                                 endtime='20190101')
        # load monthly SE hydro data from SCB (GWh)
        wb = openpyxl.load_workbook('Data/SCB_elproduktion_per_manad.xlsx')
        ws = wb['EN0108A3']

        dstart = ws.cell(3, 2).value

        dates = pd.date_range(start='{0}-{1}-01'.format(dstart[0:4], dstart[5:7]), \
                              periods=ws.max_column - 2, freq='M')

        se_hydro = pd.Series(index=dates, dtype=float)
        for cidx in range(0, ws.max_column - 2):
            se_hydro.iloc[cidx] = ws.cell(5, cidx + 2).value

        # Aggregate hydro production for different hydro types
        cols = ['NO1', 'NO2', 'NO3', 'NO4', 'NO5', 'FI', 'SE']
        hydro_prod = pd.DataFrame(index=pd_generation['NO1'].index, columns=cols)
        for area in ['NO1', 'NO2', 'NO3', 'NO4', 'NO5', 'FI']:
            hydro_prod.loc[:, area] = pd_generation[area].sum(axis=1)
        for area in ['SE1', 'SE2', 'SE3', 'SE4']:
            hydro_prod.loc[:, area] = pd_se_generation[area].sum(axis=1)

        ## CALCULATE WEEKLY HYDRO PRODUCTION ##

        hydro_weekly = pd.DataFrame(index=pd_reservoir.index,
                                    columns=['NO1', 'NO2', 'NO3', 'NO4', 'NO5', 'FI', 'SE', 'SE1', 'SE2', 'SE3', 'SE4'],
                                    dtype=float)
        for area in ['NO1', 'NO2', 'NO3', 'NO4', 'NO5', 'FI', 'SE1', 'SE2', 'SE3', 'SE4']:
            for idx in range(hydro_weekly.index.__len__() - 1):
                # get production for current week
                sdate = week_to_date(hydro_weekly.index[idx])
                edate = week_to_date(hydro_weekly.index[idx + 1])
                drange = hydro_prod.loc[(hydro_prod.index >= sdate) * (hydro_prod.index < edate), area]
                if drange.__len__() > 0:
                    hydro_weekly.at[hydro_weekly.index[idx], area] = drange.sum()  # note: summing over nan gives 0

        #    # for SE1-SE4 use svk data
        #    for area in ['SE1','SE2','SE3','SE4']:
        #        for idx in range(hydro_weekly.index.__len__()-1):
        #             # get production for current week
        #            sdate = week_to_date(hydro_weekly.index[idx])
        #            edate = week_to_date(hydro_weekly.index[idx+1])
        #            drange = hydro_prod.loc[(hydro_prod.index >= sdate) * (hydro_prod.index < edate),area]
        #            if drange.__len__() > 0:
        #                hydro_weekly.loc[hydro_weekly.index[idx],area] = drange.sum() # note: summing over nan gives 0
        #
        # for SE impute weekly production from monthly data
        for idx in range(hydro_weekly.index.__len__() - 1):
            sdate = week_to_date(hydro_weekly.index[idx])
            edate = week_to_date(hydro_weekly.index[idx + 1])
            if sdate.month == edate.month:  # if all days are within the same month
                # find number of days in month
                if sdate.month == 12:
                    ndays = (datetime.date(sdate.year + 1, 1, 1) - datetime.date(sdate.year, 12, 1)).days
                else:
                    ndays = (datetime.date(sdate.year, sdate.month + 1, 1) - datetime.date(sdate.year, sdate.month,
                                                                                           1)).days
                try:
                    hydro_weekly.at[hydro_weekly.index[idx], 'SE'] = se_hydro.at[sdate.strftime('%Y-%m')][0] * 7 / ndays
                except KeyError as err:  # SE monthly hydro data does not cover whole range for hydro_weekly, accept key errors when accessing se_hydro
                    print(str(err))
            else:  # week overlapping two months
                ndays1 = (datetime.date(edate.year, edate.month, 1) - datetime.date(sdate.year, sdate.month, 1)).days
                if edate.month == 12:
                    ndays2 = (datetime.date(edate.year + 1, 1, 1) - datetime.date(edate.year, 12, 1)).days
                else:
                    ndays2 = (datetime.date(edate.year, edate.month + 1, 1) - datetime.date(edate.year, edate.month,
                                                                                            1)).days
                # find number of days in each month
                m1days = 0
                while sdate.month == (sdate + datetime.timedelta(days=m1days + 1)).month:
                    m1days += 1
                try:
                    # calculate production this week by weighting contributions from months
                    hydro_weekly.at[hydro_weekly.index[idx], 'SE'] = \
                        m1days / ndays1 * se_hydro.loc[sdate.strftime('%Y-%m')][0] \
                        + (7 - m1days) / ndays2 * se_hydro.loc[edate.strftime('%Y-%m')][0]
                except KeyError as err:  # accept key errors when accessing se_hydro
                    print(str(err))
        # convert to GWh
        for area in ['NO1', 'NO2', 'NO3', 'NO4', 'NO5', 'FI', 'SE1', 'SE2', 'SE3', 'SE4']:
            hydro_weekly.loc[:, area] = hydro_weekly.loc[:, area] / 1000

        hydro_weekly.index = [week_to_date(w) for w in hydro_weekly.index]
        plt.plot(hydro_weekly)
        plt.legend(hydro_weekly.columns)
        plt.grid()
        plt.ylabel('GWh')
        plt.title('Weekly Hydro Production')

        #        plt.figure()
        #        plt.plot(hydro_weekly['SE'])
        #        plt.plot(hydro_weekly.loc[:,['SE1','SE2','SE3','SE4']].sum(axis=1))

        fig = plt.gcf()
        cm_per_inch = 2.5
        fig.set_size_inches(20 / cm_per_inch, 10 / cm_per_inch)

        plt.savefig('Figures/hydro_weekly.png')
        plt.savefig('Figures/hydro_weekly.pdf')
        plt.savefig('Figures/hydro_weekly.eps')
        plt.show()

        hydro_weekly.index = [date_to_week(w) for w in hydro_weekly.index]

        # Calculate inflow (in GWh) as
        # V(t)=P(t)+M(t)-M(t-1)
        # P - hydro_weekly, M - pd_reservoir
        # Note: P(t) is the production during a week, M(t) from Nordpool data is the
        # reservoir content at the END of a week
        Vweekly = pd.DataFrame(columns=hydro_weekly.columns, index=hydro_weekly.index, dtype=float)
        for area in ['NO1', 'NO2', 'NO3', 'NO4', 'NO5', 'FI', 'SE', 'SE1', 'SE2', 'SE3', 'SE4']:
            for idx in range(1, hydro_weekly.__len__()):
                Vweekly.at[Vweekly.index[idx], area] = \
                    hydro_weekly.at[Vweekly.index[idx], area] \
                    + pd_reservoir.at[Vweekly.index[idx], area] \
                    - pd_reservoir.at[Vweekly.index[idx - 1], area]

        # remove few negative values
        Vweekly[Vweekly < 0] = 0

        # Load SE weekly inflow data - from Energiföretagen
        wb = openpyxl.load_workbook('Data/Inflow_SE_2000-2018.xlsx')
        ws = wb['Inflow - Sweden']

        years = range(2000, 2019)
        cidxs = range(3, 22)

        index = []
        for year in years:
            index = index + ['{0}:{1}'.format(year, week) for week in weekdd]

        # inflow_SE = pd.DataFrame(dtype=float,index=range(1,53),columns=years)
        inflow_SE = pd.DataFrame(dtype=float, index=index, columns=['SE'])
        hrow = 8  # row with headers
        for cidx in cidxs:
            year = ws.cell(hrow, cidx).value
            for ridx in range(hrow + 1, hrow + 54):
                week = ws.cell(ridx, 2).value
                val = ws.cell(ridx, cidx).value
                if not val is None:
                    if week < 53:
                        # inflow_SE.loc[week,year] = val
                        inflow_SE.at['{0}:{1}'.format(year, weekdd[week - 1]), 'SE'] = val
                    else:  # add week 53 to week 52
                        # inflow_SE.loc[52,year] = inflow_SE.loc[52,year] + val
                        inflow_SE.at['{0}:52'.format(year), 'SE'] = inflow_SE.at['{0}:52'.format(year), 'SE'] + val

        # Compare calculated inflow with measured values

        # Convert scales to datetime
        inflow_SE.index = [week_to_date(w) for w in inflow_SE.index]
        Vweekly.index = [week_to_date(w) for w in Vweekly.index]

        # find dates for which there is data
        xdates = Vweekly.index[pd.isna(Vweekly.loc[:, 'SE1']) == False]
        # compure rmse
        rmse1 = np.sqrt(np.mean(np.square(inflow_SE.loc[xdates, 'SE'] - Vweekly.loc[xdates, 'SE'])))
        rmse2 = np.sqrt(np.mean(
            np.square(inflow_SE.loc[xdates, 'SE'] - Vweekly.loc[xdates, ['SE1', 'SE2', 'SE3', 'SE4']].sum(axis=1))))
        print('Monthly rmse: {0:.4}'.format(rmse1))
        print('Hourly rmse: {0:.4}'.format(rmse2))
        rmse = np.sqrt(np.mean(np.square(inflow_SE.loc[Vweekly.index, 'SE'] - Vweekly.loc[:, 'SE'])))
        print('Inflow rmse: {}'.format(rmse))

        # plt.plot(Vweekly.loc[xdates,'SE'])
        plt.plot(Vweekly.loc[xdates, ['SE1', 'SE2', 'SE3', 'SE4']].sum(axis=1))
        plt.plot(inflow_SE.loc[xdates, 'SE'])
        plt.legend(['Reconstructed', 'Data from Energiföretagen'])
        plt.grid()
        plt.ylabel('GWh')
        plt.title('SE reservoir inflow, RMSE = {0:.4}'.format(rmse2))
        fig = plt.gcf()
        cm_per_inch = 2.5
        fig.set_size_inches(20 / cm_per_inch, 10 / cm_per_inch)

        plt.savefig('Figures/reservoir_inflow_se_hourly.png')
        plt.savefig('Figures/reservoir_inflow_se_hourly.eps')
        plt.savefig('Figures/reservoir_inflow_se_hourly.pdf')
        plt.show()

        ## PUT INFLOW DATA INTO DATABASE

        # make sqlite database
        conn = sqlite3.connect(self.db)
        c = conn.cursor()

        c.execute('DROP TABLE IF EXISTS inflow')
        c.execute('CREATE TABLE inflow (' + \
                  'time TEXT NOT NULL,' + \
                  'area TEXT NOT NULL,' + \
                  'GWh REAL' + \
                  ')')

        # convert index back to weeks
        Vweekly.index = [date_to_week(date) for date in Vweekly.index]

        for area in Vweekly.columns:
            for week in Vweekly.index:
                val = Vweekly.at[week, area]
                if not pd.isna(val):
                    cmd = "INSERT INTO inflow (time,area,GWh) values ('{0}','{1}',{2})".format(week, area, val)
                    c.execute(cmd)

        conn.commit()
        conn.close()

        # !!! NO COMPENSATION FOR UTC/CET -> wrong data !!!

    def update_wind_data(self):  # Note: Bad style, merge data sets in model instead,

        conn = sqlite3.connect(self.db)
        c = conn.cursor()
        # Nordpool data is missing wind production for FI, NO, take entso-e data instead
        import entsoe_transparency_db as entsoe
        entsoe_db = entsoe.Database()

        data = entsoe_db.select_gen_per_type_data(areas=['FI', 'NO1', 'NO2', 'NO3', 'NO4', 'NO5'],
                                                  types=['Wind onsh', 'Wind offsh'])
        # sum onshore and offshore
        for area in data:
            data[area].loc[:, 'Wind Total'] = data[area].sum(axis=1)

        print('Reading wind production data from ENTSO-E transparency database')
        # put data into nordpool database
        for area in data:
            for idx in data[area].index:
                cmd = "INSERT INTO wind (time,area,MWh) values ('{0}','{1}',{2})".format(idx.strftime('%Y%m%d:%H'),
                                                                                         area, data[area].at[
                                                                                             idx, 'Wind Total'])
                c.execute(cmd)

        conn.commit()
        conn.close()

    def query_categories(self, table):
        """ Query database to find values for 'area' and 'transfer'
        """

        if table not in self.tables:
            print('Could not find table ''{0}'' in database'.format(table))
            return None
        else:

            conn = sqlite3.connect(self.db)
            c = conn.cursor()

            if table == 'exchange':
                category = 'transfer'
            else:
                category = 'area'

            cmd = 'SELECT DISTINCT {1} FROM {0}'.format(table, category)
            c.execute(cmd)

            values = []
            for res in c:
                values.append(res[0])
            return values

    def select_data(self, table, categories=[], starttime='', endtime='', excelfile=''):
        """ Select time series from sqlite database with nordpool data. Data
        is returned as a pandas dataframe, and optionally exported to excel file.

        Input:
            areas - list of areas to choose, or in the case of table 'exchange'
                    the list of transfers, by default all categories are selected
            start - string with starting date in format "YYYYMMDD:HH"
            end - string with ending date in format "YYYYMMDD:HH"
            excelfile - path to excel file where data should be stored

        Output:
            pd_data - pandas dataframe with one column for each time series
        """

        if not type(starttime) is str:
            starttime = str(starttime)
        if not type(endtime) is str:
            endtime = str(endtime)

        conn = sqlite3.connect(self.db)
        c = conn.cursor()

        if table == 'exchange':
            cmd = "SELECT MWh,time,transfer FROM exchange"
        elif table == 'consumption':
            cmd = "SELECT MWh,time,area FROM consumption"
        elif table == 'wind':
            cmd = "SELECT DISTINCT MWh,time,area FROM wind"
        elif table == 'spotprice':
            cmd = "SELECT EUR,time,area FROM spotprice"
        elif table == 'reservoir':
            cmd = "SELECT DISTINCT GWh,time,area FROM reservoir"
        elif table == 'inflow':
            cmd = "SELECT GWh,time,area FROM inflow"
        elif table == 'production':
            cmd = "SELECT MWh,time,area FROM production"
        else:
            print('Table ''{0}'' does not exist'.format(table))
            return None

        cmd_max = "SELECT max(time) FROM {0}".format(table)
        cmd_min = "SELECT min(time) FROM {0}".format(table)

        # check if some categories don't exist in table:
        for cat in categories:
            if cat not in self.cat[table]:
                print('Category ''{0}'' does not exist in table ''{1}'''.format(cat, table))

        if categories != []:
            str_categories = '('
            for idx, cat in enumerate(categories):
                if idx > 0:
                    str_categories += ",'{0}'".format(cat)
                else:
                    str_categories += "'{0}'".format(cat)
            str_categories += ')'

        conditions = []
        if categories != []:
            if table == 'exchange':
                cat_cnd = 'transfer in ' + str_categories
            else:
                cat_cnd = 'area in ' + str_categories
            conditions.append('category')
        if starttime != '':
            start_cnd = 'time >= ' + "'{0}'".format(starttime)
            conditions.append('start')
        if endtime != '':
            end_cnd = 'time <= ' + "'{0}'".format(endtime)
            conditions.append('end')

        n = conditions.__len__()
        if n > 0:
            cmd += ' WHERE '
            cmd_max += ' WHERE '
            cmd_min += ' WHERE '
            for idx, cnd in enumerate(conditions):
                if idx > 0:
                    cmd += ' AND '
                    cmd_max += ' AND '
                    cmd_min += ' AND '
                if cnd == 'category':
                    cmd += cat_cnd
                    cmd_max += cat_cnd
                    cmd_min += cat_cnd
                elif cnd == 'start':
                    cmd += start_cnd
                    cmd_max += start_cnd
                    cmd_min += start_cnd
                elif cnd == 'end':
                    cmd += end_cnd
                    cmd_max += end_cnd
                    cmd_min += end_cnd
                else:
                    print('Unknown condition type: {0}'.format(c))

        c.execute(cmd_min)
        for row in c:
            start = row[0]
        c.execute(cmd_max)
        for row in c:
            end = row[0]

        if start is None:  # if query gave no results then return None
            print("Queries for time range gave no result, returning None")
            return None

        if table == 'reservoir' or table == 'inflow':  # different time format
            # sdate = datetime.datetime(int(start[0:4]),1,1) + datetime.timedelta(days=7*(int(start[5:7])-1))
            # edate = datetime.datetime(int(end[0:4]),1,1) + datetime.timedelta(days=7*(int(end[5:7])-1))
            # dates = pd.date_range(start=sdate,end=edate,freq='7D')
            start_year = int(start[0:4])
            start_week = int(start[5:])
            end_year = int(end[0:4])
            end_week = int(end[5:])
            dd_numbers = ['0{0}'.format(i) for i in range(1, 10)] + [str(i) for i in range(10, 53)]
            if start_year != end_year:
                firstperiod = ['{0}:{1}'.format(start_year, dd_numbers[w]) for w in range(start_week - 1, 52)]
                lastperiod = ['{0}:{1}'.format(end_year, dd_numbers[w]) for w in range(0, end_week)]
                middleperiod = []
                for y in range(start_year + 1, end_year):
                    middleperiod += ['{0}:{1}'.format(y, dd_numbers[w]) for w in range(0, 52)]
                dates = firstperiod + middleperiod + lastperiod
            else:
                dates = ['{0}:{1}'.format(start_year, dd_numbers[w]) for w in range(start_week - 1, end_week)]
        else:
            # create index for data frame
            sdate = datetime.datetime(int(start[0:4]), int(start[4:6]), int(start[6:8]), int(start[9:11]))
            edate = datetime.datetime(int(end[0:4]), int(end[4:6]), int(end[6:8]), int(end[9:11]))
            dates = pd.date_range(start=sdate, end=edate, freq='H')

        # find columns for data frame
        if categories == []:  # all areas selected by default
            categories = self.cat[table]

        # allocate panda data frame for data
        pd_data = pd.DataFrame( \
            dtype=float, \
            index=dates, \
            columns=categories)

        # get data
        # print(cmd)
        c.execute(cmd)
        for row in c:
            if table == 'reservoir' or table == 'inflow':  # different time format
                date = date = row[1]
            else:
                date = datetime.datetime(int(row[1][0:4]), int(row[1][4:6]), int(row[1][6:8]), int(row[1][9:11]))
            pd_data.at[date, row[2]] = row[0]

        conn.close()

        return pd_data


if __name__ == "__main__":
    db = Database()

    #    db.add_uk_prices()

    data = db.select_data(table='spotprice', categories=['SYS', 'UK'], starttime='20180101:00', endtime='20180102:23')

    # db.calculate_inflow_data()

#    # make sqlite database
#    conn = sqlite3.connect(db.db)
#    c = conn.cursor()
#
#
#    data_path = 'C:/Users/elisn/Box Sync/Data/Nordpool/Prices/'
#
#    files = [ \
#             'n2ex-day-ahead-auction-prices_2015_hourly_eur.csv', \
#             'n2ex-day-ahead-auction-prices_2016_hourly_eur.csv', \
#             'n2ex-day-ahead-auction-prices_2017_hourly_eur.csv', \
#             'n2ex-day-ahead-auction-prices_2018_hourly_eur.csv', \
#             'n2ex-day-ahead-auction-prices_2019_hourly_eur.csv', \
#             ]
#
#    for data_file in files:
#        print("Reading {0}".format(data_file))
#        with open(data_path+data_file) as f:
#            csv_reader = csv.reader(f,delimiter=';')
#            ridx = 0
#            for row in csv_reader:
#                if ridx == 0:
#                    pass
#                elif ridx == 1:
#                    pass
#                elif ridx == 2:
#                    pass
#
#                else:
#                    # reformat date
#                    dmy = row[0].split('-')
#                    date = dmy[2] + dmy[1] + dmy[0]
#                    # reformat hour
#                    # Note: first get UK time
#                    hour = row[2][0:2]
#                    uk_time = date + ':' + hour
#                    t = str_to_date(uk_time)
#                    cet_time = (t + datetime.timedelta(seconds=-3600)).strftime('%Y%m%d:%H')
#
#                    area = 'UK'
#                    val = row[3].replace(',','.')
#                    if val == '': # repliace missing data with NULL
#                        val = 'NULL'
#                    # insert into database
#                    cmd = 'INSERT INTO spotprice (time,area,EUR) values("{0}","{1}",{2})'.format(cet_time,area,val)
#                    print(cmd)
#                    c.execute(cmd)
#                ridx += 1
#
#
#    conn.commit()
#    conn.close()

