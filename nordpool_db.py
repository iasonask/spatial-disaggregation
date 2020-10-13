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
    data = db.select_data(table='spotprice', categories=['SYS', 'UK'], starttime='20180101:00', endtime='20180102:23')
    print(data)