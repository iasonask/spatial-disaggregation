# -*- coding: utf-8 -*-
""" Database with ENTSO-E transparency data. Data is downloaded
    in xml format from the transparency server using a required
    key. This module also has some functions for processing the
    transparency data.

    Tables:

    cap_per_type(TEXT year, TEXT area, TEXT type, FLOAT cap)
        - installed capacities, note that data for SE regions is missing
s        from entsoe database

    gen_per_type(TEXT time, TEXT area, TEXT type, FLOAT gen)
        - actual generation for different production types from
        2015-2018. 93 MB of data, takes ~6 hours to download

    se_gen_per_type(TEXT time, TEXT area, TEXT type, FLOAT gen)
        - generation per production type for SE, from SvK data
        Has broader categories: Wind, Solar, Nuclear, Hydro, CHP, Gas, Other

    gen_per_unit - actual generation per unit, not implemented

    Note: Entso-e transparency data also has installed capacity
    per type and per unit. This data can be downloaded using
    get_entsoe_gen_data() with datatype=1/2. However, since the amount of
    data is small it is not stored in the sqlite database, as it
    can be downloaded directly when it is needed.

    Note: For nordpool data for hour 00-01 we have used time stamp YYYYMMDD:00
    ENTSO-E data in UTC, Nordpool data in CET=UTC+1:
    Nordpool  -> Entsoe
    20180101:00  20171231:23
    20180101:01  20180101:00



Created on Wed Jan 16 11:31:07 2019

@author: elisn
"""

# from help_functions import *
# from nordic490 import N490

import datetime
import logging
import sqlite3
from pathlib import Path

# import json
from xml.etree import ElementTree

import matplotlib.pyplot as plt
import numpy as np
import pandas as pd
import requests
from prettytable import PrettyTable

from help_functions import intersection, str_to_date

logger = logging.getLogger("Nordic490.entsoe_transparency_db")

# bid zone keys
tbidz_key = {
    "SE1": "10Y1001A1001A44P",
    "SE2": "10Y1001A1001A45N",
    "SE3": "10Y1001A1001A46L",
    "SE4": "10Y1001A1001A47J",
    "NO1": "10YNO-1--------2",
    "NO2": "10YNO-2--------T",
    "NO3": "10YNO-3--------J",
    "NO4": "10YNO-4--------9",
    "NO5": "10Y1001A1001A48H",
    "LV": "10YLV-1001A00074",
    "LT": "10YLT-1001A0008Q",
    "FI": "10YFI-1--------U",
    "EE": "10Y1001A1001A39I",
    "DK1": "10YDK-1--------W",
    "DK2": "10YDK-2--------M",
    "SE": "10YSE-1--------K",
    "NO": "10YNO-0--------C",
    "DK": "10Y1001A1001A796",
    "DE": "10Y1001A1001A83F",
    "PL": "10YPL-AREA-----S",
}

# construt reverse key
tbidz_rkey = {}
for f in tbidz_key:
    tbidz_rkey[tbidz_key[f]] = f

# production type key
tpsr_rkey = {
    "B01": "Biomass",
    "B02": "Fossil Brown coal/Lignite",
    "B03": "Fossil Coal-derived gas",
    "B04": "Fossil Gas",
    "B05": "Fossil Hard coal",
    "B06": "Fossil Oil",
    "B07": "Fossil Oil shale",
    "B08": "Fossil Peat",
    "B09": "Geothermal",
    "B10": "Hydro Pumped Storage",
    "B11": "Hydro Run-of-river and poundage",
    "B12": "Hydro Water Reservoir",
    "B13": "Marine",
    "B14": "Nuclear",
    "B15": "Other renewable",
    "B16": "Solar",
    "B17": "Waste",
    "B18": "Wind Offshore",
    "B19": "Wind Onshore",
    "B20": "Other",
}

# construct reverse reverse key
tpsr_key = {}
for f in tpsr_rkey:
    tpsr_key[tpsr_rkey[f]] = f

# abbreviations for production types
tpsr_rabbrv = {
    "B01": "Biomass",
    "B02": "Brown coal",
    "B03": "Coal-gas",
    "B04": "Gas",
    "B05": "Hard coal",
    "B06": "Oil",
    "B07": "Oil shale",
    "B08": "Peat",
    "B09": "Geothermal",
    "B10": "Hydro pump",
    "B11": "Hydro ror",
    "B12": "Hydro res",
    "B13": "Marine",
    "B14": "Nuclear",
    "B15": "Other renew",
    "B16": "Solar",
    "B17": "Waste",
    "B18": "Wind offsh",
    "B19": "Wind onsh",
    "B20": "Other",
}

# construct reverse reverse key
tpsr_abbrv = {}
for f in tpsr_rabbrv:
    tpsr_abbrv[tpsr_rabbrv[f]] = f

# more broadly defined production types
aggr_types = {
    "Slow": ["B01", "B02", "B05", "B08", "B17", "B15", "B20"],  # include other renewables, other
    "Fast": ["B03", "B04", "B06", "B07"],
    "Hydro": ["B10", "B11", "B12", "B09"],  # include geothermal
    "Nuclear": ["B14"],
    "Wind": ["B18", "B19"],
}
# solar and marine excluded
se_aggr_types = {
    "Slow": ["CHP"],
    "Fast": ["Gas"],
    "Hydro": ["Hydro"],
    "Nuclear": ["Nuclear"],
    "Wind": ["Wind"],
    "Thermal": ["CHP", "Gas"],
}
area_codes = ["SE1", "SE2", "SE3", "SE4", "DK1", "DK2", "EE", "LT", "LV", "FI", "NO1", "NO2", "NO3", "NO4", "NO5"]

area_codes_idx = {}
for idx, c in enumerate(area_codes):
    area_codes_idx[c] = idx

country_codes = ["SE", "DK", "NO", "FI", "EE", "LV", "LT"]

# Production types for SE data
se_types = {
    "Vindkraft": "Wind",  # Wind onshore
    "Vattenkraft": "Hydro",  # Hydro reservoir
    "Ospec": "Other",  # Other
    "Solkraft": "Solar",  # Solar
    "Kärnkraft": "Nuclear",  # Nuclear
    "Värmekraft": "CHP",  # Biomass
    "Gas": "Gas",  # Fossil gas
}


#        prod_types = {
#                'Vindkraft':'B19', # Wind onshore
#                'Vattenkraft':'B12', # Hydro reservoir
#                'Ospec':'B20', # Other
#                'Solkraft':'B16', # Solar
#                'Kärnkraft':'B14', # Nuclear
#                'Värmekraft':'B01', # Biomass
#                'Gas':'B04', # Fossil Gas
#        }


class DatabaseGenUnit:
    """Class for database with generation per unit. Due to the different structure of this database
    it is a separate class."""

    def __init__(self, db="C:/Data/entsoe_transparency_gen_per_unit.db"):
        self.db = db

        if Path(self.db).exists():
            pass

    def download_data(
        self, starttime="20160101", endtime="20160110", countries=["SE", "NO", "DK", "FI", "LT", "LV", "EE"]
    ):
        """Download ENTSO-E data to database. Will not overwrite any data already present
        in the database, either in TABLE units or TABLE CC_YYYYMM, thus this function can be
        called multiple times to extend database. Time for download is approximately 40 min/month if
        all countries are included"""

        # make sqlite database
        conn = sqlite3.connect(self.db)
        c = conn.cursor()

        # check if units table exists, if not create it
        c.execute("SELECT 'units' FROM sqlite_master WHERE type ='table'")
        if c.fetchone() is None:
            # create table with unit info
            c.execute(
                "CREATE TABLE units ("
                + "id TEXT NOT NULL,"
                + "name TEXT NOT NULL,"
                + "country TEXT NOT NULL,"
                + "area TEXT,"
                + "type TEXT NOT NULL,"
                + "resource TEXT NOT NULL"
                + ")"
            )
            conn.commit()

        days = pd.date_range(start=str_to_date(starttime), end=str_to_date(endtime), freq="D")

        # get data
        for day in days:
            if day.day == 1:
                logger.info("------------------------")
                logger.info("Fetching data for {0}".format(day.strftime("%Y%m")))
                logger.info("------------------------")
            for country in countries:
                ## data table ##
                # name of table
                table = country + "_" + day.strftime("%Y%m")

                # check if table exists, if not create table
                c.execute("SELECT name FROM sqlite_master WHERE type ='table' AND name='{0}'".format(table))
                if c.fetchone() is None:
                    # add table
                    c.execute("CREATE TABLE {0} (id TEXT NOT NULL,time TEXT NOT NULL,MWh FLOAT NOT NULL)".format(table))

                ## units table ##
                # get all units in table
                c.execute("SELECT id FROM units")
                unit_list = []
                for unit in c:
                    unit_list.append(unit[0])

                data = get_entsoe_gen_data(
                    datatype=4, area=country, start=day.strftime("%Y%m%d"), end=day.strftime("%Y%m%d")
                )
                if data is not None:
                    for unit in data:
                        ## data table ##
                        for row in unit["Period"].items():
                            # check if data already exists
                            c.execute(
                                "SELECT count(*) FROM {0} WHERE id = '{1}' AND time = '{2}'".format(
                                    table, unit["id"], row[0].strftime("%Y%m%d:%H")
                                )
                            )
                            if c.fetchone()[0] == 0:
                                c.execute(
                                    "INSERT INTO {0}(id,time,MWh) values ('{1}','{2}',{3})".format(
                                        table, unit["id"], row[0].strftime("%Y%m%d:%H"), row[1]
                                    )
                                )

                        ## units table ##
                        if unit["id"] not in unit_list:
                            # add unit to table
                            c.execute(
                                "INSERT INTO units(id,name,country,type,resource) values ('{0}','{1}','{2}','{3}','{4}')".format(
                                    unit["id"],
                                    unit["name"],
                                    country,
                                    unit["production_type"],
                                    unit["registeredResource.mRID"],
                                )
                            )
                    conn.commit()
                else:
                    logger.info("No data for {0} for {1}".format(country, day.strftime("%Y%m%d")))

    def select_data(self, start="20160101", end="20160301", countries=["SE", "NO", "FI", "DK", "EE", "LT", "LV"]):
        starttime = start + ":00"
        endtime = end + ":23"

        # connect to database
        conn = sqlite3.connect(self.db)
        c = conn.cursor()

        # get list of all tables
        c.execute("SELECT name FROM sqlite_master WHERE type ='table'")
        all_tables = [row[0] for row in c.fetchall() if row[0] != "units"]

        # select those tables which are relevant for the present request
        rel_tables = [t for t in all_tables if t[:2] in countries and t[3:] >= starttime[:6] and t[3:] <= endtime[:6]]

        # create panda dataframe
        df = pd.DataFrame(index=pd.date_range(start=str_to_date(starttime), end=str_to_date((endtime)), freq="h"))

        # loop over tables
        for t in rel_tables:
            # get data from table
            c.execute("SELECT id,time,MWh FROM {0} WHERE time >= '{1}' AND time <= '{2}'".format(t, starttime, endtime))
            for point in c:
                df.at[str_to_date(point[1]), point[0]] = point[2]

        # also return generator info for all generators
        df2 = pd.DataFrame(index=df.columns, columns=["name", "type", "country", "resource"])
        c.execute("SELECT id,name,country,type,resource FROM units")
        for point in c:
            if point[0] in df2.index:
                df2.at[point[0], "name"] = point[1]
                df2.at[point[0], "country"] = point[2]
                df2.at[point[0], "type"] = point[3]
                df2.at[point[0], "resource"] = point[4]

        #            df.columns = df2.loc[df.columns,'name']
        #            df.plot()

        return df, df2


class Database:
    def __init__(self, db="Data/entsoe_transparency.db"):
        self.db = db

        if Path(self.db).exists():
            pass

    def download_cap_per_type_data(self, start_year=2015, end_year=2018, areas=[]):
        """
        Download capacities per production type and store in sqlite database,
        in table cap_per_type
        """
        # make sqlite database
        conn = sqlite3.connect(self.db)
        # logger.info(sqlite3.version)
        c = conn.cursor()

        c.execute("DROP TABLE IF EXISTS cap_per_type")
        c.execute(
            "CREATE TABLE cap_per_type ("
            + "year TEXT NOT NULL,"
            + "type TEXT NOT NULL,"
            + "area TEXT NOT NULL,"
            + "cap REAL"
            + ")"
        )

        # download data for each year and price area
        for year in range(start_year, end_year):
            for area in area_codes:
                data = get_entsoe_gen_data(
                    datatype=1, area=area, start="{0}0101".format(year), end="{0}0101".format(year)
                )

                if data is not None:
                    logger.info("Fetched data for {0} for {1}".format(area, year))
                    # collect data
                    for point in data:
                        # acode = point['inBiddingZone_Domain.mRID']
                        gentype = tpsr_rkey[point["MktPSRType"]]
                        cmd = "INSERT INTO cap_per_type (year,type,area,cap) VALUES ('{0}','{1}','{2}',{3})".format(
                            year, gentype, area, point["Period"][0]
                        )
                        c.execute(cmd)
                        # self.areas[tbidz_rkey[acode]].gen_cap[tpsr_rabbrv[gcode]][year] = point['Period'][0]
                        # insert data into database
                else:
                    logger.info("Data collection failed for {0} for {1}".format(area, year))

        conn.commit()
        conn.close()

    def select_cap_per_type_data(self):
        """Select data with generation capacity per type from database

        Output:
            gdata - dict with panda dataframe for each area
        """

        # make sqlite database
        conn = sqlite3.connect(self.db)
        # logger.info(sqlite3.version)
        c = conn.cursor()

        cmd_min = "SELECT min(year) FROM cap_per_type"
        cmd_max = "SELECT max(year) FROM cap_per_type"

        c.execute(cmd_min)
        for row in c:
            startyear = row[0]
        c.execute(cmd_max)
        for row in c:
            endyear = row[0]
        if startyear is None:
            pass

        # create index for data frame
        dates = range(int(startyear), int(endyear) + 1)

        # create columns
        areas = area_codes
        types = list(tpsr_key.keys())

        # allocate panda data frame for each area
        gdata = {}
        for area in areas:
            gdata[area] = pd.DataFrame(dtype=float, index=dates, columns=types)

        # read date into data frame
        cmd = "SELECT year,area,type,cap FROM cap_per_type"
        c.execute(cmd)
        for row in c:
            gdata[row[1]][row[2]][int(row[0])] = row[3]

        conn.close()

        for area in gdata:
            fillna(gdata[area])

        return gdata

    def download_gen_per_type_data(self, start_year=2015, end_year=2018, areas=[]):
        """Download actual generation by production type for all bidding areas.
        The data is saved to the table "gen_per_type" in the given database:

        TABLE gen_per_type(TEXT time,TEXT type,TEXT area,REAL gen)

        time has format 'YYYYMMDD:HH'

        Note that some areas lacks data, such as SE1 which only has data on production
        for onshore wind.
        """

        # make sqlite database
        conn = sqlite3.connect(self.db)
        # logger.info(sqlite3.version)
        c = conn.cursor()

        c.execute("DROP TABLE IF EXISTS gen_per_type")
        c.execute(
            "CREATE TABLE gen_per_type ("
            + "time TEXT NOT NULL,"
            + "type TEXT NOT NULL,"
            + "area TEXT NOT NULL,"
            + "gen REAL"
            + ")"
        )

        if areas == []:
            areas = [
                "SE1",
                "SE2",
                "SE3",
                "SE4",
                "DK1",
                "DK2",
                "EE",
                "LT",
                "LV",
                "FI",
                "NO1",
                "NO2",
                "NO3",
                "NO4",
                "NO5",
            ]

        nfiles = areas.__len__() * (start_year - end_year + 1) * 365

        logger.info("Downloading data from entsoe transparency: ")
        for area in areas:
            # iterate over time
            date = datetime.datetime(start_year, 1, 1)
            counter = 0
            while date.year <= end_year:
                # retrieve data
                sdate = date.strftime("%Y%m%d")
                # logger.info(sdate)
                # get data for one day
                data = get_entsoe_gen_data(datatype=3, area=area, start=sdate, end=sdate, file=None)

                if data is not None:
                    for point in data:
                        gtype = point["production_type"]
                        # area = tbidz_rkey[point['inBiddingZone_Domain.mRID']]
                        for row in point["Period"].items():
                            time = row[0].strftime("%Y%m%d:%H")
                            val = str(row[1])
                            try:  # insert row into table
                                cmd = 'INSERT INTO gen_per_type (time,type,area,gen) values("{0}","{1}","{2}",{3})'.format(
                                    time, gtype, area, val
                                )
                                # logger.info(cmd)
                                c.execute(cmd)
                            except sqlite3.Error as err:
                                logger.info(err)
                                logger.info("Area: " + area + ", type: " + gtype + ", time: " + time)
                else:
                    logger.info("Data collection failed for {0} for {1}".format(area, sdate))
                    # increment
                date = date + datetime.timedelta(days=1)
                if np.remainder(counter, 10) == 0:
                    logger.info("Progress: {0}%".format(str(counter / nfiles * 100)[:4]))
                counter += 1
        conn.commit()
        conn.close()

    def select_gen_per_type_data(self, areas=[], types=[], starttime="", endtime="", excelfile=None):
        """Select time series from sqlite database with transparency data. Data
        is returned as a pandas dataframe, and optionally exported to excel file.

        Input:
            db - path to database file
            areas - list of areas to choose, by default all areas are selected
            types - list of production types, all types by default
            starttime - string with starting date in format "YYYYMMDD:HH"
            endtime - string with ending date in format "YYYYMMDD:HH"

        Output:
            pd_data - pandas dataframe with one column for each time series, the columns
                    are named in the manner "Area:Type", e.g. "FI:Biomass"

        """

        conn = sqlite3.connect(self.db)
        c = conn.cursor()

        if areas != []:
            str_areas = "("
            for idx, area in enumerate(areas):
                if idx > 0:
                    str_areas += ",'{0}'".format(area)
                else:
                    str_areas += "'{0}'".format(area)
            str_areas += ")"

        if types != []:
            str_types = "("
            for idx, gtype in enumerate(types):
                if idx > 0:
                    str_types += ",'{0}'".format(gtype)
                else:
                    str_types += "'{0}'".format(gtype)
            str_types += ")"

        cmd = "SELECT gen,time,type,area FROM gen_per_type"
        # Note: Two additional querys are used to find the starting and ending date
        cmd_max = "SELECT max(time) FROM gen_per_type"
        cmd_min = "SELECT min(time) FROM gen_per_type"

        conditions = []
        if areas != []:
            area_cnd = "area in " + str_areas
            conditions.append("area")
        if types != []:
            type_cnd = "type in " + str_types
            conditions.append("type")
        if starttime != "":
            start_cnd = "time >= '" + starttime + "'"
            conditions.append("start")
        if endtime != "":
            end_cnd = "time <= '" + endtime + "'"
            conditions.append("end")

        n = conditions.__len__()
        if n > 0:
            cmd += " WHERE "
            cmd_max += " WHERE "
            cmd_min += " WHERE "
            for idx, cnd in enumerate(conditions):
                if idx > 0:
                    cmd += " AND "
                    cmd_max += " AND "
                    cmd_min += " AND "
                if cnd == "area":
                    cmd += area_cnd
                    cmd_max += area_cnd
                    cmd_min += area_cnd
                elif cnd == "type":
                    cmd += type_cnd
                    cmd_max += type_cnd
                    cmd_min += type_cnd
                elif cnd == "start":
                    cmd += start_cnd
                    cmd_max += start_cnd
                    cmd_min += start_cnd
                elif cnd == "end":
                    cmd += end_cnd
                    cmd_max += end_cnd
                    cmd_min += end_cnd
                else:
                    logger.info("Unknown condition type: {0}".format(c))

        # logger.info(cmd_min)
        c.execute(cmd_min)
        for row in c:
            start = row[0]
        c.execute(cmd_max)
        for row in c:
            end = row[0]
        if start is None:
            logger.info("The following command returned no data: {0}".format(cmd))
            return None

        # create index for data frame
        sdate = datetime.datetime(int(start[0:4]), int(start[4:6]), int(start[6:8]), int(start[9:11]))
        edate = datetime.datetime(int(end[0:4]), int(end[4:6]), int(end[6:8]), int(end[9:11]))

        dates = pd.date_range(start=sdate, end=edate, freq="h")

        # find columns for data frame
        if areas == []:  # all areas selected by default
            areas = area_codes
        if types == []:
            types = list(tpsr_abbrv.keys())
        # create header for each combination of area and type
        #    cols = []
        #    for area in areas:
        #        for gtype in types:
        #            cols.append(area + ':' + gtype)

        # allocate panda data frame for each area
        gdata = {}
        for area in areas:
            gdata[area] = pd.DataFrame(dtype=float, index=dates, columns=types)

        #    # allocate panda data frame for data
        #    pd_data = pd.DataFrame( \
        #                dtype = float, \
        #                index=dates, \
        #                columns=cols)

        # get data
        c.execute(cmd)  # SELECT gen,time,type,area FROM gen_per_type
        for row in c:
            date = datetime.datetime(int(row[1][0:4]), int(row[1][4:6]), int(row[1][6:8]), int(row[1][9:11]))
            # pd_data[row[3] + ':' + row[2]][date] = row[0]
            gdata[row[3]].loc[date, row[2]] = row[0]

        conn.close()

        # remove all columns which are NaN
        #    isnan = pd_data.isnull().sum()
        #    dropcols = []
        #    for row in isnan.items():
        #        if row[1] == pd_data.__len__():
        #            dropcols.append(row[0])
        #    pd_data = pd_data.drop(columns=dropcols)

        for area in areas:
            isnan = gdata[area].isnull().sum()
            dropcols = []
            for row in isnan.items():
                if row[1] == gdata[area].__len__():
                    dropcols.append(row[0])
            gdata[area] = gdata[area].drop(columns=dropcols)

        if excelfile is not None:
            writer = pd.ExcelWriter(excelfile)
            for area in areas:
                gdata[area].to_excel(writer, sheet_name=area)
            writer.save()
            # pd_data.to_excel(excelfile)

        return gdata

    def get_se_gen_data(self):
        """
        Enter SvK generation data per type into separate table:

            se_gen_per_type(TEXT time, TEXT hype, TEXT area, FLOAT gen)

        Data comes from excel files from SvK homepage

        """

        # get SvK production data

        data_path = "C:/Users/elisn/Box Sync/Data/SvK/"

        import xlrd

        # connect to sqlite database
        conn = sqlite3.connect(self.db)
        c = conn.cursor()

        # create separeate table for SE data
        # create separeate table for SE data
        c.execute("DROP TABLE IF EXISTS se_gen_per_type")
        c.execute(
            "CREATE TABLE se_gen_per_type ("
            + "time TEXT NOT NULL,"
            + "type TEXT NOT NULL,"
            + "area TEXT NOT NULL,"
            + "gen REAL"
            + ")"
        )

        files = [
            "statistik-per-elomrade-och-timme-2018.xls",
            "timvarden-2017-01-12.xls",
            "timvarden-2016-01-12.xls",
            "statistik-per-timme-och-elomrade-2015.xls",
            "timvarden-2014-01-12.xls",
            "timvarden-2013-01-12.xls",
            "timvarden-2012-01-12.xls",
            "timvarden-2011-01-12.xls",
        ]

        # file_name = 'statistik-per-elomrade-och-timme-2018.xls'

        for file_name in files:
            logger.info("Reading {0}".format(file_name))
            wb = xlrd.open_workbook(data_path + file_name)
            ws = wb.sheet_by_index(0)

            headers1 = ws.row_values(0)
            headers2 = ws.row_values(1)
            headers = [x[0] + x[1] for x in zip(headers1, headers2)]

            areas = ws.row_values(2)

            # check which columns to keep
            col_idxs = []
            col_names = []
            for idx, col in enumerate(headers):
                if "Vindkraft" in col:
                    col_idxs.append(idx)
                    col_names.append("Vindkraft")
                elif "Vattenkraft" in col:
                    col_idxs.append(idx)
                    col_names.append("Vattenkraft")
                elif "Ospec" in col:
                    col_idxs.append(idx)
                    col_names.append("Ospec")
                elif "Solkraft" in col:
                    col_idxs.append(idx)
                    col_names.append("Solkraft")
                elif "Kärnkraft" in col:
                    col_idxs.append(idx)
                    col_names.append("Kärnkraft")
                elif "Värmekraft" in col:
                    col_idxs.append(idx)
                    col_names.append("Värmekraft")
                elif "Gast" in col:
                    col_idxs.append(idx)
                    col_names.append("Gas")

            ridx = 0
            for row in ws.get_rows():
                if ridx >= 5:
                    # get current datetime
                    if type(row[0].value) is str:
                        timeinfo = row[0].value.replace(".", " ").split(" ")
                        day = timeinfo[0]
                        if day.__len__() < 2:
                            day = "0" + day
                        month = timeinfo[1]
                        if month.__len__() < 2:
                            month = "0" + month
                        hour = timeinfo[3]
                        hour = hour.split(":")[0]
                        if hour.__len__() < 2:
                            hour = "0" + hour
                        timestr = timeinfo[2] + month + day + ":" + hour
                    elif type(row[0].value) is float:
                        py_date = xlrd.xldate.xldate_as_datetime(row[0].value, wb.datemode)
                        timestr = py_date.strftime("%Y%m%d:%H")
                    for nidx, cidx in enumerate(col_idxs):
                        area = areas[cidx]
                        # gtype = tpsr_rabbrv[prod_types[col_names[nidx]]]
                        gtype = se_types[col_names[nidx]]
                        data = row[cidx].value
                        if type(data) is not float:
                            data = "NULL"
                        cmd = "INSERT INTO se_gen_per_type (time,type,area,gen) VALUES ('{0}','{1}','{2}',{3})".format(
                            timestr, gtype, area, data
                        )
                        # logger.info(cmd)
                        c.execute(cmd)
                ridx += 1

        conn.commit()
        conn.close()

    def select_se_gen_per_type_data(self, areas=[], types=[], starttime="", endtime="", excelfile=None):
        """Select production per type data from SE table

        Input:
            db - path to database file
            areas - list of areas to choose, by default all areas are selected
            types - list of production types, all types by default
            starttime - string with starting date in format "YYYYMMDD:HH"
            endtime - string with ending date in format "YYYYMMDD:HH"

        Output:
            pd_data - pandas dataframe with one column for each time series, the columns
                    are named in the manner "Area:Type", e.g. "FI:Biomass"

        """

        conn = sqlite3.connect(self.db)
        c = conn.cursor()

        if areas != []:
            str_areas = "("
            for idx, area in enumerate(areas):
                if idx > 0:
                    str_areas += ",'{0}'".format(area)
                else:
                    str_areas += "'{0}'".format(area)
            str_areas += ")"

        if types != []:
            str_types = "("
            for idx, gtype in enumerate(types):
                if idx > 0:
                    str_types += ",'{0}'".format(gtype)
                else:
                    str_types += "'{0}'".format(gtype)
            str_types += ")"

        cmd = "SELECT gen,time,type,area FROM se_gen_per_type"
        # Note: Two additional querys are used to find the starting and ending date
        cmd_max = "SELECT max(time) FROM se_gen_per_type"
        cmd_min = "SELECT min(time) FROM se_gen_per_type"

        conditions = []
        if areas != []:
            area_cnd = "area in " + str_areas
            conditions.append("area")
        if types != []:
            type_cnd = "type in " + str_types
            conditions.append("type")
        if starttime != "":
            start_cnd = "time >= '" + starttime + "'"
            conditions.append("start")
        if endtime != "":
            end_cnd = "time <= '" + endtime + "'"
            conditions.append("end")

        n = conditions.__len__()
        if n > 0:
            cmd += " WHERE "
            cmd_max += " WHERE "
            cmd_min += " WHERE "
            for idx, cnd in enumerate(conditions):
                if idx > 0:
                    cmd += " AND "
                    cmd_max += " AND "
                    cmd_min += " AND "
                if cnd == "area":
                    cmd += area_cnd
                    cmd_max += area_cnd
                    cmd_min += area_cnd
                elif cnd == "type":
                    cmd += type_cnd
                    cmd_max += type_cnd
                    cmd_min += type_cnd
                elif cnd == "start":
                    cmd += start_cnd
                    cmd_max += start_cnd
                    cmd_min += start_cnd
                elif cnd == "end":
                    cmd += end_cnd
                    cmd_max += end_cnd
                    cmd_min += end_cnd
                else:
                    logger.info("Unknown condition type: {0}".format(c))

        # logger.info(cmd_min)
        c.execute(cmd_min)
        for row in c:
            start = row[0]
        c.execute(cmd_max)
        for row in c:
            end = row[0]
        if start is None:
            logger.info("The following command returned no data: {0}".format(cmd))
            return None

        # create index for data frame
        sdate = datetime.datetime(int(start[0:4]), int(start[4:6]), int(start[6:8]), int(start[9:11]))
        edate = datetime.datetime(int(end[0:4]), int(end[4:6]), int(end[6:8]), int(end[9:11]))

        dates = pd.date_range(start=sdate, end=edate, freq="h")

        # find columns for data frame
        if areas == []:  # all areas selected by default
            areas = ["SE1", "SE2", "SE3", "SE4"]
        if types == []:
            types = [se_types[f] for f in se_types]

        # allocate panda data frame for each area
        gdata = {}
        for area in areas:
            gdata[area] = pd.DataFrame(dtype=float, index=dates, columns=types)

        # get data
        c.execute(cmd)  # SELECT gen,time,type,area FROM gen_per_type
        for row in c:
            date = datetime.datetime(int(row[1][0:4]), int(row[1][4:6]), int(row[1][6:8]), int(row[1][9:11]))
            gdata[row[3]].loc[date, row[2]] = row[0]

        conn.close()

        for area in areas:
            isnan = gdata[area].isnull().sum()
            dropcols = []
            for row in isnan.items():
                if row[1] == gdata[area].__len__():
                    dropcols.append(row[0])
            gdata[area] = gdata[area].drop(columns=dropcols)

        if excelfile is not None:
            writer = pd.ExcelWriter(excelfile)
            for area in areas:
                gdata[area].to_excel(writer, sheet_name=area)
            writer.save()

        return gdata

    def get_entsoe_production_stats(
        self,
        starttime="20180101:00",
        endtime="20181231:23",
        thermal=["Biomass", "Brown coal", "Coal-gas", "Gas", "Hard coal", "Oil", "Oil shale", "Peat", "Waste"],
        excelfile=None,
    ):
        """Find min, max and mean values for the different production types
        based on entso-e transparency data, for generation according to different
        categories
        Also find maximum ramping rates
        """

        # get entso-e actual production data
        # pd_data = self.select_gen_per_type_data(areas=area_codes,starttime=starttime,endtime=endtime,excelfile=excelfile)

        # aggregate production over broader types
        #        adata = {}
        #        for area in pd_data.keys():
        #            adata[area] = aggregate_gen_per_type_data(pd_data[area])
        #            adata[area]['Total'] = adata[area].sum(axis=1)
        #            adata[area]['Thermal'] = adata[area].loc[:,[f for f in ['Fast','Slow'] if f in adata[area].columns]].sum(axis=1)
        #
        #
        #        # get se production data
        #        se_data = self.select_se_gen_per_type_data(starttime=starttime,endtime=endtime)
        #        for area in se_data.keys():
        #            adata[area] = pd.DataFrame(columns=list(se_aggr_types.keys()),index=se_data[area].index)
        #            for gtype in se_aggr_types.keys():
        #                # check if any column exist in se-data
        #                cols = [c for c in se_data[area].columns if c in se_aggr_types[gtype]]
        #                if cols != []:
        #                    adata[area].loc[:,gtype] = se_data[area].loc[:,cols].sum(axis=1)
        #
        pd_data = self.select_gen_per_type_wrap(starttime=starttime, endtime=endtime, thermal=thermal)
        adata = {}
        for area in pd_data:
            adata[area] = pd_data[area].drop(
                columns=[col for col in pd_data[area].columns if col not in ["Thermal", "Hydro", "Nuclear", "Wind"]]
            )
        # for each time series, determine min, max, and average
        stats = {}
        for area in adata.keys():
            astats = {}
            # first replace zero values with NaN
            adata[area][adata[area] == 0] = np.nan
            for icol, col in enumerate(adata[area].columns):
                cstats = {}
                cstats["min"] = np.min(adata[area][col])
                cstats["max"] = np.max(adata[area][col])
                cstats["avg"] = np.mean(adata[area][col])
                diff = adata[area][col].diff()
                cstats["maxramp"] = np.max(diff)
                cstats["minramp"] = np.min(diff)
                astats[col] = cstats

            stats[area] = astats

        return stats

    def select_gen_per_type_wrap(
        self,
        starttime="20180101:00",
        endtime="20180107:23",
        areas=area_codes,
        thermal=["Biomass", "Brown coal", "Coal-gas", "Gas", "Hard coal", "Oil", "Oil shale", "Peat", "Waste"],
        hydro=["Hydro ror", "Hydro res", "Hydro pump"],
        wind=["Wind offsh", "Wind onsh"],
    ):
        """
        Wrapper for select functions. Selects data from ENTSO-E for non-SE regions, and from
        SvK for SE regions. ENTSO-E data is also time-displaced one hour to fix time lag.
        Also the aggregate production for categories 'Hydro' and 'Thermal' are computed
        according to the given definitions.

        Thermal: ['Biomass',Brown coal','Coal-gas','Gas','Hard coal','Oil','Oil shale','Peat','Waste']
        'B01':'Biomass',
        'B02':'Brown coal',
        'B03':'Coal-gas',
        'B04':'Gas',
        'B05':'Hard coal',
        'B06':'Oil',
        'B07':'Oil shale',
        'B08':'Peat',
        Waste
        """
        # get data
        pd_data = self.select_gen_per_type_data(
            areas=[a for a in areas if "SE" not in a],
            starttime=(str_to_date(starttime) + datetime.timedelta(hours=-1)).strftime("%Y%m%d:%H"),
            endtime=(str_to_date(endtime) + datetime.timedelta(hours=-1)).strftime("%Y%m%d:%H"),
        )
        pd_data_se = self.select_se_gen_per_type_data(starttime=starttime, endtime=endtime)

        # index = pd.date_range(start=str_to_date(starttime),end=str_to_date(endtime),freq='h')

        # correct index for ENTSO-E data: UTC -> UTC + 1
        for area in pd_data:
            new_index = [t + datetime.timedelta(hours=1) for t in pd_data[area].index]
            pd_data[area].index = new_index

        # aggregate hydro and thermal generation (this is now done in Nordic490 script to keep control)
        """
        for area in pd_data:
            pd_data[area].loc[:,'Hydro'] = pd_data[area].loc[:,[h for h in hydro if h in pd_data[area].columns]].sum(axis=1)
            pd_data[area].loc[:,'Thermal'] = pd_data[area].loc[:,[h for h in thermal if h in pd_data[area].columns]].sum(axis=1)
            pd_data[area].loc[:,'Wind'] = pd_data[area].loc[:,[h for h in wind if h in pd_data[area].columns]].sum(axis=1)
        """

        # copy SE data into pd_data
        for area in [a for a in areas if "SE" in a]:
            pd_data[area] = pd.DataFrame(index=pd_data_se[area].index, columns=["Hydro", "Thermal", "Solar"])
            pd_data[area].loc[:, "Hydro"] = pd_data_se[area].loc[:, "Hydro"]
            pd_data[area].loc[:, "Thermal"] = pd_data_se[area].loc[:, "CHP"]

            if "Nuclear" in pd_data_se[area].columns:
                pd_data[area].loc[:, "Nuclear"] = pd_data_se[area].loc[:, "Nuclear"]
            if "Wind" in pd_data_se[area].columns:
                pd_data[area].loc[:, "Wind"] = pd_data_se[area].loc[:, "Wind"]
            if "Solar" in pd_data_se[area].columns:
                pd_data[area].loc[:, "Solar"] = pd_data_se[area].loc[:, "Solar"]
        return pd_data

    def drop_tables(self):
        """Drop all tables"""
        # make sqlite database
        conn = sqlite3.connect(self.db)
        c = conn.cursor()

        # drop all tables
        c.execute("SELECT name FROM sqlite_master WHERE type ='table'")
        for tab in c.fetchall():
            c.execute("DROP TABLE '{0}'".format(tab[0]))
        conn.commit()


def get_entsoe_gen_data(datatype=1, area="SE1", start="20160101", end="20160101", file=None):
    """Get generation data (actual generation or installed capacity) from
    ENTSO-E transparency database.
    Input:
        datatype - 1: capacity per type
                   2: capacity per unit
                   3: actual generation per type
                   4: actual generation per unit
        area - the price area for which to obtain data
        start - start date
        end - end date
        file - name of xml file to write
    Output:
        data - list containing the returned time series
    Notes:
    1. For type 4, it is only possible to extract one day of data at a time.
    Only data corresponding to the start date will be returned. For the other
    types maximum 1 year of data can be obtained at once.
    2. For type 1 and 2 the data has yearly frequency. For type 3 and 4 the
    data has hourly frequency.
    """

    req_par = {}
    req_url = "https://transparency.entsoe.eu/api?"
    req_token = "a954a1fe-a63e-4c55-84a8-425c35484edb"

    if datatype == 1:  # Installed capacity per type
        req_par["documentType"] = "A68"
        req_par["processType"] = "A33"
    elif datatype == 2:  # Installed capacity per unit
        req_par["documentType"] = "A71"
        req_par["processType"] = "A33"
    elif datatype == 3:  # Actual generation per type
        req_par["documentType"] = "A75"
        req_par["processType"] = "A16"
    elif datatype == 4:  # Actual generation per unit
        req_par["documentType"] = "A73"
        req_par["processType"] = "A16"
    else:
        logger.info("Wrong data type " "%i" "".format())
        return None

    req_par["In_Domain"] = tbidz_key[area]

    sdate = datetime.datetime(int(start[0:4]), int(start[4:6]), int(start[6:8]))
    edate = datetime.datetime(int(end[0:4]), int(end[4:6]), int(end[6:8]))

    req_par["periodStart"] = start + "0000"
    if datatype == 4:
        # can only obtain one day of data
        # req_par['periodEnd'] = start+'2300'
        edate = sdate + datetime.timedelta(days=1)
    else:
        # req_par['periodEnd'] = end+'2300'
        edate += datetime.timedelta(days=1)
    req_par["periodEnd"] = edate.strftime("%Y%m%d") + "0000"

    # logger.info(edate.strftime("%Y%m%d"))
    send_par = []
    for f in req_par.keys():
        if not req_par[f] == "":
            send_par.append(f)

    query = req_url + "securityToken=" + req_token + "&"
    for i, f in enumerate(send_par):
        query = query + f + "=" + req_par[f]
        if i < send_par.__len__() - 1:
            query = query + "&"

    r = requests.get(query)
    root = ElementTree.fromstring(r.content)

    # extract prefix
    idx = root.tag.find("}")
    doctype = root.tag[0 : idx + 1]

    if r.status_code == requests.codes.ok:
        # query was ok

        time_series = root.findall(doctype + "TimeSeries")
        data = []
        for t in time_series:
            ts = {}
            # read fields
            for e in t:
                field = e.tag[idx + 1 :]
                if field == "Period":
                    # process data
                    start = e.findall(doctype + "timeInterval/" + doctype + "start")[0].text
                    end = e.findall(doctype + "timeInterval/" + doctype + "end")[0].text
                    resolution = e.findall(doctype + "resolution")[0].text
                    resolution_key = {"PT60M": "h", "P1Y": "Y"}

                    # create panda time series
                    edate = datetime.datetime(int(end[0:4]), int(end[5:7]), int(end[8:10]))
                    # if yearly resolution, set start date one year before end date
                    if resolution_key[resolution] == "Y":
                        sdate = edate + datetime.timedelta(days=-365)
                    else:  # hourly resolution, take hours into account
                        edate = datetime.datetime(int(end[0:4]), int(end[5:7]), int(end[8:10]), int(end[11:13]))
                        sdate = datetime.datetime(int(start[0:4]), int(start[5:7]), int(start[8:10]), int(start[11:13]))

                    alen = e.__len__() - 2
                    dates = pd.date_range(start=sdate, end=edate, freq=resolution_key[resolution])
                    dates = dates[0:alen]  # remove last time, as we only want starting time for each period

                    ts["Period"] = pd.Series(np.zeros(alen, dtype=float), index=dates)
                    for i, point in enumerate(e[2:]):
                        ts["Period"][i] = float(point[1].text)
                elif field == "MktPSRType":
                    ts[field] = e[0].text
                    system_resource = e.findall(doctype + "PowerSystemResources")
                    if not system_resource == []:
                        ts["id"] = system_resource[0][0].text
                        ts["name"] = system_resource[0][1].text

                else:
                    ts[field] = e.text
            data.append(ts)

            for d in data:
                if "MktPSRType" in d:
                    d["production_type"] = tpsr_rabbrv[d["MktPSRType"]]
    else:
        errormsg = root.findall(doctype + "Reason/" + doctype + "text")
        if not errormsg == []:
            logger.info("Invalid query: " + errormsg[0].text)
        else:
            logger.info("Could not find <Reason> in xml document")
        return None

    if file is not None:
        tree = ElementTree.ElementTree(root)
        tree.write(file)

    return data


def aggregate_gen_per_type_data(pd_data):
    """Aggregate production data according to more broader categories given
    in 'aggr_types'.
    Input:
        pd_data - original data frame with entsoe transparency production types
    Output:
        pd_aggrdata - new data frame with production aggregated by categories
        given in 'aggr_types'
    """

    pd_aggrdata = pd.DataFrame(columns=aggr_types, index=pd_data.index, dtype=float)
    for gentype in aggr_types:
        cols = list(intersection(list(pd_data.columns), [tpsr_rabbrv[f] for f in aggr_types[gentype]]))
        if cols != []:
            pd_aggrdata[gentype] = pd_data[cols].sum(axis=1)
        else:
            pd_aggrdata[gentype] = np.zeros(pd_aggrdata.__len__(), dtype=float)

    return pd_aggrdata


def plot_generation(data, area="", savefigs=False):
    """Plot the generation in data. Data may be dictionary with one dataframe
        per area, or it may be a single dataframe, in which case the name
        of the area must be specified
    Input:
        data - dictionary with one DataFrame for each price area
    """

    import seaborn

    if type(data) == dict:
        # figidx = 1
        for area in data.keys():
            number_of_plots = data[area].columns.__len__()

            colors = seaborn.color_palette("hls", number_of_plots)  # Set2, hls
            # colors = seaborn.husl_palette(number_of_plots)
            # colors = seaborn.mpl_palette("Set2",number_of_plots)

            plt.figure()

            lines = plt.plot(data[area])
            for i, l in enumerate(lines):
                l.set_color(colors[i])

            plt.legend(data[area].columns)
            plt.title(area)
            plt.ylabel("MW")

            if savefigs:
                plt.savefig("Figures/{0}.png".format(area))

            # figidx += 1
    else:
        number_of_plots = data.columns.__len__()

        colors = seaborn.color_palette("hls", number_of_plots)  # Set2, hls
        # colors = seaborn.husl_palette(number_of_plots)
        # colors = seaborn.mpl_palette("Set2",number_of_plots)

        plt.figure()

        lines = plt.plot(data)
        for i, l in enumerate(lines):
            l.set_color(colors[i])

        plt.legend(data.columns)
        plt.title(area)
        plt.ylabel("MW")

        if savefigs:
            plt.savefig("Figures/{0}.png".format(area))


def print_installed_capacity(data, areas=[], file="Data/capacities.txt"):
    """Print pretty tables with capacity per area and type
    Input:
        data - dictionary with DataFrame for each area
    """
    if file is not None:
        f = open(file, "w")

    if areas == []:
        areas = area_codes

    for a in areas:
        # find nan and zero columns
        nnan = data[a].isnull().sum()
        nzero = (data[a] == 0).sum()
        nrows = data[a].__len__()
        cols = []
        for c in data[a].columns:
            if nnan[c] + nzero[c] < nrows:
                cols.append(c)

        # short column names
        # scols = [tpsr_rabbrv[tpsr_abbrv[c]] for c in cols]

        t = PrettyTable()
        t.field_names = ["year"] + list(cols)
        for row in data[a].iterrows():
            t.add_row([row[0]] + [row[1][f] for f in cols])

        logger.info("Area: {0}".format(a))
        logger.info(t)
        if file is not None:
            f.write("Area: {0}\n".format(a))
            f.write(t.get_string())
            f.write("\n")
    if file is not None:
        f.close()


def fillna(pdframe):
    """Replace NaN values with zero for those years in which there is at least
    one column which has non-missing data
    """
    for row in pdframe.iterrows():
        if row[1].isnull().sum() < row[1].__len__():
            # impute zero values
            pdframe.loc[row[0]] = pdframe.loc[row[0]].fillna(0)


def compare_nuclear_generation():
    """Compare nuclear generation of individual units with aggregate values"""

    db = DatabaseGenUnit()

    starttime = "20180101:00"
    endtime = "20181231:00"

    data, plants = db.select_data(start=starttime, end=endtime, countries=["SE", "FI"])

    pnames = ["Ringhals block 1 G11", "Ringhals block 1 G12", "Ringhals block 2 G21", "Ringhals block 2 G22"]

    # select nuclear
    se_nuclear = [
        idx for idx in plants.index if plants.at[idx, "type"] == "Nuclear" and plants.at[idx, "country"] == "SE"
    ]
    fi_nuclear = [
        idx for idx in plants.index if plants.at[idx, "type"] == "Nuclear" and plants.at[idx, "country"] == "FI"
    ]

    nuclear_list = [idx for idx in plants.index if plants.at[idx, "type"] == "Nuclear"]

    data_nuclear = data.loc[:, nuclear_list].copy(deep=True)
    data_nuclear.columns = [plants.at[code, "name"] for code in data_nuclear.columns]

    data_decom = data_nuclear.loc[:, pnames].copy(deep=True)
    data_decom["tot"] = data_decom.sum(axis=1)
    data_decom.plot()

    # get aggregate nuclear production
    db2 = Database()
    data2 = db2.select_gen_per_type_wrap(starttime=starttime, endtime=endtime, areas=["SE3", "FI"])

    # compare aggregate and individual nuclear production
    plt.figure()
    ax = data2["SE3"].loc[:, "Nuclear"].plot()
    data.loc[:, se_nuclear].sum(axis=1).plot(ax=ax)
    plt.show()
    plt.close()

    plt.figure()
    ax = data2["FI"].loc[:, "Nuclear"].plot()
    data.loc[:, fi_nuclear].sum(axis=1).plot(ax=ax)
    plt.show()
    plt.close()


if __name__ == "__main__":
    starttime = "20180101:00"
    endtime = "20180107:23"

    db = Database()

    import nordpool_db as nordpool

    db_np = nordpool.Database()
    df_np = db_np.select_data(table="production", starttime=starttime, endtime=endtime)

    data1 = db.select_gen_per_type_wrap(starttime=starttime, endtime=endtime)
    data2 = db.select_gen_per_type_data(starttime=starttime, endtime=endtime)

    area = "SE2"

    # compare wind generation for SE

    # ax = data2[area]['Wind onsh'].plot()
    ax = data2[area].sum(axis=1).plot()
    ax = data1[area].loc[:, ["Hydro", "Thermal", "Wind"]].sum(axis=1).plot()
    df_np.loc[:, area].plot(ax=ax)

    plt.legend(["ENTSO-E", "Nordpool"])
    plt.show()

    """
    Process Type:
        A01 - Day ahead
        A02 - Intra day incremental
        A16 - Realised 
        A18 - Intraday total 
        A31 - Week ahead 
        A32 - Month ahead 
        A33 - Year ahead 
        A39 - Synchronization process
        A40 - Intraday process

    Document Type:
        A09 - Finalised schedule
        A11 - Aggregated energy data report
        A25 - Allocation result document
        A26 - Capacity document
        A31 - Agreed capacity
        A44 - Price Document
        A61 - Estimated Net Transfer Capacity
        A63 - Redispatch notice
        A65 - System total load
        A68 - Installed generation per type
        A69 - Wind and solar forecast
        A70 - Load forecast margin
        A71 - Generation forecast
        A72 - Reservoir filling information
        A73 - Actual generation
        A74 - Wind and solar generation
        A75 - Actual generation per type
        A76 - Load unavailability
        A77 - Production unavailability
        A78 - Transmission unavailability
        A79 - Offshore grid infrastructure unavailability
        A80 - Generation unavailability
        A81 - Contracted reserves
        A82 - Accepted offers
        A83 - Activated balancing quantities
        A84 - Activated balancing prices
        A85 - Imbalance prices
        A86 - Imbalance volume
        A87 - Financial situation
        A88 - Cross border balancing
        A89 - Contracted reserve prices
        A90 - Interconnection network expansion
        A91 - Counter trade notice
        A92 - Congestion costs
        A93 - DC link capacity
        A94 - Non EU allocations
        A95 - Configuration document
        B11 - Flow-based allocations

    Business Type:
        A29 - Already allocated capacity (AAC)
        A43 - Requested capacity (without price)
        A46 - System Operator redispatching
        A53 - Planned maintenance
        A54 - Unplanned outage
        A85 - Internal redispatch
        A95 - Frequency containment reserve
        A96 - Automatic frequency restoration reserve
        A97 - Manual frequency restoration reserve
        A98 - Replacement reserve
        B01 - Interconnector network evolution
        B02 - Interconnector network dismantling
        B03 - Counter trade
        B04 - Congestion costs
        B05 - Capacity allocated (including price)
        B07 - Auction revenue
        B08 - Total nominated capacity
        B09 - Net position
        B10 - Congestion income
        B11 - Production unit

    Psr Type:
        A03 - Mixed
        A04 - Generation
        A05 - Load
        B01 - Biomass
        B02 - Fossil Brown coal/Lignite
        B03 - Fossil Coal-derived gas
        B04 - Fossil Gas
        B05 - Fossil Hard coal
        B06 - Fossil Oil
        B07 - Fossil Oil shale
        B08 - Fossil Peat
        B09 - Geothermal
        B10 - Hydro Pumped Storage
        B11 - Hydro Run-of-river and poundage
        B12 - Hydro Water Reservoir
        B13 - Marine
        B14 - Nuclear
        B15 - Other renewable
        B16 - Solar
        B17 - Waste
        B18 - Wind Offshore
        B19 - Wind Onshore
        B20 - Other
        B21 - AC Link
        B22 - DC Link
        B23 - Substation
        B24 - Transformer

    Areas:
        10YSE-1--------K - Sweden
        10Y1001A1001A44P - SE1
        10Y1001A1001A45N - SE2
        10Y1001A1001A46L - SE3
        10Y1001A1001A47J - SE4
        10YPL-AREA-----S - Poland
        10YNO-0--------C - Norway
        10YNO-1--------2 - NO1
        10YNO-2--------T - NO2
        10YNO-3--------J - NO3
        10YNO-4--------9 - NO4
        10Y1001A1001A48H - NO5
        10YLV-1001A00074 - Latvia LV
        10YLT-1001A0008Q - Lithuania LT
        10Y1001A1001A83F - Germany
        10YFI-1--------U - Finland FI
        10Y1001A1001A39I - Estonia EE
        10Y1001A1001A796 - Denmark
        10YDK-1--------W - DK1
        10YDK-2--------M - DK2

    """
