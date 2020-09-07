# -*- coding: utf-8 -*-
"""
Created on Aug 2020

@author: Aravind
"""

import os
import pandas as pd
import numpy as np
import matplotlib.pyplot as plt
import pickle
from geopy.geocoders import Nominatim
from help_functions import c_trans


def modify_network():
    """ Function to modify load distribution at buses """

    bus = pd.read_excel("Data/N490.xlsx", index_col=0)  # Read the bus topology file

    "Resetting the assigned loads "
    bus.loc[:, 'load_share'] = 0

    # Improve load distribution in Sweden
    "Read the load file for Sweden"
    Se = pd.read_excel("Data/Loads/Sweden.xlsx", index_col=0)  # Direct to the path where load data is stored
    # Update load_share with new data
    for i, row1 in bus.iterrows():
        for j, row2 in Se.iterrows():
            if row1['country'] == 'SE':
                if row2['bus'] == i:
                    bus.loc[i, 'load_share'] += row2['Load']

    # Load distribution for Norway
    "Read the load file for Norway"
    No = pd.read_excel("Data/Loads/Norway.xlsx", index_col=0)  # Direct to the path where load data is stored
    # Update load_share with new data
    for i, row1 in bus.iterrows():
        for j, row2 in No.iterrows():
            if row1['country'] == 'NO':
                if row2['bus'] == i:
                    bus.loc[i, 'load_share'] += row2['Load']

    # Load distribution for Finland
    "Read the load file for Finland"
    Fi = pd.read_excel("Data/Loads/Finland.xlsx", index_col=0)  # Direct to the path where load data is stored
    # Update load_share with new data
    for i, row1 in bus.iterrows():
        for j, row2 in Fi.iterrows():
            if row1['country'] == 'FI':
                if row2['bus'] == i:
                    bus.loc[i, 'load_share'] += row2['Load']

    # Load distribution for Denmark
    "Read the load file for Denmark"
    Dk = pd.read_excel("Data/Loads/Denmark.xlsx", index_col=0)  # Direct to the path where load data is stored
    # Update load_share with new data
    for i, row1 in bus.iterrows():
        for j, row2 in Dk.iterrows():
            if row1['country'] == 'DK':
                if row2['bus'] == i:
                    bus.loc[i, 'load_share'] += row2['Load']

    bus['load_share'].fillna(0, inplace=True)  # Remove NaN values

    # update load_shares
    bidz = ['SE1', 'SE2', 'SE3', 'SE4', 'NO1', 'NO2', 'NO3', 'NO4', 'NO5', 'FI', 'DK2']  # Price areas in the Nordics
    for b in bidz:
        sum_share = bus.loc[bus.bidz == b, 'load_share'].sum()
        bus.loc[bus.bidz == b, 'load_share'] *= 1 / sum_share
    bus['load_share'].fillna(0, inplace=True)  # Remove NaN values

    return bus


def regression():
    """ Function for filling missing values in energy consumption data using linear regression. The function returns an
        array of the estimated values """
    """ Create an excel file using the downloaded statistics with years as rows and municipality as column """
    df = pd.read_excel("consumption.xlsx")
    res = []
    for i in range(len(df)):
        y1 = df.iloc[i]
        y = y1.dropna()
        x = y.index.values
        coef = np.polyfit(x, y, 1)
        poly1d_fn = np.poly1d(coef)
        res.append(poly1d_fn(2018))  # Use the year for which you are building the model here

    return res


def coordinates():
    """ Function to obtain the latitude and longitude of the municipalities -> returns a dataframe
        Better to add the municipality name + country name to avoid discrepancies in obtained geodetic data"""

    geolocator = Nominatim(user_agent="specify_your_app_name_here", timeout=300) # Ensure a big timeout for larger files
    # Add the geolocation service that you are using
    df = pd.read_excel("Municipalities.xlsx", index_col=0) # Read the file with municipality names
    for i, row in df.iterrows():
        location = geolocator.geocode(df.at[i, 'Muncipality'])
        df.loc[i, 'lat'] = location.latitude
        df.loc[i, 'lon'] = location.longitude

    """ To obtain the Cartesian coordinates in SWEREF99TM standard """
    res = c_trans(df['lat'], df['lon'])
    df['x'] = res[1]
    df['y'] = res[0]

    return df

