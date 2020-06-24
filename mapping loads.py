"""
Created on April 2020

@author: Aravind
"""

import os
import numpy as np
import pandas as pd
import pickle
import matplotlib.path as mpltPath
from numpy import atleast_1d as arr
from scipy.spatial.distance import cdist
import matplotlib.pyplot as plt


def mult_ind(a, b, miss=np.nan):
    """ Get indices for elements of a in b, returns numpy array.
    E.g. mult_ind([1,2,1,4],[3,2,1]) -> array([2.,1.,2.,nan]) """
    bind = {}
    for i, elt in enumerate(b):
        if elt not in bind:
            bind[elt] = i
    return arr([bind.get(itm, miss) for itm in a])


bidz_map = os.path.join('Data', 'raw', 'map_with_bidz2018.npz')
temp = np.load(bidz_map, allow_pickle=True)
x_map = temp['x']
y_map = temp['y']

xn1 = x_map[17]
yn1 = y_map[17]
polygonNO1 = np.vstack((xn1, yn1)).T
pathN1 = mpltPath.Path(polygonNO1)

xn2 = x_map[15]
yn2 = y_map[15]
polygonNO2 = np.vstack((xn2, yn2)).T
pathN2 = mpltPath.Path(polygonNO2)

xn3 = x_map[16]
yn3 = y_map[16]
polygonNO3 = np.vstack((xn3, yn3)).T
pathN3 = mpltPath.Path(polygonNO3)

xn4 = x_map[18]
yn4 = y_map[18]
polygonNO4 = np.vstack((xn4, yn4)).T
pathN4 = mpltPath.Path(polygonNO4)

xn5 = x_map[4]
yn5 = y_map[4]
polygonNO5 = np.vstack((xn5, yn5)).T
pathN5 = mpltPath.Path(polygonNO5)

xs1 = x_map[22]
ys1 = y_map[22]
polygonSE1 = np.vstack((xs1, ys1)).T
pathS1 = mpltPath.Path(polygonSE1)

xs2 = x_map[21]
ys2 = y_map[21]
polygonSE2 = np.vstack((xs2, ys2)).T
pathS2 = mpltPath.Path(polygonSE2)

xs3 = x_map[20]
ys3 = y_map[20]
polygonSE3 = np.vstack((xs3, ys3)).T
pathS3 = mpltPath.Path(polygonSE3)

xs4 = x_map[19]
ys4 = y_map[19]
polygonSE4 = np.vstack((xs4, ys4)).T
pathS4 = mpltPath.Path(polygonSE4)

xFi = x_map[8]
yFi = y_map[8]
polygonFi = np.vstack((xFi, yFi)).T
pathFi = mpltPath.Path(polygonFi)

xDk = x_map[5]
yDk = y_map[5]
polygonDK2 = np.vstack((xDk, yDk)).T
pathD2 = mpltPath.Path(polygonDK2)

bus = pd.read_excel("N490.xlsx", index_col=0)

# Assign Loads in Norway to buses

"Read the load file for Norway"
No = pd.read_excel("Norway.xlsx", index_col=0)

# Find the nearest bus
loc = cdist(arr(No.iloc[:, mult_ind(['x', 'y'], list(No))]), arr(bus.loc[:, ['x', 'y']]))
pos = arr(bus.index[np.argmin(loc, axis=1)])
region = np.array([])

"To limit distribution of loads to within Norway"
i = 0
for k, row in No.iterrows():
    j = 0
    while j < 40:
        pos[i] = bus.index[np.argpartition(loc[i], j)[j]]
        j += 1
        points = np.array([No.at[k, 'x'], No.at[k, 'y']]).reshape(1, 2)
        if (bus.at[pos[i], 'country'] == 'NO'):
            if pathN1.contains_points(points):
                if (bus.at[pos[i], 'bidz'] == 'NO1'):
                    region = np.append(region, 'NO1')
                    break
                else:
                    continue
            elif pathN2.contains_points(points):
                if (bus.at[pos[i], 'bidz'] == 'NO2'):
                    region = np.append(region, 'NO2')
                    break
                else:
                    continue
            elif pathN3.contains_points(points):
                if (bus.at[pos[i], 'bidz'] == 'NO3'):
                    region = np.append(region, 'NO3')
                    break
                else:
                    continue
            elif pathN4.contains_points(points):
                if (bus.at[pos[i], 'bidz'] == 'NO4'):
                    region = np.append(region, 'NO4')
                    break
                else:
                    continue
            elif pathN5.contains_points(points):
                if (bus.at[pos[i], 'bidz'] == 'NO5'):
                    region = np.append(region, 'NO5')
                    break
                else:
                    continue
            else:
                region = np.append(region, bus.at[pos[i], 'bidz'])
                break
        else:
            continue
    i += 1

No['bus'] = pos
No['bidz'] = region

bN = []
for i, row in No.iterrows():
    if No.at[i, 'bidz'] == 'NO3':
        if pathN3.contains_points(np.array([No.at[i, 'x'], No.at[i, 'y']]).reshape(1, 2)):
            continue
        else:
            bN = np.append(bN, No.at[i, 'Muncipality'])

# Assign loads in Sweden to buses

"Read the load file for Sweden"
Se = pd.read_excel("Sweden.xlsx", index_col=0)

# Find the nearest bus
loc = cdist(arr(Se.iloc[:, mult_ind(['x', 'y'], list(Se))]), arr(bus.loc[:, ['x', 'y']]))
pos = arr(bus.index[np.argmin(loc, axis=1)])
region = np.array([])

"To limit distribution of loads to within Sweden"
i = 0
for k, row in Se.iterrows():
    j = 0
    while j < 40:
        pos[i] = bus.index[np.argpartition(loc[i], j)[j]]
        j += 1
        points = np.array([Se.at[k, 'x'], Se.at[k, 'y']]).reshape(1, 2)
        if (bus.at[pos[i], 'country'] == 'SE'):
            if pathS1.contains_points(points):
                if (bus.at[pos[i], 'bidz'] == 'SE1'):
                    region = np.append(region, 'SE1')
                    break
                else:
                    continue
            elif pathS2.contains_points(points):
                if (bus.at[pos[i], 'bidz'] == 'SE2'):
                    region = np.append(region, 'SE2')
                    break
                else:
                    continue
            elif pathS3.contains_points(points):
                if (bus.at[pos[i], 'bidz'] == 'SE3'):
                    region = np.append(region, 'SE3')
                    break
                else:
                    continue
            elif pathS4.contains_points(points):
                if (bus.at[pos[i], 'bidz'] == 'SE4'):
                    region = np.append(region, 'SE4')
                    break
                else:
                    continue
            else:
                region = np.append(region, bus.at[pos[i], 'bidz'])
                break
        else:
            continue
    i += 1

Se['bus'] = pos
Se['bidz'] = region

bS = []
for i, row in Se.iterrows():
    if Se.at[i, 'bidz'] == 'SE4':
        if pathS4.contains_points(np.array([Se.at[i, 'x'], Se.at[i, 'y']]).reshape(1, 2)):
            continue
        else:
            bS = np.append(bS, Se.at[i, 'Muncipality'])

# Assign loads in Finland to buses
"Read the load file for Finland"
Fi = pd.read_excel("Finland.xlsx", index_col=0)

# To find the nearset bus
loc = cdist(arr(Fi.iloc[:, mult_ind(['x', 'y'], list(Fi))]), arr(bus.loc[:, ['x', 'y']]))
pos = arr(bus.index[np.argmin(loc, axis=1)])
region = np.array([])

" To limit distribution of loads to within Finland"
for i in range(len(pos)):
    j = 0
    while j < 40:
        if (bus.at[pos[i], 'country'] == 'FI'):
            region = np.append(region, bus.at[pos[i], 'bidz'])
            break
        else:
            j += 1
            pos[i] = bus.index[np.argpartition(loc[i], j)[j]]

Fi['bus'] = pos
Fi['bidz'] = region

bF = []
for i, row in Fi.iterrows():
    if Fi.at[i, 'bidz'] == 'FI':
        if pathFi.contains_points(np.array([Fi.at[i, 'x'], Fi.at[i, 'y']]).reshape(1, 2)):
            continue
        else:
            bF = np.append(bF, Fi.at[i, 'Muncipality'])

# Assign loads in Denmark to buses
"Read the load file for Denmark"
Dk = pd.read_excel("Denmark.xlsx", index_col=0)

# To find nearest bus
loc = cdist(arr(Dk.iloc[:, mult_ind(['x', 'y'], list(Dk))]), arr(bus.loc[:, ['x', 'y']]))
pos = arr(bus.index[np.argmin(loc, axis=1)])
region = np.array([])

" To limit distribution of loads to within Denmark"
for i in range(len(pos)):
    j = 0
    while j < 40:
        if (bus.at[pos[i], 'country'] == 'DK'):
            region = np.append(region, bus.at[pos[i], 'bidz'])
            break
        else:
            j += 1
            pos[i] = bus.index[np.argpartition(loc[i], j)[j]]

Dk['bus'] = pos
Dk['bidz'] = region

bD = []
for i, row in Dk.iterrows():
    if Dk.at[i, 'bidz'] == 'DK2':
        if pathD2.contains_points(np.array([Dk.at[i, 'x'], Dk.at[i, 'y']]).reshape(1, 2)):
            continue
        else:
            bD = np.append(bD, Dk.at[i, 'Muncipality'])

""" To plot the muncipalities and bus locations"""
plt.subplots(1, 1, figsize=[6, 8])
plt.fill([-1e6, -1e6, 2e6, 2e6, -1e6], [5e6, 9e6, 9e6, 5e6, 5e6], facecolor=(220. / 255, 238. / 255, 1))
plt.tick_params(axis='both', which='both', bottom=False, labelbottom=False, top=False, labeltop=False, left=False,
               labelleft=False, right=False, labelright=False)
for x, y in zip(x_map, y_map):
    plt.fill(x, y, 'w')
    plt.plot(x, y, 'k', lw=0.5)
plt.plot(Se.x, Se.y, '.', label='Sweden Muncipalities')
plt.plot(No.x, No.y, '.', label='Norway Muncipalities')
plt.plot(Fi.x, Fi.y, '.', label='Finland Muncipalities')
plt.plot(Dk.x, Dk.y, '.', label='Denmark Muncipalities')
plt.plot(bus.x, bus.y, 'x', label='Buses')
plt.legend()
plt.xlim([-1.2e5, 1.35e6])
plt.ylim([5.9e6, 7.95e6])
plt.subplots_adjust(bottom=0.01, top=0.99, left=0.01, right=0.99)
plt.show()

"""To modify bus allocation for generators"""
gen = pickle.load(open('gen_org.pkl', 'rb'))

loc = cdist(arr(gen.iloc[:, mult_ind(['x', 'y'], list(gen))]), arr(bus.loc[:, ['x', 'y']]))
pos = arr(bus.index[np.argmin(loc, axis=1)])
region = np.array([])
i = 0
for k, row in gen.iterrows():
    j = 0
    while j < 40:
        pos[i] = bus.index[np.argpartition(loc[i], j)[j]]
        j += 1
        points = np.array([gen.at[k, 'x'], gen.at[k, 'y']]).reshape(1, 2)
        if (gen.at[k, 'country'] == 'NO'):
            if pathN1.contains_points(points):
                if (bus.at[pos[i], 'bidz'] == 'NO1'):
                    region = np.append(region, 'NO1')
                    break
                else:
                    continue
            elif pathN2.contains_points(points):
                if (bus.at[pos[i], 'bidz'] == 'NO2'):
                    region = np.append(region, 'NO2')
                    break
                else:
                    continue
            elif pathN3.contains_points(points):
                if (bus.at[pos[i], 'bidz'] == 'NO3'):
                    region = np.append(region, 'NO3')
                    break
                else:
                    continue
            elif pathN4.contains_points(points):
                if (bus.at[pos[i], 'bidz'] == 'NO4'):
                    region = np.append(region, 'NO4')
                    break
                else:
                    continue
            elif pathN5.contains_points(points):
                if (bus.at[pos[i], 'bidz'] == 'NO5'):
                    region = np.append(region, 'NO5')
                    break
                else:
                    continue
            else:
                region = np.append(region, bus.at[pos[i], 'bidz'])
                break

        elif (gen.at[k, 'country'] == 'SE'):
            if pathS1.contains_points(points):
                if (bus.at[pos[i], 'bidz'] == 'SE1'):
                    region = np.append(region, 'SE1')
                    break
                else:
                    continue
            elif pathS2.contains_points(points):
                if (bus.at[pos[i], 'bidz'] == 'SE2'):
                    region = np.append(region, 'SE2')
                    break
                else:
                    continue
            elif pathS3.contains_points(points):
                if (bus.at[pos[i], 'bidz'] == 'SE3'):
                    region = np.append(region, 'SE3')
                    break
                else:
                    continue
            elif pathS4.contains_points(points):
                if (bus.at[pos[i], 'bidz'] == 'SE4'):
                    region = np.append(region, 'SE4')
                    break
                else:
                    continue
            else:
                region = np.append(region, bus.at[pos[i], 'bidz'])
                break
        elif (gen.at[k, 'country'] == 'FI'):
            if (bus.at[pos[i], 'bidz'] == 'FI'):
                region = np.append(region, 'FI')
                break
            else:
                continue

        elif (gen.at[k, 'country'] == 'DK'):
            if (bus.at[pos[i], 'bidz'] == 'DK2'):
                region = np.append(region, 'DK2')
                break
            else:
                continue
        else:
            continue
    i += 1

gen['bus'] = pos
gen['bidz'] = region

""" To plot the generators and buses"""
plt.subplots(1, 1, figsize=[6, 8])
plt.fill([-1e6, -1e6, 2e6, 2e6, -1e6], [5e6, 9e6, 9e6, 5e6, 5e6], facecolor=(220. / 255, 238. / 255, 1))
plt.tick_params(axis='both', which='both', bottom=False, labelbottom=False, top=False, labeltop=False, left=False,
               labelleft=False, right=False, labelright=False)
for x, y in zip(x_map, y_map):
    plt.fill(x, y, 'w')
    plt.plot(x, y, 'k', lw=0.5)
plt.plot(gen.x, gen.y, '.', label='Generators')
plt.plot(bus.x, bus.y, 'x', label='Buses')
plt.legend()
plt.xlim([-1.2e5, 1.35e6])
plt.ylim([5.9e6, 7.95e6])
plt.subplots_adjust(bottom=0.01, top=0.99, left=0.01, right=0.99)
plt.show()

"""To modify bus allocation for wind farms"""
farms = pickle.load(open('farms_org.pkl', 'rb'))

loc = cdist(arr(farms.iloc[:, mult_ind(['x', 'y'], list(farms))]), arr(bus.loc[:, ['x', 'y']]))
pos = arr(bus.index[np.argmin(loc, axis=1)])
region = np.array([])
i = 0
for k, row in farms.iterrows():
    j = 0
    while j < 40:
        pos[i] = bus.index[np.argpartition(loc[i], j)[j]]
        j += 1
        points = np.array([farms.at[k, 'x'], farms.at[k, 'y']]).reshape(1, 2)
        if (farms.at[k, 'country'] == 'NO'):
            if pathN1.contains_points(points):
                if (bus.at[pos[i], 'bidz'] == 'NO1'):
                    region = np.append(region, 'NO1')
                    break
                else:
                    continue
            elif pathN2.contains_points(points):
                if (bus.at[pos[i], 'bidz'] == 'NO2'):
                    region = np.append(region, 'NO2')
                    break
                else:
                    continue
            elif pathN3.contains_points(points):
                if (bus.at[pos[i], 'bidz'] == 'NO3'):
                    region = np.append(region, 'NO3')
                    break
                else:
                    continue
            elif pathN4.contains_points(points):
                if (bus.at[pos[i], 'bidz'] == 'NO4'):
                    region = np.append(region, 'NO4')
                    break
                else:
                    continue
            elif pathN5.contains_points(points):
                if (bus.at[pos[i], 'bidz'] == 'NO5'):
                    region = np.append(region, 'NO5')
                    break
                else:
                    continue
            else:
                region = np.append(region, bus.at[pos[i], 'bidz'])
                break

        elif (farms.at[k, 'country'] == 'SE'):
            if pathS1.contains_points(points):
                if (bus.at[pos[i], 'bidz'] == 'SE1'):
                    region = np.append(region, 'SE1')
                    break
                else:
                    continue
            elif pathS2.contains_points(points):
                if (bus.at[pos[i], 'bidz'] == 'SE2'):
                    region = np.append(region, 'SE2')
                    break
                else:
                    continue
            elif pathS3.contains_points(points):
                if (bus.at[pos[i], 'bidz'] == 'SE3'):
                    region = np.append(region, 'SE3')
                    break
                else:
                    continue
            elif pathS4.contains_points(points):
                if (bus.at[pos[i], 'bidz'] == 'SE4'):
                    region = np.append(region, 'SE4')
                    break
                else:
                    continue
            else:
                region = np.append(region, bus.at[pos[i], 'bidz'])
                break

        elif (farms.at[k, 'country'] == 'FI'):
            if (bus.at[pos[i], 'bidz'] == 'FI'):
                region = np.append(region, 'FI')
                break
            else:
                continue

        elif (farms.at[k, 'country'] == 'DK'):
            if (bus.at[pos[i], 'bidz'] == 'DK2'):
                region = np.append(region, 'DK2')
                break
            else:
                continue
        else:
            continue
    i += 1

farms['bus'] = pos
farms['bidz'] = region

""" To plot the Wind Farms"""
plt.subplots(1, 1, figsize=[6, 8])
plt.fill([-1e6, -1e6, 2e6, 2e6, -1e6], [5e6, 9e6, 9e6, 5e6, 5e6], facecolor=(220. / 255, 238. / 255, 1))
plt.tick_params(axis='both', which='both', bottom=False, labelbottom=False, top=False, labeltop=False, left=False,
               labelleft=False, right=False, labelright=False)
for x, y in zip(x_map, y_map):
    plt.fill(x, y, 'w')
    plt.plot(x, y, 'k', lw=0.5)
plt.plot(farms.x, farms.y, 'g1', label='Wind Farms')
plt.plot(bus.x, bus.y, 'x', label='Buses')
plt.legend()
plt.xlim([-1.2e5, 1.35e6])
plt.ylim([5.9e6, 7.95e6])
plt.subplots_adjust(bottom=0.01, top=0.99, left=0.01, right=0.99)
plt.show()
