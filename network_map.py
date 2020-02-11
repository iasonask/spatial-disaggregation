# -*- coding: utf-8 -*-
"""
Created on Wed May  9 14:09:53 2018

@author: jolauson

Make a map of network based on Nordic490 object

parallella ledningar eller nära objekt -> funktion för att trycka på n och få nästa



"""

import numpy as np
from numpy import flatnonzero as find
from numpy import atleast_1d as arr
from numpy import concatenate as cat
import matplotlib.pyplot as plt
from matplotlib import collections  as mcoll
from scipy.spatial.distance import cdist
import scipy.interpolate
from scipy.spatial import cKDTree
from matplotlib import cm
import os


########################################################################
# A few functions
########################################################################


def mult_ind(a, b, miss=np.nan):
    """ Get indices for elements of a in b, returns numpy array.
    E.g. mult_ind([1,2,1,4],[3,2,1]) -> array([2.,1.,2.,nan]) """
    bind = {}
    for i, elt in enumerate(b):
        if elt not in bind:
            bind[elt] = i
    return arr([bind.get(itm, miss) for itm in a])


def str_sig(number, sign_figs):
    """ Returns number as string with specified significant figures.
    Special treatment of bool, str, nan and complex. """

    if type(number) == np.bool_ and number == True:
        return 'True'
    if type(number) == np.bool_ and number == False:
        return 'False'

    if type(number) in [str, np.str_]:
        return number

    if np.isnan(number):
        return 'nan'

    if np.iscomplex(number):
        real = str_sig(np.real(number), sign_figs)
        imag = str_sig(np.imag(number), sign_figs)
        if imag[0] == '-':
            return real + ' - j' + imag[1:]
        else:
            return real + ' + j' + imag

    a = float("{1:.{0}e}".format(sign_figs - 1, number))
    if abs(a) < 10 ** (sign_figs - 1):
        if a == 0:
            return '0'
        else:
            return str(a)
    else:
        return str(int(a))


########################################################################
# Class for making maps
########################################################################

class Map:
    """ Class for making network plots, see documentation and methods for more info.
    """

    def __init__(self, n490):

        """ Initiate using Nordic490 object
        """

        self.bus, self.line, self.link = n490.bus.copy(), n490.line.copy(), n490.link.copy()

        # Add generator info to bus?

        self.line_colored = None
        self.line_widthed = None
        self.hm = None

        # Make settings
        self.plot_settings()
        self.set_map_properties()

    def plot_settings(self):
        """ Plotting settings (colors, linewidths etc.), possibly depending on bus variable.
        """

        var = 'Vbase'  # base colors etc on Vbase
        var_lim = [380, 300, 0]  # different categories of Vbase, should be a list

        # bus settings
        self.sets_variable_lim = var_lim
        var = self.bus.loc[:, var]
        self.bus_set = arr([find(v >= arr(var_lim))[0] if v >= var_lim[-1] else -1 for v in var])
        self.bus_color = ['r', (230. / 255, 152. / 255, 0), 'g']
        self.bus_name_color = ['k'] * 3
        self.bus_lw = [1.5, 1, 1]
        self.bus_name_fs = [0, 0, 0]

        # line settings
        var_line = var.loc[self.line.bus0]
        self.line_set = arr([find(v >= arr(var_lim))[0] if v >= var_lim[-1] else -1 for v in var_line])
        self.line_lw = [1, 1, 1]
        self.line_color = ['r', (230. / 255, 152. / 255, 0), 'g']

        # Link
        self.link_lw = 1
        self.link_color = 'b'

        # Interactive plot
        self.interactive = True  # interactive map mode
        self.picker_node = 7  # tolerance for interactive picking
        self.picker_arc = 3
        self.significant_figures = 3  # when info is displayed
        self.info_fc = [213. / 255, 230. / 255, 1]  # color for info box
        self.info_ec = 'k'  # color info-box edge
        self.info_lw = 1  # info-box edge width

        self.equal_aspect = False

    def set_map_properties(self):
        """ Map properties such as range, depend on visible nodes. """

        # Default map properties
        self.x_min = -1.2e5
        self.x_max = 1.35e6
        self.y_min = 5.9e6
        self.y_max = 7.95e6
        self.extent = [self.x_min, self.x_max, self.y_min, self.y_max]
        self.x_range = self.x_max - self.x_min
        self.y_range = self.y_max - self.y_min
        self.sub_borders = [0.01, 0.99, 0.01, 0.99]  # bottom, top, left, right for subplots

        # Default heatmap properties
        gp = max(len(self.bus), 300) * 100  # ~100*N grid points in heatmaps
        self.x_grid_points = int(gp ** 0.5 * (self.x_range / self.y_range) ** 0.5)
        self.y_grid_points = int(gp / self.x_grid_points)

    def line_colors_from(self, var, cmap='jet'):
        """ Let the color of the arcs depend on variable var (e.g. 'line_loading'). """

        if type(var) is str:
            var = self.line.loc[:, var].values

        if np.nanmax(var) > np.nanmin(var):
            var = (var - np.nanmin(var)) / (np.nanmax(var) - np.nanmin(var))  # normalise 0-1
        var[np.isnan(var)] = 0
        cmap = cm.get_cmap(cmap)
        self.line_colored = cmap(var)

    def line_widths_from(self, var, w_max=4, w_min=0.5):
        """ Let the width of the arcs depend on absolute value of variable var (e.g. 'P1'). """
        if type(var) is str:
            var = self.line.loc[:, var].values
        var = var * w_max / np.nanmax(var)  # normalise to w_max
        var[np.isnan(var)] = w_min
        var[var < w_min] = w_min
        self.line_widthed = var

    def add_heatmap(self, var, method=1, num=6, w_exp=2, cmap='jet', clim=None):
        """ Calculate heatmap for node variable 'var' using interpolation
        method = 0: weighted on distance^w_exp (only num nearest neighbours considered)
        method = 1: LinearNDInterpolator
        method = 2: NearestNDInterpolator
        method = 3: griddata (cubic)
        clim e.g. [0.9, 1.1], cmap e.g. 'jet','viridis' """

        ind = self.bus_set >= 0  # nodes displayed
        if type(var) is str:
            self.heatmap_var = var
            var = self.bus.loc[:, var].values[ind]
        else:  # array
            self.heatmap_var = ''
            var = var[ind]

        xx = np.linspace(self.x_min, self.x_max, self.x_grid_points)  # points for interpolation
        yy = np.linspace(self.y_min, self.y_max, self.y_grid_points)
        xx2, yy2 = np.meshgrid(xx, yy)
        xy = np.column_stack((self.bus.x.values[ind], self.bus.y.values[ind]))  # nnode * 2 matrix
        if method == 0:
            xy2 = np.column_stack((xx2.flatten(), yy2.flatten()))
            tree = cKDTree(xy)  # build tree
            d, ind = tree.query(xy2, np.arange(1, num + 1))  # query nearest distances
            w = d ** w_exp
            hm = (np.sum(w * var, axis=1) / np.sum(w, axis=1)).reshape((len(yy), -1))
        elif method == 1:
            f = scipy.interpolate.LinearNDInterpolator(xy, var)
            hm = f(xx2, yy2)
        elif method == 2:
            f = scipy.interpolate.NearestNDInterpolator(xy, var)
            hm = f(xx2, yy2)
        elif method == 3:
            hm = scipy.interpolate.griddata(xy, var, (xx2, yy2), method='cubic')

        hm = np.flipud(hm)

        if clim is None:
            self.hm = self.ax.imshow(hm, extent=self.extent, cmap=cmap, zorder=1, aspect='auto')
        else:
            self.hm = self.ax.imshow(hm, extent=self.extent, clim=clim, cmap=cmap, zorder=1, aspect='auto')

    def init_plot(self, fig_size=[13, 8], bidz_map=os.path.join('Data', 'raw', 'map_with_bidz2018.npz')):
        """ Initialize plot.
        fig_size can be:
            1) [w,h] in inches, the one that limits sets size
            2) height in inches
            3) [x,y,dx,dy] in pixels"""

        temp = np.load(bidz_map, allow_pickle=True)
        x_map = temp['x']
        y_map = temp['y']

        if self.x_range / self.y_range > fig_size[0] / fig_size[1]:  # width limits size
            fs = (fig_size[0], fig_size[0] * self.y_range / self.x_range)
        else:  # height limits size
            fs = (fig_size[1] * self.x_range / self.y_range, fig_size[1])
        self.fig, self.ax = plt.subplots(figsize=fs)

        sb = self.sub_borders
        self.fig.subplots_adjust(bottom=sb[0], top=sb[1], left=sb[2], right=sb[3])
        plt.subplots_adjust(wspace=0, hspace=0)
        self.ax.set_xlim([self.x_min, self.x_max])
        self.ax.set_ylim([self.y_min, self.y_max])
        self.ax.tick_params(axis='both', which='both', bottom=False, labelbottom=False,
                            top=False, labeltop=False, left=False, labelleft=False, right=False, labelright=False)

        # background color of sea and white land
        self.ax.fill([-1e6, -1e6, 2e6, 2e6, -1e6], [5e6, 9e6, 9e6, 5e6, 5e6], facecolor=(220. / 255, 238. / 255, 1))
        for x, y in zip(x_map, y_map):
            plt.fill(x, y, 'w')

        # plot borders (and bidding zone names)
        for x, y in zip(x_map, y_map):
            plt.plot(x, y, 'k', lw=0.5)

    def add_topo(self):
        """ Add topology; nodes and arcs with plotting properties set with plot_settings()."""

        # Lists used to update plot after manual editing of node positions, not necessary now when possibility
        # to change bus positions has been removed, but some code changes required to simplify...
        self.segments_index = []  # node index
        self.segments = []  # coordinates
        self.segments_object = []  # plotting object (LineCollection or plot)

        # buses + labels
        for s in np.unique(self.bus_set[self.bus_set >= 0]):
            lw, c = self.bus_lw[s], self.bus_color[s]
            fs, c2 = self.bus_name_fs[s], self.bus_name_color[s]
            ind = find(self.bus_set == s)
            x = self.bus.x.values[ind]
            y = self.bus.y.values[ind]
            text = arr([str(s) for s in self.bus.index.values[ind]])

            if s == 0:
                label = '380 kV'
            elif s == 1:
                label = '300 kV'
            else:
                label = '220 kV or less'

                # plot buses
            gid = ['bus%d' % i for i in ind]
            self.segments_index.append(ind)
            self.segments.append(np.column_stack((x, y)))
            self.segments_object.append(self.ax.plot(x, y, ms=lw ** 2, marker='o', ls='', c=c, zorder=10,
                                                     label=label, picker=5, gid=gid)[0])

            # bus text
            if fs > 0:
                dy = lw * self.y_range / 2000  # displacement of text

                for n in range(len(ind)):
                    self.ax.text(x[n], y[n] + dy, text[n], va='bottom', ha='center', color=c2, size=fs, zorder=15)

        # lines
        for s in np.unique(self.line_set[self.line_set >= 0]):
            ind = find(self.line_set == s)
            lw, c = self.line_lw[s], self.line_color[s]
            if self.line_colored is not None:
                c = self.line_colored[ind]  # color depending on some variable
            if self.line_widthed is not None:
                lw = self.line_widthed[ind]  # line width depending on some variable

            # plot lines
            gid = ['line%d' % i for i in ind]
            coll = zip(self.line.x.values[ind], self.line.y.values[ind])
            segments = [[(x, y) for x, y in zip(xx, yy)] for xx, yy in coll]
            self.segments.append(segments)
            lc = mcoll.LineCollection(segments, colors=c, linewidths=lw, gid=gid, zorder=5, picker=self.picker_arc)
            self.segments_object.append(self.ax.add_collection(lc))

        # links
        lw, c = self.link_lw, self.link_color
        gid = ['link%d' % i for i in range(len(self.link))]
        coll = zip(self.link.x.values, self.link.y.values)
        segments = [[(x, y) for x, y in zip(xx, yy)] for xx, yy in coll]
        self.segments.append(segments)
        lc = mcoll.LineCollection(segments, colors=c, linewidths=lw, gid=gid, zorder=5, picker=self.picker_arc)
        self.segments_object.append(self.ax.add_collection(lc))

    def add_legend(self):
        """ Adds legend and colorbar (if heatmap present). """
        handles, labels = self.ax.get_legend_handles_labels()
        labels, handles = zip(*sorted(zip(labels, handles), key=lambda t: t[0], reverse=True))  # Largest to smallest
        corners = arr([[self.x_max, self.y_max], [self.x_min, self.y_max],  # best corners for legend and colorbars
                       [self.x_min, self.y_min], [self.x_max, self.y_min]])
        min_dist = [min(x) for x in cdist(corners, np.column_stack((self.bus.x, self.bus.y))[self.bus_set >= 0, :])]
        loc = 2
        self.ax.legend(handles, labels, frameon=False, loc=loc, fontsize=8)
        min_dist[loc] = 0  # do not use same corner again

        # colorbar(s)
        pos_cb = [[0.9, 0.8, 0.03, 0.15], [0.05, 0.8, 0.03, 0.15], [0.05, 0.05, 0.03, 0.15], [0.9, 0.05, 0.03, 0.15]]
        if self.hm is not None:
            pos_cb_hm = pos_cb[np.argmax(min_dist)]
            min_dist[np.argmax(min_dist)] = 0
            cbaxes_hm = self.fig.add_axes(pos_cb_hm)
            cbar_hm = self.fig.colorbar(self.hm, cax=cbaxes_hm)
            cbar_hm.ax.set_xlabel(self.heatmap_var, size=9)
            cbar_hm.ax.tick_params(labelsize=9)

    def save(self, file_save):
        """ Save the map, e.g. pdf, png."""
        try:
            plt.savefig(file_save)
        except PermissionError:
            print('%s already open or permission denied' % file_save)

    def show(self):
        """ Show map (saved file or interactive plt.show). """

        def display_info(event):
            """ On click, display node/arc information. """

            ind = arr(event.artist.get_gid())[event.ind[0]]

            if self.event_id == ind:
                annot.set_visible(False)
                self.event_id = ''
                self.fig.canvas.draw_idle()
                return
            else:
                self.event_id = ind

            if 'bus' in ind:
                n = int(ind[3:])  # bus index (starting from 0)
                info = str(self.bus.iloc[n])
                x, y = self.bus.x.values[n], self.bus.y.values[n]
            elif 'line' in ind:
                n = int(ind[4:])  # line index
                info = str(self.line.iloc[n].drop(['x', 'y', 'lat', 'lon']))  # do not show arrays of coordinates
                x = np.mean(self.line.x.values[n])
                y = np.mean(self.line.y.values[n])
            else:
                n = int(ind[4:])  # link index
                info = str(self.link.iloc[n].drop(['x', 'y', 'lat', 'lon']))
                x = np.mean(self.link.x.values[n])
                y = np.mean(self.link.y.values[n])
            annot.xy = (x, y)
            annot.set_position(annot_xy(x, y, info))
            annot.set_visible(True)
            annot.set_text(info)
            self.fig.canvas.draw_idle()

        def annot_xy(x, y, info):
            """ Calculate xytext for annotation.
            (approximately good location, would be better with anchor)"""
            info_width = np.max([len(i) for i in info.split('\n')])
            info_lines = len(info.split('\n'))
            x1, x2 = self.ax.get_xlim()
            y1, y2 = self.ax.get_xlim()
            d = cdist(np.atleast_2d([x, y]), arr([[x1, y1], [x1, y2], [x2, y2], [x2, y1]]))
            loc = np.argmax(d[0])  # best placement of text
            xs, ys = 4.5, 8
            if loc == 0:
                xt = -info_width * xs - 20
                yt = -info_lines * ys - 20
            elif loc == 1:
                xt = -info_width * xs - 20
                yt = 20
            elif loc == 2:
                xt, yt = 20, 20
            elif loc == 3:
                xt = 20
                yt = -info_lines * ys - 20

            return (xt, yt)

        self.event_id = None
        annot = self.ax.annotate("", xy=(0, 0), xytext=(-100, 20), textcoords="offset points",
                                 bbox=dict(boxstyle="round", fc=self.info_fc, ec=self.info_ec, lw=self.info_lw),
                                 arrowprops=dict(arrowstyle="->"), zorder=100, fontsize=8)
        annot.set_visible(False)
        self.mpl_id = [self.fig.canvas.mpl_connect('pick_event', display_info)]

        plt.show()


