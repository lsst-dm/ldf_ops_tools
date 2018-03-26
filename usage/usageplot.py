"""Create a plot of the lsst-dev node usage.

Module creates the plot illustrating node usage of the lsst-dev cluster during
an LSST campaign. It also prints campaign's short summary including:
- the total number of node hours,
- the elapsed time for each task (i.e. how much time it took to complete all
coadd jobs, etc.)

Code uses output from usage.py and needs to be assigned a plot title and a
file name for the .png file to be made.
"""

import matplotlib
matplotlib.use('Agg')
import matplotlib.pyplot as plt
import numpy as np
import argparse
from collections import Counter


JOB_MAPPING = {'Wi': 'singleFrame', 'Co': 'singleFrame', 'mo': 'mosaic',
               'co': 'coadd', 'mt': 'multiband'}


def create_parser():
    """Command line parser to pick up the command line arguments.

    Returns
    -------
    p : `argparse.ArgumentParser` object
        Specifies command line options and stores command line data.Returns
    """
    p = argparse.ArgumentParser()
    p.add_argument('-t', '--title', type=str, default=None,
                   help='Desired title of the plot, '
                        'if omitted, there will be no title on the plot.')
    p.add_argument('-d', '--data', type=str, required=True,
                   help='Name of the usage output file made from usage.py.')
    p.add_argument('-n', '--name', type=str, default=None,
                   help='Desired name of the .png file. eg: usage_w2017_26. '
                        'if omitted, "usage" will be used. Note: Do not add '
                        'the ".png" to the end of the plot name.')
    p.add_argument('-c', '--color', action='store_true',
                   help='Make the plot color-coded based on the jobnames '
                        'from slurm.  If omitted, the plot will show only '
                        'the overall node-usage without color-coding.')
    return p


def get_args(args):
    """Gets values of the arguments and assigns any missing arguments their
    default values.

    Parameters
    ----------
    args: `argparse.Namespace`
        Namespace with command line arguments.

    Returns
    -------
    title : `str`
        Desired title of the plot
    data_file : `str`
        Name of the data file
    name : `str`
        Desired name of the plot png file.
    color : `bool`
        If the plot will be color-coded by job names.
    """
    if args.title is None:
        title = ''
    else:
        title = args.title

    datafile = args.data

    if args.name is None:
        name = 'usage'
    else:
        name = args.name

    color = args.color

    return title, datafile, name, color


def make_plot(title, times, nodes, name, color):
    """Makes a time vs. NNode plot named 'name'.png.

    Parameters
    ----------
    title : `str`
        title of the plot
    times : Numpy array of `float`
        array of the time values from the data_file
    nodes : Numpy array of `int`
        array of the node values from the data_file
    name : `str`
        name of the plot png file
    color : `bool`
        if the plot will be color-coded by job name
    """
    plt.figure(figsize=(8, 8))
    plt.grid(linestyle=':')

    if color:
        plt.plot(times, nodes, 'k', alpha=0.25, drawstyle="steps-post")
        start_end = get_first_last(jobs)
        colors = {'singleFrame': 'c', 'mosaic': 'xkcd:yellow', 'coadd': 'g',
                  'multiband': 'b', 'unknown': 'r'}
        hatch = {'singleFrame': '///', 'mosaic': '\\\\', 'coadd': '...',
                 'multiband': 'x', 'unknown': '**'}

        for key in start_end.keys():
            for val_tup in start_end[key]:
                plt.fill_between(times[val_tup[0]:val_tup[1]+2], 0,
                                 nodes[val_tup[0]:val_tup[1]+2],
                                 hatch=hatch[key], step="post",
                                 facecolor=colors[key], alpha=0.5,
                                 label=key if key not in
                                 plt.gca().get_legend_handles_labels()[1]
                                 else '')
        plt.legend(title='Code Name')

    else:
        plt.plot(times, nodes, 'b', drawstyle="steps-post")
        plt.fill_between(times, 0, nodes, step="post", facecolor='b',
                         alpha=0.25)

    plt.ylim(0, 50)
    plt.xlim(xmin=0)

    plt.xlabel(r'time [h]', fontsize=16)
    plt.ylabel(r'$\ N_{node} $', fontsize=16)
    plt.tick_params(axis='both', which='major', labelsize=14)
    plt.title(title, fontsize=18)
    plt.savefig(name + ".png")


def get_first_last(jobs):
    """Gets the first and the last index of the code names of the same type,
    (i.e.: will return the first and last index of the singleFrameDriver jobs)
    which will allow for color-coding in make-plot.

    Parameters
    ----------
    jobs : `list` of `lists`
        list of the job names from SLURM at a given time step

     Returns
    -------
    start_end : `dict` of `list` of `tuple`
        dictionary containing code names and the start and stop indexes
        of the code runs.
    """
    # Creates dictionary of the first two letters of a job name & the indexes
    # where those job names can be found.
    names = dict()
    for idx, lst in enumerate(jobs):
        for job in lst:
            names.setdefault(job[:2], set()).add(idx)

    # Translates common entries in the names dictionary to the actual code
    # names. If the key in names doesn't match with any of those below, the
    # code will be marked as "unknown"
    data_final = {k: set() for k in set(JOB_MAPPING.values())}
    for key, val in names.items():
        name = JOB_MAPPING.get(key, 'unknown')
        data_final[name].update(val)

    # Removes repeated index values for each key and sorts the values into
    # ascending order.
    data_final = {k: sorted(v) for k, v in data_final.items()}

    # Creates a dictionary only listing tuples of the first and last indexes of
    # when a code was run.  If there are gaps in the indexes, then there will
    # be multiple tuples for each key, thus denoting when that code started and
    # stopped. This is done to make the shading in make_plot more accurate.
    start_end = dict()
    for key, value in data_final.items():
        gap = [[s, e] for s, e, in zip(value, value[1:]) if e - s > 1]
        edges = value[:1] + sum(gap, []) + value[-1:]
        start_end[key] = list(zip(edges[::2], edges[1::2]))

    return start_end


def get_data(data_file):
    """Extracts the data from the data_file.

    Parameters
    ----------
    data_file : `str`
        name of the data file to extract data from

    Returns
    -------
    times : Numpy array of `float`
        time array from the data file in hours
    nodes : Numpy array of `int`
        array of node usage from the data file
    jobs : `list` of `str`
        list of the job names from SLURM at a given time step
    """
    t = []
    n = []
    jobs = []
    with open(data_file, 'r') as f:
        data = [line.strip() for line in f]
    for line in data:
        x, y, *j = line.split()
        t.append(float(x)/3600.0)
        n.append(float(y))
        if j:
            jobs.append(j[0].split(','))
        else:
            jobs.append([])
    times = np.asarray(t)
    nodes = np.asarray(n)
    return times, nodes, jobs


def get_nodehours(times, nodes):
    """Takes the times and nodes arrays and outputs the node-hours used for
    all of the slurm jobs requested.

    Parameters
    ----------
    times : Numpy array of `float`
        time array from the data file in hours
    nodes : Numpy array of `int`
        array of node usage from the data file

    Returns
    -------
    node_hours : `float`
        the total node-hours spent on all of the slurm jobs passed into
        usage.py.
    """
    dt = times[1] - times[0]
    node_hours = np.sum(dt*nodes)
    return node_hours


def get_elapsed(times, jobs):
    """Finds the elapsed times for each code (ie, how much time it took to
    complete all coadd jobs, etc.).

    Parameters
    ----------
    times : Numpy array of `float`
        time array from the data file in hours
    jobs : `list` of `str`
        list of the job names from SLURM at a given time step

    Returns
    -------
    elapsed : `dict` of `str` keys and `int` values
        dictionary containing the code names and their associated elapsed
        running time on slurm
    """
    # Finds time between two data points
    dt = times[1] - times[0]
    # Dict that finds number of times a certain code is mentioned in jobs
    abrev_jobs = Counter([word[:2] for lst in jobs for word in lst])
    elapsed = {k: 0 for k in set(JOB_MAPPING.values())}
    for key, val in abrev_jobs.items():
        name = JOB_MAPPING.get(key, 'unknown')
        elapsed[name] += val*dt
    return elapsed


if __name__ == '__main__':
    parse = create_parser()
    args = parse.parse_args()
    title, data_file, name, color = get_args(args)
    times, nodes, jobs = get_data(data_file)
    make_plot(title, times, nodes, name, color)
    elapsed = get_elapsed(times, jobs)
    node_hours = get_nodehours(times, nodes)
    print(node_hours)
    print(elapsed)
