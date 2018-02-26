"""Create a plot of the lsst-dev node useage over time and prints the total 
node_hours of the jobs provided to usage.py.

Code uses output from usage.py and needs to be assigned a plot title and a
file name for the .png file to be made.
"""


import matplotlib
import matplotlib.pyplot as plt
import numpy as np
import argparse


matplotlib.use('Agg')


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
    p.add_argument('-d', '--data', type=str, default=None,
                   help='Name of the usage output file made from usage.py, '
                        'if omitted, usage.out in the current directory will be used.')
    p.add_argument('-n', '--name', type=str, default=None,
                   help='Desired name of the .png file. eg: usage_w2017_26. '
                        'if omitted, "usage" will be used. Note: Do not add the ".png" '
                        'to the end of the plot name.')
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
    """
    if args.title is None:
        title = ''
    else:
        title = args.title

    if args.data is None:
        datafile = 'usage.out'
    else:
        datafile = args.data

    if args.name is None:
        name = 'usage'
    else:
        name = args.name

    return title, datafile, name


def make_plot(title, times, nodes, name):
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
    """ 
    plt.figure(figsize=(8, 8))
    plt.plot(times, nodes, 'b', drawstyle="steps-post")
    plt.ylim(0, 50)
    plt.xlim(xmin=0)
    plt.grid(linestyle=':')
    plt.fill_between(times, 0, nodes, step="post", facecolor='b', alpha=0.25)
    plt.xlabel(r'time [h]', fontsize=16)
    plt.ylabel(r'$\ N_{node} $', fontsize=16)
    plt.tick_params(axis='both', which='major', labelsize=14)
    plt.title(title, fontsize=18)
    plt.savefig(name + ".png")


def get_data(data_file):
    """Extracts the data from the data_file and puts it into two arrays. 

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
    """
    t = []
    n = []
   
    with open(data_file, 'r') as f:
        data = [line.strip() for line in f]
    for line in data:
        x, y = line.split()
        t.append(float(x)/3600.0)
        n.append(float(y))
    times = np.asarray(t)
    nodes = np.asarray(n)
    
    return times, nodes


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


if __name__ == '__main__':
    parse = create_parser()
    args = parse.parse_args()
    title, data_file, name = get_args(args)
    times, nodes = get_data(data_file)
    make_plot(title, times, nodes, name)   
    node_hours = get_nodehours(times, nodes)
    print(node_hours)
