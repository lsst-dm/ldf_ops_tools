"""Create a plot of the lsst-dev node useage over time

Code uses output from usage.py and needs to be assigned
a plot title and a file name for the .png file to be made.
"""

import matplotlib
matplotlib.use('Agg')
import matplotlib.pyplot as plt
import numpy as np
import argparse

def create_parser():
    """Command line parser to pick up the command line arguments.
    Returns
    -------
    p : 'argparse.ArgumentParser' object 
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
    """ Gets values of the arguments and assigns any missing arguments their
	default values

    Parameters
    ----------
    args: 'argparse.Namespace'
	Namespace with command line arguments.

    Returns
    -------
    title : 'str'
	Desired title of the plot
    data_file : 'str'
	Name of the data file
    name : 'str'
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

def make_plot(title, data_file, name):
    """ Gets the title and data_file name and makes a time vs. NNode plot named usage.png

    Parameters
    ----------
    title : 'str'
	title of the plot
    data_file : 'str'
	name of the data file to extract data from
    name : 'str'
	name of the plot png file 
    """
    t = []
    n = []

    with open(data_file,'r') as f:
        data = [line.strip() for line in f]
    for line in data:
        x, y = line.split()
        t.append(float(x)/3600.0)
        n.append(float(y))
    t = np.asarray(t)
    n = np.asarray(n)       

    plt.figure(figsize=(8, 8))
    plt.plot(t, n, 'b', drawstyle="steps-post")
    plt.ylim(0, 50)
    plt.xlim(xmin=0)
    plt.grid(linestyle=':')
    plt.fill_between(t, 0, n, step="post", facecolor='b', alpha=0.25)
    plt.xlabel(r'time [h]', fontsize=12)
    plt.ylabel(r'$\ N_{node} $', fontsize=12)
    plt.title(title, fontsize=14)
    plt.savefig(name + ".png")

if __name__== '__main__':
    parse = create_parser()
    args = parse.parse_args()	
    title, data_file, name = get_args(args)
    make_plot(title, data_file, name)   

