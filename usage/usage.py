#!/usr/bin/env python

"""Create a plot of the lsst-dev node usage.

Script uses SLURM's sacct command to collect data regarding cluster
utilization, i.e. number of nodes used either by given users or jobs.
It then creates the plot illustrating node usage of the lsst-dev cluster during
an LSST campaign. It also prints campaign's short summary including:
- the total number of node hours,
- the node-hours for each task (i.e. how many node-hours it took to complete
all coadd jobs, etc.)
"""

import extract 
import output
import process 
import argparse
import json

DEFAULT_JOB_MAPPING = {'Wi': 'singleFrame', 'Co': 'singleFrame',
                       'mo': 'mosaic', 'co': 'coadd',
                       'mt': 'multiband', 'un': 'unknown'}


def create_parser():
    """Create command line parser.

    Returns
    -------
    p : `argparse.ArgumentParser`
        Specifies command line options and stores command line data.
    """
    p = argparse.ArgumentParser()
    g = p.add_mutually_exclusive_group()
    g.add_argument('-u', '--users', type=str, default=None,
                   help='Comma separated list of users to consider. '
                        'If omitted, jobs of all users will be used.')
    g.add_argument('-j', '--jobs', type=str, default=None,
                   help='Comma separated list of jobs to consider. '
                        'If omitted, jobs of all users will be used.')
    p.add_argument('-f', '--failed', type=str, default=None,
                   help='Comma separated list of jobs that failed that '
                        'still must be added.')
    p.add_argument('-t', '--title', type=str, default=None,
                   help='Desired title of the plot. '
                        'If omitted, there will be no title on the plot.')
    p.add_argument('-n', '--name', type=str, default=None,
                   help='Desired name of the .png file. eg: usage_w2017_26. '
                        'If omitted, "usage" will be used. Note: Do not add '
                        '".png" to the end of the plot name.')
    p.add_argument('-c', '--color', action='store_true',
                   help='Make the plot color-coded based on the jobnames '
                        'from slurm.  If omitted, the plot will show only '
                        'the overall node-usage without color-coding.')
    p.add_argument('-m', '--mapping', type=str, default=None,
                   help='File with mapping between SLURM job names and their '
                        'codes in JSON format. See README.rst for more ' 'details.')
    p.add_argument('-r', '--resolution', type=int, default=800,
                   help='How many time bins the node utilization plot data '
                        'will be sorted into.  If omitted, the resolution '
                        'will be set to 800.')
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
    name : `str`
        Desired name of the plot png file.
    color : `bool`
        If the plot will be color-coded by job names.
    mapping : `dict`
        Dictionary connecting jobnames to codes used
    resolution : `int`
        Number of bins that the node usage will be sorted into
    """
    if args.title is None:
        title = ''
    else:
        title = args.title

    if args.name is None:
        name = 'usage'
    else:
        name = args.name

    color = args.color

    mapping = dict(DEFAULT_JOB_MAPPING)
    if args.mapping is not None:
        with open(args.mapping, 'r') as f:
            mapping = json.load(f)
        if 'un' in mapping:
            mapping.pop('un')
        mapping['un'] = 'unknown'

    resolution = args.resolution

    return title, name, color, mapping, resolution


if __name__ == '__main__':
    parser = create_parser()
    args = parser.parse_args()
    data = extract.gather_data(args)
    title, name, color, mapping, resolution = get_args(args)
    extract.convert_times(data)
    extract.convert_names(data, mapping)
    times, nodes, jobs = process.get_usage(data, res=resolution)
    output.make_plot(title, times, nodes, name, jobs, color)
    node_hours = process.get_nodehours(data)
    print(node_hours)
    code_nodehours = process.get_codehours(data)
    print(code_nodehours)
