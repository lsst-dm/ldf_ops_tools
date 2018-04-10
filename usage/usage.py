#!/usr/bin/env python

"""Create a plot of the lsst-dev node usage.

Script uses SLURM's sacct command to collect data regarding cluster
utilization, i.e. number of nodes used either by given users or jobs.
It then creates the plot illustrating node usage of the lsst-dev cluster during
an LSST campaign. It also prints campaign's short summary including:
- the total number of node hours,
- the elapsed time for each task (i.e. how much time it took to complete all
coadd jobs, etc.)
"""


import json
import numpy as np
import argparse
import datetime
from subprocess import PIPE, Popen
import matplotlib
matplotlib.use('Agg')
import matplotlib.pyplot as plt


DEFAULT_JOB_MAPPING = {'Wi': 'singleFrame', 'Co': 'singleFrame',
                       'mo': 'mosaic', 'co': 'coadd',
                       'mt': 'multiband', 'un': 'unknown'}
DEFAULT_KEY_LEN = 2


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
                        'codes in JSON format. See README.rst for more '
                        'details.')

    return p


def gather_data(args):
    """Gather cluster usage data using SLURM's sacct command.

    Parameters
    ----------
    args : `argparse.Namespace`
        Namespace with command line arguments.

    Returns
    -------
    `list` of `dict`
        Accounting data from slurm.

    Raises
    ------
    OSError
        If executing sacct command fails for some reason.
    """
    # Set command name.
    argv = ['sacct']

    # Specify what data should be displayed.
    fields = ['jobid', 'jobname', 'nnodes', 'submit', 'start', 'end', 'state']
    argv.append('--format={}'.format(','.join(fields)))

    # Specify either jobs or users for which the data will be gathered.
    if args.jobs is None:
        if args.users is None:
            argv.append('--allusers')
        else:
            argv.append('--user={}'.format(args.users))
    else:
        argv.append('--jobs={}'.format(args.jobs))

    # Include completed jobs and those terminated due to node failures as they
    # could be succesfully compeleted after being rescheduled.
    argv.append('--state=CD,NF')

    # Output data in a format easy to parse (here: CSV).
    argv.extend(['--delimiter=,', '--noheader', '--parsable2'])

    # Execute the command.
    proc = Popen(argv, stdout=PIPE, stderr=PIPE, encoding='utf-8')
    stdout, stderr = proc.communicate()

    # Ignore warnings about conflicting records, but terminate execution in any
    # other case.
    if stderr != '':
        if "Conflicting JOB_TERMINATED record (COMPLETED)" in stderr:
            pass
        else:
            print(stderr)
            raise OSError('failed to gather data.')

    # Collect accounting data for all jobs (even failed ones), but only for
    # sucessfull steps.
    jobs, steps = {}, {}
    lines = stdout.splitlines()
    for line in lines:
        values = line.strip().split(',')
        rec = {field: value for field, value in zip(fields, values)}
        tokens = rec['jobid'].split('.')
        id_ = tokens[0]
        if len(tokens) == 1:
            jobs[id_] = rec
        else:
            if rec['state'] == 'COMPLETED':
                steps[id_] = rec

    # Update selected accounting data of failed jobs with those from
    # corresponding succesfull steps.
    fails = [id_ for id_, job in jobs.items() if job['state'] != 'COMPLETED']
    for id_ in set(fails) & set(steps):
        job = steps[id_]
        patch = {key: job[key] for key in ['submit', 'start', 'end', 'state']}
        jobs[id_].update(patch)
    return list(job for job in jobs.values() if job['state'] == 'COMPLETED')


def get_usage(data, res=100):
    """Find out cluster node usage.

    Parameters
    ----------
    data : `list` of `dict`
        Accounting data.
    res : `int`, optional
        Number of samples, defaults to 100.

    Returns
    -------
    times : np.array of `float`
        Midpoints of time intervals
    jobs : `list` of `list` of `int`
        List of number of nodes used either by given jobs or users in the
        corresponding time intervals.
    usage : Numpy array of `int`
        array of the node values from the data_file
    """
    begin = min(rec['start'] for rec in data)
    end = max(rec['end'] for rec in data)

    duration = (end - begin).total_seconds()
    step = duration / res

    # Express time relative to the beginning of the first job.
    start_times = [(rec['start'] - begin).total_seconds() for rec in data]
    end_times = [(rec['end'] - begin).total_seconds() for rec in data]

    node_counts = [int(rec['nnodes']) for rec in data]
    job_type = [rec['jobname'] for rec in data]

    # Make a histogram representing number of used nodes in a given time
    # interval
    usage = [0] * res
    names = [''] * res
    for begin, end, nodes, name in zip(start_times, end_times, node_counts, job_type):
        i, j = int(begin / step), int(end / step)
        for k in range(i, j):
            usage[k] += nodes
            names[k] += name + ','
    names = [s.rstrip(',') for s in names]
    times = np.asarray([step * (i + 0.5) for i in range(len(usage))])/3600.0
    usage = np.asarray(usage)

    jobs = []
    for j in names:
        if j != '':
            jobs.append(j.split(','))
        else:
            jobs.append([])

    return times, usage, jobs


def convert_times(data):
    """Convert times to `datetime` objects for easier manipulation.

    Parameters
    ----------
    data : `list` of `dict`
        accounting data from slurm.
    """
    fmt = '%Y-%m-%dT%H:%M:%S'
    for datum in data:
        for field in ['end', 'start', 'submit']:
            datum[field] = datetime.datetime.strptime(datum[field], fmt)


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
    key_len : `int`
        Length of the keys in "mapping"
    mapping : `dict`
        Dictionary connecting jobnames to codes used
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
    key_len = DEFAULT_KEY_LEN
    if args.mapping is not None:
        with open(args.mapping, 'r') as f:
            mapping = json.load(f)
        if 'un' in mapping:
            mapping.pop('un')
        key_len = len(list(mapping.keys())[0])
        mapping['un'] = 'unknown'

    return title, name, color, key_len, mapping


def make_plot(title, times, nodes, name, job_list, color, key_len, mapping):
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
    job_list : `list` of `lists`
        list of the job names from SLURM at a given time step
    color : `bool`
        if the plot will be color-coded by job name
    key_len : `int`
        Length of the keys in "mapping"
    mapping : `dict`
        Dictionary connecting jobnames to codes used
    """
    plt.figure(figsize=(8, 8))
    plt.grid(linestyle=':')

    if color:
        plt.plot(times, nodes, 'k', alpha=0.25, drawstyle="steps-post")
        start_end = get_first_last(job_list, key_len, mapping)
        colors = {'singleFrame': 'c', 'mosaic': 'xkcd:yellow', 'coadd': 'g',
                  'multiband': 'b', 'unknown': 'r', 'forc': 'xkcd:purple'}
        hatch = {'singleFrame': '///', 'mosaic': '\\\\', 'coadd': '...',
                 'multiband': 'x', 'unknown': '**', 'forc': 'oo'}

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


def get_first_last(jobs, key_len, mapping):
    """Gets the first and the last index of the code names of the same type,
    (i.e.: will return the first and last index of the singleFrameDriver jobs)
    which will allow for color-coding in make-plot.

    Parameters
    ----------
    jobs : `list` of `lists`
        list of the job names from SLURM at a given time step
    key_len : `int`
        Length of the keys in "mapping"
    mapping : `dict`
        Dictionary connecting jobnames to codes used


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
            names.setdefault(job[:key_len], set()).add(idx)

    # Translates common entries in the names dictionary to the actual code
    # names. If the key in names doesn't match with any of those below, the
    # code will be marked as "unknown"
    data_final = {k: set() for k in set(mapping.values())}

    for key, val in names.items():
        name = mapping.get(key, 'unknown')
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


def get_nodehours(data):
    """Takes the accounting data from SLURM and outputs the node-hours used for
    all of the slurm jobs requested.

    Parameters
    ----------
    data : `list` of `dict`
        accounting data from slurm.

    Returns
    -------
    node_hours : `float`
        the total node-hours spent on all of the slurm jobs passed into
        usage.py.
    """
    # Find duration of each job
    duration = [(rec['end'] - rec['start']).total_seconds() for rec in data]
    nodes = [int(rec['nnodes']) for rec in data]

    # Collect elapsed time data
    node_hours = sum(dur*node for dur, node in zip(duration, nodes))

    return round(node_hours/3600.0, 2)


def get_elapsed(data, key_len, mapping):
    """Finds the elapsed times for each code (ie, how much time it took to
    complete all coadd jobs, etc.).

    Parameters
    ----------
    data : `list` of `dict`
        accounting data from slurm.
    key_len : `int`
        Length of the keys in "mapping"
    mapping : `dict`
        Dictionary connecting jobnames to codes used

    Returns
    -------
    elapsed : `dict` of `str` keys and `float` values
        dictionary containing the code names and their associated elapsed
        running time on SLURM in hours
    """
    # Find duration of each job
    duration = [(rec['end'] - rec['start']).total_seconds() for rec in data]

    job_type = [rec['jobname'] for rec in data]

    # Collect elapsed time data
    elapsed_times = {}
    for dur, name in zip(duration, job_type):
        elapsed_times[name] = (dur)/3600.0

    # First you add the numbers together
    elapsed = {k: 0 for k in set(mapping.values())}
    for key, val in elapsed_times.items():
        name = mapping.get(key[:key_len], 'unknown')
        elapsed[name] += val

    # Then you find the significant figures
    for key, val in elapsed.items():
        elapsed[key] = round(val, 2)

    return elapsed


if __name__ == '__main__':
    parser = create_parser()
    args = parser.parse_args()
    data = gather_data(args)
    title, name, color, key_len, mapping = get_args(args)
    convert_times(data)
    times, nodes, jobs = get_usage(data, res=800)
    make_plot(title, times, nodes, name, jobs, color, key_len, mapping)
    node_hours = get_nodehours(data)
    print(node_hours)
    elapsed = get_elapsed(data, key_len, mapping)
    print(elapsed)
