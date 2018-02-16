#!/usr/bin/env python

"""Gather data regarding lsst-dev usage.

Script uses SLURM's sacct command to collect data regarding cluster
utilization, i.e. number of nodes, used either by given users or jobs.
"""

import argparse
import datetime
from subprocess import PIPE, Popen


def create_parser():
    """Create command line parser.
    """
    p = argparse.ArgumentParser()
    g = p.add_mutually_exclusive_group()
    g.add_argument('-u', '--users', type=str, default=None,
                   help='Comma separated list of users to consider, '
                        'if omitted jobs of all users will be used.')
    g.add_argument('-j', '--jobs', type=str, default=None,
                   help='Comma separated list of jobs to consider, '
                        'if omitted jobs of all users will be used.')
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

    # Show only cumulative statistics for each job (no steps).
    argv.append('--allocations')

    # Specify what data should be displayed.
    fields = ['user', 'jobid', 'jobname', 'nnodes', 'submit', 'start', 'end']
    argv.append('--format={}'.format(','.join(fields)))

    # Specify either jobs or users for which the data will be gathered.
    if args.jobs is None:
        if args.users is None:
            argv.append('--allusers')
        else:
            argv.append('--user={}'.format(args.users))
    else:
        argv.append('--jobs={}'.format(args.jobs))

    # Include only completed jobs.
    argv.append('--state=CD')

    # Output data in a format easy to parse (here: CSV).
    argv.extend(['--delimiter=,', '--noheader', '--parsable2'])

    # Execute the command.
    proc = Popen(argv, stdout=PIPE, stderr=PIPE, encoding='utf-8')
    stdout, stderr = proc.communicate()
    if stderr != '':
        print(stderr)
        raise OSError('failed to gather data.')

    lines = stdout.split('\n')[:-1]
    data = []
    for line in lines:
        values = line.strip().split(',')
        data.append({field: value for field, value in zip(fields, values)})
    return data


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
    t, u : `list` of numbers
        Lists representing midpoints of time intervals and corresponding number
        of nodes used either by given jobs or users in these intervals.
    """
    begin = min(rec['start'] for rec in data)
    end = max(rec['end'] for rec in data)

    duration = (end - begin).total_seconds()
    step = duration / res

    # Express time relative to the beginning of the first job.
    start_times = [(rec['start'] - begin).total_seconds() for rec in data]
    end_times = [(rec['end'] - begin).total_seconds() for rec in data]

    node_counts = [int(rec['nnodes']) for rec in data]

    # Make a histogram representing number of used nodes in a given time
    # interval.
    usage = [0] * res
    for begin, end, nodes in zip(start_times, end_times, node_counts):
        i, j = int(begin / step), int(end / step)
        for k in range(i, j):
            usage[k] += nodes
    times = [step * (i + 0.5) for i in range(len(usage))]
    return times, usage


def convert_times(data):
    """Convert times to `datetime` objects for easier manipulation.

    Parameters
    ----------
    data : `list` of `dict`
        Accounting data from slurm.
    """
    fmt = '%Y-%m-%dT%H:%M:%S'
    for datum in data:
        for field in ['end', 'start', 'submit']:
            datum[field] = datetime.datetime.strptime(datum[field], fmt)


if __name__ == '__main__':
    parser = create_parser()
    args = parser.parse_args()
    data = gather_data(args)
    convert_times(data)
    times, usage = get_usage(data)
    for t, u in zip(times, usage):
        print(t, u)
