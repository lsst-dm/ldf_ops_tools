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
    
    Returns
    -------
    `argparse.Namespace`
        Namespace with commad line arguments.
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
    times : `list` of `float`
        List representing midpoints of time intervals.
    nodes : `list` of `int`
        List of number of nodes used either by given jobs or users in the
        corresponding time intervals.
    names : `list` of `str`
        List of group of tasks running in the corresponding time intervals.    
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
    # interval.
    usage = [0] * res
    names = [''] * res
    for begin, end, nodes, name in zip(start_times, end_times, node_counts, job_type):
        i, j = int(begin / step), int(end / step)
        for k in range(i, j):
            usage[k] += nodes
            names[k] += name + ','
    names = [s.rstrip(',') for s in names]
    times = [step * (i + 0.5) for i in range(len(usage))]
    return times, usage, names


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
    times, usage, names = get_usage(data, res=400)
    for t, u, n in zip(times, usage, names):
        print(t, u, n)
