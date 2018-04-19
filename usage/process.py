import numpy as np


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

def get_codehours(data):
    """Finds the elapsed node-hours for each code (ie, how much time it took to
    complete all coadd jobs multiplied by the nodes used, etc.).

    Parameters
    ----------
    data : `list` of `dict`
        accounting data from slurm.

    Returns
    -------
    code_nodehours : `dict` of `str` keys and `float` values
        dictionary containing the code names and their associated node-hours
        on SLURM in hours*nodes
    """
    # Find duration of each job & nodes used
    duration = [(rec['end'] - rec['start']).total_seconds() for rec in data]
    nodes = [int(rec['nnodes']) for rec in data]
    job_type = [rec['jobname'] for rec in data]

    # Return elapsed_times
    code_nodehours = dict.fromkeys(job_type, 0.0)
    for dur, name, node in zip(duration, job_type, nodes):
        code_nodehours[name] += dur*node
    return {key: round(val/3600.0, 2) for key, val in code_nodehours.items()}

