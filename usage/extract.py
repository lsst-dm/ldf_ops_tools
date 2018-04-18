import re
import datetime
from subprocess import PIPE, Popen


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
    failed = []
    if args.jobs is None:
        if args.users is None:
            argv.append('--allusers')
        else:
            argv.append('--user={}'.format(args.users))
    else:
        jobs = args.jobs
        if args.failed is not None:
            jobs = ','.join([jobs, args.failed])
            failed = args.failed.split(',')
        argv.append('--jobs={}'.format(jobs))

    # Include completed jobs, those terminated due to node failures as they
    # could be succesfully compeleted after being rescheduled, and failed jobs
    # that have been specifically included.
    argv.append('--state=CD,NF,F')

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
    return list(job for job in jobs.values() if job['state'] == 'COMPLETED' or job['jobid'] in failed)


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


def convert_names(data, mapping):
    """Convert names from JobNames to their code names.

    Parameters
    ----------
    data : `list` of `dict`
        accounting data from slurm.
    mapping : `dict`
        dictionary mapping jobName prefixes to code names
    """
    # Extract mapping keys for matching
    cores = mapping.keys()
    for datum in data:
        name = datum['jobname']

        # See if name matches any keys
        matches = [re.match(core, name) for core in cores]
        matches = [match[0] for match in matches if match]

        # Find new name or throw error if too many matches
        if len(matches) == 0:
            code = 'unknown'
        elif len(matches) == 1:
            code = mapping[matches[0]]
        else:
            msg = 'ERROR: Ambiguous mapping: ' \
                  'following keys "%s" can be mapped to "%s".' % \
                  (', '.join(match for match in matches), name)
            raise RuntimeError(msg)

        # Assign new code name
        datum['jobname'] = code
