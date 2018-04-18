import matplotlib
matplotlib.use('Agg')
import matplotlib.pyplot as plt


def make_plot(title, times, nodes, name, job_list, color):
    """Plots the number of nodes vs. time.

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
    """
    plt.figure(figsize=(8, 8))
    plt.grid(linestyle=':')

    if color:
        plt.plot(times, nodes, 'k', alpha=0.25, drawstyle="steps-post")
        start_end = get_first_last(job_list)
        colors = {'singleFrame': 'c', 'mosaic': 'xkcd:yellow', 'coadd': 'g',
                  'multiband': 'b', 'unknown': 'r', 'forc': 'xkcd:purple',
                  'quick': 'xkcd:orange', 'skyCorrection': 'm'}
        hatch = {'singleFrame': '///', 'mosaic': '\\\\', 'coadd': '...',
                 'multiband': 'x', 'unknown': '**', 'forc': 'oo',
                 'quick': '++', 'skyCorrection': '.*o'}

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
            names.setdefault(job, set()).add(idx)

    # Removes repeated index values for each key and sorts the values into
    # ascending order.
    data_final = {k: sorted(v) for k, v in names.items()}

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
