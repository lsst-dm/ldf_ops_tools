Usage
=====

Overview
--------

Set of tools for gathering and illustrating **lsst-dev** node utilization:

``usage.py``
    A script for gathering data by querying `SLURM`_ database on the cluster
    and then plotting the node-usage data while also returning the total
    node-hours used and the node-hours for each code. Uses functions from
    ``data.py`` and ``output.py``. 

``extract.py``
    Function module that contains all functions for ``usage.py`` involving
    collecting the slurm data and preparing the data for the functions in 
    output.py and process.py.

``process.py``
    Function modules that contains all processing codes, such as those for
    calculating the node usage histograms and node-hours.

``output.py``
    Function module that contains all functions for ``usage.py`` involving 
    the plotting of the node utilization data.

Original code by Mikolaj Kowalik. Modified by Samantha Thrush.


Prerequisites
-------------

This script was tested with Python 3.6.4.

Installation
------------

There is no installation process. Just put the script and function modules in
the directory of your choice.

Tutorial
--------

Gather & plot data
^^^^^^^^^^^

To gather information about cluster utilization by user **mxk** and then
plot it, run the following code:

.. code-block:: bash 

   ./usage.py -u mxk > nodehour_elapsed.out

Besides making the usage.png plot, the script will also return the total
node-hours used by SLURM on the jobs whose data is being plotted, as well as
the node-hours for each type of code. We caught them in the nodehour.out file.

You can find out other supported command-line options by running:

.. code-block:: bash

   ./usage.py -h

or equivalently

.. code-block:: bash

   ./usage.py --help

There are examples of mapping files and plots included in the **examples** 
subdirectory.  There is also a script file that shows more usage cases.

Advanced Plotting
^^^^^^^^^^^^^^^^^
In the same directory where the data file usage.out resides, run the following
in the command line:

.. code-block:: bash

   ./usage.py -j '125653,125654,125522,125588,125590' -f '125653,125654' -t 'thrush node usage' -n usage_thrush -c -m mapping.txt -r 8000 > nodehour_elapsed.out

This creates a plot illustrating **lsst-dev** cluster usage (in PNG format)
based on data from the SLURM accounting database. It also outputs basic
statistics (total node-hour usage and node-hours for each code type) to
standard output (caught here with nodehour_elapsed.out).  Below is an
explanation of all of the possible options to use with usage.py.

**-u** list
    specifies user(s) to gather data for (ex: -u 'mxk,thrush'); conflicts with
    **-j**.

**-j** list 
    specifies job id(s) to gather data for(ex: -j '108408,108425,108426'); 
    conflicts with **-u**

**-f** list
    specifies job id(s) that have failed that should still be included in the
    plot(ex: -f '125653,125654').  Can be used with just **-j**. This argument
    is only needed if there are failed jobs in the list of jobs to be plotted.

**-t** string
    specifies the plot title (completely arbitrary); enclose in quotes if
    contains spaces

**-n** filename
    specifies name of the .png file that will be made

    **DO NOT** include ".png" at the end of the string for this argument!

**-c**
    specifies if you would like the plots color-coded by the SLURM jobNames
    that you have assigned.

    If you would prefer to not have the plots color-coded, then don't include the
    '-c' option. 

**-m** textfile name
    specifies the text file that will hold the python dictionary matching the
    SLURM job name prefixes to the codes used.  The value for each key must be
    one of the following: **singleFrame, mosaic, coadd, multiband, quick, 
    skyCorrection and forc**. For example, if all of your SLURM jobs are from
    singleFrameDriver.py and the jobnames start with "Wi" or "Co", then the
    text file should contain: {"Wi": "singleFrame", "Co": "singleFrame"}.
    The keys must be unique and are case dependent. All keys and values must
    be enclosed within double quotes, as is shown below. Do not include a key
    called "un"; that is reserved for the "unknown" classification.

    If **-m** is omitted, the following mapping will be used: 

    **{"Wi": "singleFrame", "un": "unknown", "Co": "singleFrame", 
    "mo": "mosaic", "co": "coadd", "mt": "multiband"}.**

    See the **examples** folder for two examples of a mapping text file: one
    called "**mapping.txt**" that was used for the S17B_ HSC PDR1 reprocessing
    node useage plot, and the other called "**mapping2.txt**" which is used for 
    the example script usage_RC2.sh (also found in the **examples** folder).

**-r** integer
    specifies how many time bins the node utilization plot data will be sorted
    into. 
 
    If you do not include this option, the resolution will be set to 800.
     
.. Links

.. _SLURM: https://slurm.schedmd.com/quickstart.html
.. _S17B: https://confluence.lsstcorp.org/display/DM/S17B+HSC+PDR1+reprocessing
