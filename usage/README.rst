Usage
=====

Overview
--------

Set of tools for gathering and illustrating **lsst-dev** node utilization:

``usage.py``
    A script for gathering data by querying `SLURM`_ database on the cluster
    and then plotting the node-usage data while also returning the total
    node-hours used and elapsed run times for each code.  
    Original code by Mikolaj Kowalik. 
    Modified by Samantha Thrush.

Prerequisites
-------------

This script was tested with Python 3.6.4.

Installation
------------

There is no installation process. Just put the scripts in directory of your
choice.

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
the  elapsed run time for each type of code (in hours). We caught them in the
nodehour.out file.

You can find out other supported command-line options by running:

.. code-block:: bash

   ./usage.py -h

or equivalently

.. code-block:: bash

   ./usage.py --help

There are examples of data files and plots included in the examples subdirectory.  

Advanced Plotting
^^^^^^^^^^^^^^^^^
In the same directory where the data file usage.out resides, run the following
in the command line:

.. code-block:: bash

   ./usage.py -u mxk -t 'mxk node usage' -n usage_mxk -c > nodehour_elapsed.out

This creates a plot illustrating **lsst-dev** cluster usage (in PNG format)
based on data from the SLURM accounting database. It also outputs basic
statistics (node-hour usage and elapsed runtimes for each code type) to
standard output (caught here with nodehour_elapsed.out).  Below is an
explanation of all of the possible options to use with usage.py.

**-u** comma separated list enclosed in single quotes without spaces
    (ex: -u 'mxk,thrush')
    specifies user(s) to gather data for; conflicts with **-j**.

**-j** comma separated list enclosed in single quotes without spaces 
    (ex: -j '108408,108425,108426')
    specifies job id(s) to gather data for; conflicts with **-u**

**-t** string enclosed in single quotes
    specifies the plot title (completely arbitrary)

**-n** string
    specifies name of the .png file that will be made
    **DO NOT** include ".png" at the end of the string for this argument!

**-c**
    specifies if you would like the plots color-coded by the SLURM jobNames
    that you have assigned (this will only work if singleFrameDriver.py
    job names start with "Co" or "Wi", mosaic.py job names start with 'mo',
    coaddDriver.py job names start with 'co', and multibandDriver.py job names
    start with 'mt').

    If you would prefer to not have the plots color-coded, then don't include the
    '-c' option. 

.. Links

.. _SLURM: https://slurm.schedmd.com/quickstart.html
