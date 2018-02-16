Usage
=====

Overview
--------

Set of tools for gathering and illustrating **lsst-dev** node utilization:

``usage.py``
    A script for gathering data by querying `SLURM`_ database on the cluster.
    Created by Mikolaj Kowalik. 

``usageplot.py``
    A python code for plotting the data.
    Created by Samantha Thrush.

Prerequisites
-------------

Data gathering script was tested with Python 3.6.4.

Installation
------------

There is no installation process. Just put the scripts in directory of your
choice.

Tutorial
--------

Gather data
^^^^^^^^^^^

To gather information about cluster utilization by user **mxk** and save the
results in file ``usage.out`` run:

.. code-block:: bash 

   ./usage.py -u mxk > usage.out

You can find out other supported command-line options by running:

.. code-block:: bash

   ./usage.py -h

or equivalently

.. code-block:: bash

   ./usage.py --help

Plot data
^^^^^^^^^

In the same directory where the data file usage.out resides, run the following in the command line:

.. code-block:: bash

   python usageplot.py -t w_2018_03 -d usage.out -n usage_w_2018_03

The -t argument is the plot title (completely arbitrary), the -d argument is the name of the text file with the data (created above with usage.py) and the -n argument is the name of the .png file that will be made with usageplot.py (note that you should NOT include ".png" at the end of the -n argument!!).

If you would like to know more about the arguments above, just run the following on the command line:

.. code-block:: bash

   python usageplot.py -h

or alternately:

.. code-block:: bash

   python usageplot.py --help

After you have run usageplot, you should see a new .png file in your current directory.  

There are examples of data files and plots included in the examples subdirectory.  

.. Links

.. _SLURM: https://slurm.schedmd.com/quickstart.html
