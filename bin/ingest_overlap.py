#!/usr/bin/env python

""" Code to ingest information about image and patch overlap in to CCD_OVERLAP_PATCH table """

import sys
import re
import argparse
import sqlite3

from despydb import desdbi
from despymisc import miscutils


oldpatchsep = ','
newpatchsep = '_'

overlap_table = 'CCD_OVERLAP_PATCH'

# current sqlite3 schema
#CREATE TABLE calexp (id integer primary key autoincrement, visit int,patch str,exist int,ra float,tract int,ccd int,dec float);

# current oracle schema
#create table overlap (TRACT NUMBER(5) not null, PATCH VARCHAR2(10) not null,VISIT NUMBER(10) not null,CCD NUMBER(3) not null,VERSION VARCHAR2(3) not null,SKYMAP_FILENAME VARCHAR2(100) not null);


def parse_args(argv):
    """ Parse command line arguments """
    parser = argparse.ArgumentParser(
        description='Ingest overlap information from an sqlite3 file into an oracle table')
    parser.add_argument('--des_services', action='store')
    parser.add_argument('--des_db_section', '--section', '-s', action='store', dest='section')
    parser.add_argument('--version', required=True, action='store')
    parser.add_argument('--skymap', required=True, action='store', dest='fnskymap')
    parser.add_argument('--sqlite3', action='store', dest='fnsqlite3')
    parser.add_argument('--csv', action='store', dest='fncsv')
    parser.add_argument('--delim', action='store', default=';')

    argsd = vars(parser.parse_args(argv))   # convert dict
    return argsd


def insert_row_oracle(ocurs, ovals):
    #print ovals
    okeys = ovals.keys()
    sql = 'insert into %s (tract, patch, visit, ccd, version, skymap_filename) values (:tract, :patch, :visit, :ccd, :version, :skymap)' % (
        overlap_table)
    ocurs.execute(sql, ovals)


def process_sqlite3(scurs, version, fnskymap, ocurs):
    #sdbh = sqlite3.connect(fnsqlite3+'?mode=ro')
    sql = 'select * from calexp'
    scurs.execute(sql)
    desc = [d[0].lower() for d in scurs.description]

    for line in scurs:
        svals = dict(zip(desc, line))
        if svals['exist'] != 0:
            #print svals
            newpatch = re.sub(oldpatchsep, newpatchsep, svals['patch'])
            ovals = {'tract': svals['tract'],
                     'patch': newpatch,
                     'visit': svals['visit'],
                     'ccd': svals['ccd'],
                     'version': version,
                     'skymap': fnskymap}
            insert_row_oracle(ocurs, ovals)
        else:
            print "Skipping %s %s %s %s" % (svals['tract'], svals['patch'], svals['visit'], svals['ccd'])


def check_sqlite3(scurs):
    sql = "select count(*) from calexp where exist = 1"
    scurs.execute(sql)
    cnt = scurs.fetchone()[0]
    print "%s rows in calexp table with exist = 1" % (cnt)


def check_oracle(ocurs, version):
    sql = "select count(*) from %s where version = '%s'" % (overlap_table, version)
    ocurs.execute(sql)
    cnt = ocurs.fetchone()[0]
    print "%s rows in overlap table with version = %s" % (cnt, version)


def process_csv(fncsv, delim, version, fnskymap, ocurs):
    datacnt = 0
    with open(fncsv) as infh:
        # first line has headers
        line = infh.readline()
        headers = miscutils.fwsplit(line, delim)
        print headers

        # read data
        line = infh.readline()
        while line:
            line = line.split('#')[0]
            line = line.strip()

            data = miscutils.fwsplit(line, delim)
            svals = dict(zip(headers, data))
            newpatch = re.sub(oldpatchsep, newpatchsep, svals['patch'])
            ovals = {'tract': svals['tract'],
                     'patch': newpatch,
                     'visit': svals['visit'],
                     'ccd': svals['ccd'],
                     'version': version,
                     'skymap': fnskymap}
            datacnt += 1
            insert_row_oracle(ocurs, ovals)
            line = infh.readline()
    print datacnt, "lines from csv"


def main(argv):
    argsd = parse_args(argv)

    odbh = desdbi.DesDbi(argsd['des_services'], argsd['section'])
    ocurs = odbh.cursor()

    if argsd['fnsqlite3'] is not None:
        sdbh = sqlite3.connect(argsd['fnsqlite3'])
        scurs = sdbh.cursor()
        check_sqlite3(scurs)
        process_sqlite3(scurs, argsd['version'], argsd['fnskymap'], ocurs)
    else:
        process_csv(argsd['fncsv'], argsd['delim'], argsd['version'], argsd['fnskymap'], ocurs)

    check_oracle(ocurs, argsd['version'])
    #odbh.rollback()
    odbh.commit()


if __name__ == '__main__':
    main(sys.argv[1:])
