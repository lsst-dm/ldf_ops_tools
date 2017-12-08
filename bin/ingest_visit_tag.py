#!/usr/bin/env python

"""  Reads a text file containing visit ccd pairs and inserts them into 
     the visit_tag table """


# TODO:  Check if tag exists in ops_valid_visit_tag (or catch exeception). If not give
#        nicer error message than the: cx_Oracle.IntegrityError: ORA-02291: integrity
#            constraint (PRODBETA.VISIT_TAG_FK) violated - parent key not found
# TODO:  Maybe.  Better handling of when camsym visit ccd tag already exist in table
#        Currently get generic unique constraint violation message
#        Traceback (most recent call last):
#          File "./create_visit_tag.py", line 33, in <module>
#            curs.execute(None, {'camsym': args.camsym, 'visit':visit, 'ccd':ccd, 'tag': args.tag})
#        cx_Oracle.IntegrityError: ORA-00001: unique constraint (PRODBETA.VISIT_TAG_PK) violated
# TODO:  Either make separate tool or allow this tool to make entry in ops_valid_visit_tag table
#        (both codes would need tag, description, docurl information in order to insert into table

import argparse
import sys

from despydb import desdbi
from despymisc import miscutils


# parse command line
parser = argparse.ArgumentParser(description='Insert visit+ccd pairs into visit_tag table')
parser.add_argument('--services', action='store')
parser.add_argument('--section', action='store')
parser.add_argument('--camsym', required=True, action='store',
                    help='Camera Symbol that makes visit number unique across cameras')
parser.add_argument('--tag', required=True, action='store',
                    help='Tag must already exist in ops_valid_visit_tag table')
parser.add_argument('filename', nargs=1, action='store')
args = parser.parse_args(sys.argv[1:])

# make db connection
dbh = desdbi.DesDbi(args.services, args.section)


sql = 'insert into visit_tag (camsym, visit, ccd, tag) values (%s, %s, %s, %s)' % \
    (dbh.get_named_bind_string('camsym'), dbh.get_named_bind_string('visit'),
     dbh.get_named_bind_string('ccd'), dbh.get_named_bind_string('tag'))

curs = dbh.cursor()
curs.prepare(sql)
cnt = 0

# loop through file inserting rows into visit_tag table
print("Reading visit + ccd values from %s" % args.filename)
with open(args.filename[0], 'r') as infh:
    for line in infh:
        (visit, ccd) = miscutils.fwsplit(line, ' ')
        curs.execute(None, {'camsym': args.camsym, 'visit': visit, 'ccd': ccd, 'tag': args.tag})
        cnt += 1

print("Ingested %d rows into visit_tag for tag=%s" % (cnt, args.tag))
dbh.commit()
