#!/usr/bin/python
"""
   Data Organizer Test Code

"""
import json

from DataOrganizer import CouchbaseSelector

db = CouchbaseSelector('localhost', 'nem-data')


# --  get_count

print "\nTest: get_count: no selector\n"

count = db.get_count()
print "  count = "+str(count)

# -- get_count

print "\nTest: get_count: parameters\n"

selector = { 'type': 'parameter' }
count = db.get_count(selector)
print "  count = "+str(count)

# -- get_parameter_count

print "\nTest: get_parameter_count:\n"

parameter_count = db.get_parameter_count('E')
print "  parameter_count 'E' = "+str(parameter_count)

parameter_count = db.get_parameter_count('unknown')
print "  parameter_count 'unknown' = "+str(parameter_count)

# -- get_parameter

parameter = 'E'
fields = ['value', 'value_unit']
selector = {'data_source': 'laboratory'}
sort = []
print "\nTest:\n"
for rec in db.get_parameter(parameter, fields, selector, sort):
  print json.dumps(rec)

print "\nTest:\n"
for rec in db.get_parameter(parameter, selector=selector):
  print json.dumps(rec)


