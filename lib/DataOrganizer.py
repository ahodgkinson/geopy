"""
  Data Organizer Module

"""

import json

from couchbase.bucket import Bucket
from couchbase.n1ql import N1QLQuery

class CouchbaseSelector:

  bucket = None
  url = None
  cb = None

  debug = False

  def __init__(self, server, bucket):
    self.bucket = bucket
    self.url = 'couchbase://'+server+'/'+bucket
    self.cb = Bucket(self.url)

  def setDebug(self, debug):
    self.debug = debug
    
  def get(self, key):
    if self.debug:
      print "get: key: "+key

    if self.cb is None:
      raise IOError("Database not opened")
    else:
      return self.cb.get(key).value

  def select(self, fields, where):
    if self.debug:
      print "select: fields: "+str(fields)
      print "select: where: "+str(where)

    if self.cb is None:
      raise IOError("Database not opened")

    fields_str = self._get_fields(fields)
    where_str  = self._get_where(where)

    sql = 'select '+fields_str+' from `'+self.bucket+'` '+where_str

    if self.debug:
      print "select: sql: "+sql

    query = N1QLQuery(sql)
    return self.cb.n1ql_query(query)

  # -- Helper function: G

  def _get_fields(self, fields):
    if fields is None or len(fields) < 1:
      return '*'
    else:
      return '`'+'`,`'.join(fields)+'`'

  def _get_where(self, where):
    if where is None or len(where) < 1:
      return ''
  
    if not isinstance(where, dict):
      raise TypeError("invalid where clause: "+str(where))
  
    where_parts = []
    for field in where.keys():
      where_parts.append(self._get_where_part(field, where.get(field)))
    return "where "+" and ".join(where_parts)
  
  def _get_where_part(self, field, rhs): # Returns "field operator operand", e.g `a` = 27
    return '`'+field+'` '+self._get_where_rhs(rhs)
  
  def _get_where_rhs(self, rhs):	# Return right-hand-side of where clause expression, 
    if isinstance(rhs, int) or isinstance(rhs,float): # A number
      return "= "+str(rhs)
    if isinstance(rhs, str): # A string, quote (& escape) the string
      return "= "+json.dumps(rhs)
  
    if not isinstance(rhs, dict) or len(rhs) != 1: # Expecting a 1-item dict
      raise ValueError("Invalid sub-clause: "+str(rhs))
  
    operators = { # Key is 'op_code', Value is 'operator'
      '$eq': '=', '$ne': '!=', '$gt': '>', '$gte': '>=', '$le': '<', '$lte': '<='
    }
  
    op_code = rhs.keys()[0]
    operand = rhs.get(op_code)
    operator = operators.get(op_code)
  
    if operator is None: raise ValueError("Invalid operator: "+str(rhs))
    if operand is None: raise ValueError("Invalid operand: "+str(rhs))
  
    if isinstance(operand, str):
       operand = json.dumps(operand)
    else:
       operand = str(operand)
    return operator+' '+operand

