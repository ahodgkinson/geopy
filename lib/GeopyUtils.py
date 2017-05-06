"""

    Read CSV files, 

    The first line is assumed to contain a row of field names
    The following lines contains values for the rows

    Returns an array of rows, where each row is a dictionary of fields

"""

import re
import csv
import json

from time import gmtime, strftime
from random import randint


"""
    See: http://stackoverflow.com/questions/1857780/sparse-assignment-list-in-python
"""

class SparseList(list):
    def __setitem__(self, index, value):
        missing = index - len(self) + 1
        if missing > 0:
            self.extend([None] * missing)
        list.__setitem__(self, index, value)
    def __getitem__(self, index):
        try: return list.__getitem__(self, index)
        except IndexError: return None


"""
   Static method IdField.new('table_name')

   Return ID of the form 'table-YYYYMMDD-hhmmss-rrrrrrrr'

   Where 'rrrrrrrr' is a rendom 8-digit string
"""

class IdField:

    @staticmethod
    def new(table_name):
        return "-".join( ( table_name, IdField._timestamp(), IdField._randint(8) ) )

    @staticmethod
    def _timestamp(): # Return 'YYMMDD-hhmmss' timestamp
        return strftime("%Y%m%d-%H%M%S", gmtime())

    @staticmethod
    def _randint(size): # return 'size'-digit random number
        r = ''
        for i in range(0, size):
            r = r + str(randint(0,9))
        return r

"""
    CSV Data file reader
"""

class CsvDataReader:

    file_name = None
    table_name = None
    header = None
    data = None
    
    def __init__(self, table_name, file_name):
        self.file_name = file_name 
        self.table_name = table_name 

    def read(self):
        self.data = []
        reader = csv.reader(open(self.file_name, mode='r'))
        self.header = reader.next()
        for row in reader:
            self.data = self._add_record(self.data, row)
        return self.data
      
    def _add_record(self, data, row):
        record = {}
        i = 0
        for field_name in self.header:
            parts = re.split(r'(\[|\]\.)',field_name)	# e.g. "field[1].sub_field"
            if len(parts) == 1:
                record[field_name] = row[i] 
            else:
                (field_name_base, ignore, offset, ignore, sub_field_name) = parts
                record = self._add_sub_field(record, field_name_base, int(offset), sub_field_name, row[i])
            i = i + 1

        if not 'id' in record:
            record['id'] = IdField.new(self.table_name)

        if not 'type' in record:
            record['type'] = self.table_name

        data.append(record)
        return data

    def _add_sub_field(self, record, field_name, offset, sub_field_name, value):
        if not field_name in record:
            record[field_name] = SparseList()
            record[field_name][offset] = {}

        if record[field_name][offset] is None:
            record[field_name][offset] = {}

        record[field_name][offset][sub_field_name] = value
        return record


