"""

    Read CSV files, 

    The first line is assumed to contain a row of field names
    The following lines contains values for the rows

    Returns an array of rows, where each row is a dictionary of fields

"""

import re
import csv
import json


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
    CSV Data file reader
"""

class CsvDataReader:

    file_name = None
    header = None
    data = None
    
    def __init__(self, file_name):
        self.file_name = file_name 

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
        data.append(record)
        return data

    def _add_sub_field(self, record, field_name, offset, sub_field_name, value):
        #print "---------------------"
        #print "_add_sub_field: START"
        #print "---------------------"
        #print "  field_name: "+field_name 
        #print "  offset : "+str(offset)
        #print "  sub_field_name : "+sub_field_name 
        #print "  value: "+value
        #print "  record: "+json.dumps(record,indent=2)

        if not field_name in record:
            #print " --> Adding SparseList.."
            record[field_name] = SparseList()
            record[field_name][offset] = {}

        if record[field_name][offset] is None:
            #print " --> Adding Dictionary.."
            record[field_name][offset] = {}

        #print "  record "+json.dumps(record,indent=2)
        record[field_name][offset][sub_field_name] = value
        return record

