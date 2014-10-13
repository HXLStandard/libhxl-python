"""
Writing library for the Humanitarian Exchange Language (HXL) v1.0
David Megginson
October 2014

License: Public Domain
Documentation: http://hxlstandard.org
"""

import csv

class HXLWriter:
    """
    Write HXL data to a file
    """

    def __init__(self, stream):
        self.csvwriter = csv.writer(stream)

    def writeHeaders(self, row):
        headers = []
        for value in row:
            headers.append(value.column.headerText)
        self.csvwriter.writerow(headers)

    def writeTags(self, row):
        tags = []
        for value in row:
            tags.append(value.column.hxlTag)
        self.csvwriter.writerow(tags)

    def writeData(self, row):
        row_out = []
        for value in row:
            row_out.append(value.content)
        self.csvwriter.writerow(row_out)
