from hxl.parser import HXLReader
import pprint
import sys

reader = HXLReader(sys.stdin)

for row in reader:
    for value in row:
        print(value)

