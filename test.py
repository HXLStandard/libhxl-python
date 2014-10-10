from hxl.parser import hxlreader
import pprint
import sys

reader = hxlreader(sys.stdin)

for row in reader:
    print(row)
