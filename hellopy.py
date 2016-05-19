from __future__ import print_function

import sys
from operator import add
from pyspark import SparkContext

if __name__ == "__main__":
  sc = SparkContext(appName="PythonWordCountCygwin")
  print(sc.version)
  sc.stop()
