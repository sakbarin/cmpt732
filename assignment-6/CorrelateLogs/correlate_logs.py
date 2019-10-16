from pyspark import SparkConf, SparkContext
import sys
import json

assert sys.version_info >= (3, 5) # make sure we have Python 3.5+


def main(inputs):
    input_text = sc.textFile(inputs)


if __name__ == '__main__':
    conf = SparkConf().setAppName('CorrelateLogsPy')
    sc = SparkContext(conf=conf)
    sc.setLogLevel('WARN')
    assert sc.version >= '2.4'  # make sure we have Spark 2.4+
    inputs = sys.argv[1]
    main(inputs)
