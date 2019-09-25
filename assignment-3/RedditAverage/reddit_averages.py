from pyspark import SparkConf, SparkContext
import sys

assert sys.version_info >= (3, 5) # make sure we have Python 3.5+

# add more functions as necessary

def main(inputs, output):
    print("nothing")
    # main logic starts here

if __name__ == '__main__':
    conf = SparkConf().setAppName('RedditAveragePy')
    sc = SparkContext(conf=conf)
    sc.setLogLevel('WARN')
    assert sc.version >= '2.4'  # make sure we have Spark 2.4+
    inputs = sys.argv[1]
    output = sys.argv[2]
    main(inputs, output)
