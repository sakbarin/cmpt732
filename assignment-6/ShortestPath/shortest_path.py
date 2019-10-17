import sys
assert sys.version_info >= (3, 5) # make sure we have Python 3.5+

# initial config
from pyspark.sql import SparkSession, functions, types
spark = SparkSession.builder.appName('ShortestPathDF').getOrCreate()
assert spark.version >= '2.4' # make sure we have Spark 2.4+
spark.sparkContext.setLogLevel('WARN')
sc = spark.sparkContext


def main(inputs, outputs, source_node, dest_node):
    pass

if __name__ == '__main__':
    assert sc.version >= '2.4'  # make sure we have Spark 2.4+
    inputs = sys.argv[1]
    outputs = sys.argv[2]
    source_node = int(sys.argv[3])
    dest_node = int(sys.argv[4])

    main(inputs, outputs, source_node, dest_node)
