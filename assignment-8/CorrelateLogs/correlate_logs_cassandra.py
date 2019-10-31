import sys, math
assert sys.version_info >= (3, 5) # make sure we have Python 3.5+

from pyspark.sql import SparkSession, functions
from cassandra.cluster import Cluster

# initial config
cluster_seed = ['199.60.17.32', '199.60.17.65']
spark = SparkSession.builder.appName('CorrelateLogsCassandra') \
                .config('spark.cassandra.connection.host', ','.join(cluster_seed)).getOrCreate()

assert spark.version >= '2.4' # make sure we have Spark 2.4+
spark.sparkContext.setLogLevel('WARN')
sc = spark.sparkContext


def calc_r(input):
    (n, x, x_sq, y, y_sq, xy) = input

    numerator = (n * xy) - (x * y)
    denominator1 = math.sqrt(n * x_sq - (x * x))
    denominator2 = math.sqrt(n * y_sq - (y * y))

    return (numerator / (denominator1 * denominator2))


def main(keyspace, table):
    df = spark.read.format("org.apache.spark.sql.cassandra") \
                        .options(table=table, keyspace=keyspace).load()


    df_base = df.groupBy(df['host']) \
                .agg(functions.count(df['host']).alias('x'), functions.sum(df['bytes']).alias('y'))


    df_calc = df_base.select(df_base['host'], \
                             df_base['x'], \
                             (df_base['x'] * df_base['x']).alias('x_sq'), \
                             df_base['y'], \
                             (df_base['y'] * df_base['y']).alias('y_sq'), \
                             (df_base['x'] * df_base['y']).alias('xy'))


    df_sigma = df_calc.select(functions.count(df_calc['host']).alias('n'), \
                              functions.sum(df_calc['x']).alias('x'), \
                              functions.sum(df_calc['x_sq']).alias('x_sq'), \
                              functions.sum(df_calc['y']).alias('y'), \
                              functions.sum(df_calc['y_sq']).alias('y_sq'), \
                              functions.sum(df_calc['xy']).alias('xy'))


    rdd_final = df_sigma.rdd.flatMap(tuple).collect()
    r = calc_r(rdd_final)


    print("\n\n\n---------------------------")
    print("r: %f , r^2: %f" % (r, math.pow(r, 2)))
    print("---------------------------\n\n")


if __name__ == '__main__':
    keyspace = sys.argv[1]
    table = sys.argv[2]

    main(keyspace, table)
