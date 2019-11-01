import sys
assert sys.version_info >= (3, 5) # make sure we have Python 3.5+

from pyspark.sql import SparkSession, functions
from cassandra.cluster import Cluster

# initial config
cluster_seed = ['199.60.17.32', '199.60.17.65']
spark = SparkSession.builder.appName('tpch_orders_denorm') \
                .config('spark.cassandra.connection.host', ','.join(cluster_seed)) \
                .config('spark.dynamicAllocation.maxExecutors', 16).getOrCreate()

assert spark.version >= '2.4' # make sure we have Spark 2.4+
spark.sparkContext.setLogLevel('WARN')
sc = spark.sparkContext


def get_df_for_table(table_name, keyspace):
    return spark.read.format("org.apache.spark.sql.cassandra") \
                     .options(table=table_name, keyspace=keyspace).load()


def output_line(input_rdd):
    orderkey, price, names = input_rdd
    namestr = ', '.join(sorted(list(names)))
    return 'Order #%d $%.2f: %s' % (orderkey, price, namestr)


def main(keyspace, output_directory, order_keys):
    # get data frame from cassandra
    df = get_df_for_table("orders_parts", keyspace)

    # filter data
    df_filtered = df.where(df["orderkey"].isin(order_keys)) \
                    .select(df["orderkey"], df["totalprice"], df["part_names"]) \
                    .orderBy(df["orderkey"])

    # convert to rdd
    rdd_results = df_filtered.rdd

    # apply output format function to all rows
    rdd_output = rdd_results.map(output_line)

    # write to output directory
    rdd_output.coalesce(1).saveAsTextFile(output_directory)


if __name__ == '__main__':
    keyspace = sys.argv[1]
    output_directory = sys.argv[2]
    order_keys = sys.argv[3:]

    main(keyspace, output_directory, order_keys)
