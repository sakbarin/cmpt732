import sys, math
assert sys.version_info >= (3, 5) # make sure we have Python 3.5+

from pyspark.sql import SparkSession, functions
from cassandra.cluster import Cluster

# initial config
cluster_seed = ['199.60.17.32', '199.60.17.65']
spark = SparkSession.builder.appName('tpch_orders_df') \
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
    df_orders = get_df_for_table("orders", keyspace)
    df_lineitem = get_df_for_table("lineitem", keyspace)
    df_part = get_df_for_table("part", keyspace)

    # join dataframes together
    df_joined = df_orders.join(df_lineitem, df_orders['orderkey'] == df_lineitem['orderkey'], 'inner') \
                         .join(df_part, df_lineitem['partkey'] == df_part['partkey'], 'inner') \
                         .select(df_orders['orderkey'], df_orders['totalprice'], df_part['name'])

    # get order keys sent via input
    df_filtered = df_joined.where(df_joined['orderkey'].isin(order_keys))

    # order data
    df_sorted = df_filtered.orderBy(df_filtered['orderkey'])

    # group data and collect parts
    df_final = df_sorted.groupBy(df_sorted['orderkey'], df_sorted['totalprice']) \
                        .agg(functions.collect_set(df_sorted['name']).alias('partnames'))

    # explain plan
    df_final.explain()

    # convert to rdd
    rdd_results = df_final.rdd

    # apply output format function to all rows
    rdd_outpt = rdd_results.map(output_line)

    # write to output directory
    rdd_outpt.coalesce(1).saveAsTextFile(output_directory)


if __name__ == '__main__':
    keyspace = sys.argv[1]
    output_directory = sys.argv[2]
    order_keys = sys.argv[3:]

    main(keyspace, output_directory, order_keys)
