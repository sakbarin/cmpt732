import sys, math
assert sys.version_info >= (3, 5) # make sure we have Python 3.5+

from pyspark.sql import SparkSession, functions
from cassandra.cluster import Cluster

# initial config
cluster_seed = ['199.60.17.32', '199.60.17.65']
spark = SparkSession.builder.appName('tpch_denormalize') \
                .config('spark.cassandra.connection.host', ','.join(cluster_seed)) \
                .config('spark.dynamicAllocation.maxExecutors', 16).getOrCreate()
clusters = Cluster(cluster_seed)

assert spark.version >= '2.4' # make sure we have Spark 2.4+
spark.sparkContext.setLogLevel('WARN')
sc = spark.sparkContext


# truncate table data
TRUNCATE = True


# truncate table data
def truncate_table(session, table):
    if (TRUNCATE):
        session.execute("TRUNCATE " + table)


def get_df_for_table(table_name, keyspace):
    return spark.read.format("org.apache.spark.sql.cassandra") \
                     .options(table=table_name, keyspace=keyspace).load()


def output_line(input_rdd):
    orderkey, price, names = input_rdd
    namestr = ', '.join(sorted(list(names)))
    return 'Order #%d $%.2f: %s' % (orderkey, price, namestr)


def main(input_keyspace, output_keyspace):
    # truncate output keyspace
    session_output = clusters.connect(output_keyspace)
    truncate_table(session_output, "orders_parts")


    # read input data frames
    df_orders = get_df_for_table("orders", input_keyspace)
    df_lineitem = get_df_for_table("lineitem", input_keyspace)
    df_part = get_df_for_table("part", input_keyspace)


    # join dataframes together
    df_joined = df_orders.join(df_lineitem, df_orders['orderkey'] == df_lineitem['orderkey'], 'inner') \
                     .join(df_part, df_lineitem['partkey'] == df_part['partkey'], 'inner') \
                     .select(df_orders['*'], df_part['name'])


    # group data and collect parts
    exclude_columns = ['name']
    df_final = df_joined.groupBy([col for col in df_joined.columns if col not in exclude_columns]) \
                        .agg(functions.collect_set(df_joined['name']).alias('part_names'))


    df_final.write.format("org.apache.spark.sql.cassandra").options(table="orders_parts", keyspace=output_keyspace).save()


if __name__ == '__main__':
    input_keyspace = sys.argv[1]
    output_keyspace = sys.argv[2]

    main(input_keyspace, output_keyspace)
