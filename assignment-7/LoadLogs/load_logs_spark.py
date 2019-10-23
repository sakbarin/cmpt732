import sys, re, uuid
from datetime import datetime
from pyspark.sql import SparkSession
from cassandra.cluster import Cluster

assert sys.version_info >= (3, 5) # make sure we have Python 3.5+

# initial config
cluster_seed = ['199.60.17.32', '199.60.17.65']
spark = SparkSession.builder.appName('LoadLogsSpark') \
                .config('spark.cassandra.connection.host', ','.join(cluster_seed)).getOrCreate()

assert spark.version >= '2.4' # make sure we have Spark 2.4+
spark.sparkContext.setLogLevel('WARN')
sc = spark.sparkContext


LINE_RE = re.compile(r'^(\S+) - - \[(\S+) [+-]\d+\] \"[A-Z]+ (\S+) HTTP/\d\.\d\" \d+ (\d+)$')


# drop table if exists, and re-create it
def drop_create_table(session, table):
    session.execute("DROP TABLE IF EXISTS " + table)
    session.execute("CREATE TABLE " + table + " ( host TEXT, id TEXT, datetime TIMESTAMP, path TEXT, bytes INT, PRIMARY KEY (host, id) )")


# separate text input to structured rdd
def separate_columns(line):
    columns = LINE_RE.split(line)
    if (len(columns) == 6):
        HOST = columns[1]
        ID = str(uuid.uuid4()),
        DATETIME = datetime.strptime(columns[2], "%d/%b/%Y:%H:%M:%S")
        PATH = columns[3]
        BYTES = int(columns[4])
        return (HOST, ID, DATETIME, PATH, BYTES)


# main function
def main(inputs, keyspace, table):
    clusters = Cluster(cluster_seed)
    session = clusters.connect(keyspace)
    drop_create_table(session, table)

    logs_text = sc.textFile(inputs)
    logs_repartitioned = logs_text.repartition(128)
    logs_rdd = logs_repartitioned.map(separate_columns)
    logs_valid = logs_rdd.filter(lambda x: x is not None)
    logs_df = spark.createDataFrame(logs_valid, ['host', 'id', 'datetime', 'path', 'bytes'])

    logs_df.write.format("org.apache.spark.sql.cassandra").options(table=table, keyspace=keyspace).save()


# start point
if (__name__ == '__main__'):
    inputs = sys.argv[1]
    keyspace = sys.argv[2]
    table = sys.argv[3]

    main(inputs, keyspace, table)
