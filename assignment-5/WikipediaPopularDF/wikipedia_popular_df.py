import sys
assert sys.version_info >= (3, 5) # make sure we have Python 3.5+

# initial config
from pyspark.sql import SparkSession, functions, types
spark = SparkSession.builder.appName('WikpediaPopularDataFrame').getOrCreate()
assert spark.version >= '2.4' # make sure we have Spark 2.4+
spark.sparkContext.setLogLevel('WARN')
sc = spark.sparkContext

# constants schema
wikipedia_schema = types.StructType([
    types.StructField('language', types.StringType()),
    types.StructField('title', types.StringType()),
    types.StructField('views', types.LongType()),
    types.StructField('size', types.LongType()),
])


@functions.udf(returnType=types.StringType())
def path_to_hour(path):
    file_name_parts = path.split('/')
    file_name = file_name_parts[len(file_name_parts) - 1]
    return file_name[11: len(file_name) - 7]


def main(inputs, output):
    # read page visits
    df_init = spark.read.csv(inputs, sep=' ', schema=wikipedia_schema).withColumn('filename', functions.input_file_name())

    # filter the pages we are interested in
    f1_lang = df_init.filter(df_init['language'] == 'en')
    f2_title = f1_lang.filter(f1_lang['title'] != ('Main_Page'))
    f3_title = f2_title.filter(f2_title['title'].startswith('Special:') == False)

    # select required columns
    df_cols = f3_title.select(path_to_hour(f3_title['filename']).alias('hour'), f3_title['title'], f3_title['views']).cache()

    # get hour and max of each hour
    df_max = df_cols.groupBy(df_cols['hour']).agg(functions.max(df_cols['views']).alias('views'))
    df_max_rename = df_max.select(df_max['hour'].alias('m_hour'), df_max['views'].alias('m_views'))

    # broadcast df_max
    df_max_bc = functions.broadcast(df_max_rename)

    # final join
    df_joined = df_cols.join(df_max_bc, [df_cols['hour'] == df_max_bc['m_hour'], df_cols['views'] == df_max_bc['m_views']] , 'inner')

    # sort descending
    df_sorted = df_joined.sort(df_joined['hour']).select(df_joined['hour'], df_joined['title'], df_joined['views'])

    # write to output
    df_sorted.write.json(output, compression='gzip', mode='overwrite')

if __name__ == '__main__':
    inputs = sys.argv[1]
    output = sys.argv[2]
    main(inputs, output)
