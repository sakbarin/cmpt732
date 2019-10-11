import sys
assert sys.version_info >= (3, 5) # make sure we have Python 3.5+

# initial config
from pyspark.sql import SparkSession, functions, types
spark = SparkSession.builder.appName('TempRangeDF').getOrCreate()
assert spark.version >= '2.4' # make sure we have Spark 2.4+
spark.sparkContext.setLogLevel('WARN')
sc = spark.sparkContext

# constants
OBSERVATION_SCHEMA = types.StructType([
    types.StructField('station', types.StringType()),
    types.StructField('date', types.StringType()),
    types.StructField('observation', types.StringType()),
    types.StructField('value', types.IntegerType()),
    types.StructField('mflag', types.StringType()),
    types.StructField('qflag', types.StringType()),
    types.StructField('sflag', types.StringType()),
    types.StructField('obstime', types.StringType()),
])


def main(inputs, output):
    # initial data frame
    df_init = spark.read.csv(inputs, schema=OBSERVATION_SCHEMA)

    # filter1 : qflag = null
    df_filter = df_init.filter(df_init['qflag'].isNull()).cache()
    df_cols = df_filter.select(df_filter['station'], df_filter['observation'], df_filter['date'], df_filter['value'])

    # get max and min data frames
    df_max = df_cols.filter(df_cols['observation'] == 'TMAX').withColumnRenamed('value', 'max_value')
    df_min = df_cols.filter(df_cols['observation'] == 'TMIN').withColumnRenamed('value', 'min_value')

    # join max and min data frames
    df_joined = df_max.join(df_min, ['date', 'station'], 'inner')

    # calc temperature range
    df_temp_diff = df_joined.select(df_joined['date'], df_joined['station'], ( (df_joined['max_value'] - df_joined['min_value']) / 10 ).alias('range')).cache()

    # group and get date, max range
    df_max_range = df_temp_diff.groupBy(df_temp_diff['date']).agg(functions.max(df_temp_diff['range']).alias('range'))

    # broadcast max range
    df_max_range_bc = functions.broadcast(df_max_range)

    # join max range and all ranges to get stations included
    df_final = df_temp_diff.join(df_max_range_bc, ['date', 'range'], 'inner')

    # sort data
    df_sorted = df_final.sort(df_final['date']).select(df_final['date'], df_final['station'], df_final['range'])

    # write to the output
    df_sorted.write.csv(output, mode='overwrite')

if __name__ == '__main__':
    inputs = sys.argv[1]
    output = sys.argv[2]
    main(inputs, output)
