import sys
assert sys.version_info >= (3, 5) # make sure we have Python 3.5+

# initial config
from pyspark.sql import SparkSession, functions, types
spark = SparkSession.builder.appName('WeatherETL').getOrCreate()
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
    weather = spark.read.csv(inputs, schema=OBSERVATION_SCHEMA)

    # filter1 : qflag = null
    weather_f1 = weather.filter(weather['qflag'].isNull())
    # filter2 : station = Canada
    weather_f2 = weather_f1.filter(weather['station'].startswith('CA'))
    # filter3 : observation = TMAX
    weather_f3 = weather_f2.filter(weather['observation'] == 'TMAX')

    # new column for degree as celsius -> TMAX
    weather_final = weather_f3.select(weather_f3['station'], weather_f3['date'], (weather_f3["value"] / 10).alias('TMAX'))

    # write to the output
    weather_final.write.json(output, compression='gzip', mode='overwrite')

if __name__ == '__main__':
    inputs = sys.argv[1]
    output = sys.argv[2]
    main(inputs, output)
