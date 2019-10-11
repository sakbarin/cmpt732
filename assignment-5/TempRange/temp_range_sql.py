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
    df_init.createOrReplaceTempView("VW_Temp")

    # filter1 : qflag = null
    df_filter = spark.sql("SELECT station, observation, date, value "
                          "FROM VW_Temp "
                          "WHERE qflag IS NULL").cache()
    df_filter.createOrReplaceTempView("VW_Cols")


    # get max data frame
    df_max = spark.sql("SELECT station, observation, date, value AS max_value "
                       "FROM VW_Cols "
                       "WHERE observation = 'TMAX'")
    df_max.createOrReplaceTempView("VW_Max")


    # get min data frame
    df_min = spark.sql("SELECT station, observation, date, value AS min_value "
                       "FROM VW_Cols "
                       "WHERE observation = 'TMIN'")
    df_min.createOrReplaceTempView("VW_Min")


    # join max and min data frames
    df_joined = spark.sql("SELECT VMX.date, VMX.station, max_value, min_value "
                          "FROM VW_Max VMX "
                          "     INNER JOIN VW_Min VMN "
                          "         ON VMX.station = VMN.station AND VMX.date = VMN.date")
    df_joined.createOrReplaceTempView("VW_Joined")


    # calc temperature range
    df_temp_diff = spark.sql("SELECT date, station, (max_value - min_value) / 10 AS range "
                             "FROM VW_Joined").cache()
    df_temp_diff.createOrReplaceTempView("VW_TempDiff")


    # group and get date, max range
    df_max_range = spark.sql("SELECT date, max(range) AS range "
                             "FROM VW_TempDiff "
                             "GROUP BY date")
    df_max_range.createOrReplaceTempView("VW_MaxRange")


    # join max range and all ranges to get stations included
    df_final = spark.sql("SELECT VWTD.date, VWTD.station, VWMR.range "
                         "FROM VW_TempDiff VWTD "
                         "      INNER JOIN VW_MaxRange VWMR "
                         "          ON VWTD.date = VWMR.date AND VWTD.range = VWMR.range "
                         "ORDER BY VWTD.Date")


    # write to the output
    df_final.write.csv(output, mode='overwrite')

if __name__ == '__main__':
    inputs = sys.argv[1]
    output = sys.argv[2]
    main(inputs, output)
