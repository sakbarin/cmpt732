import sys
assert sys.version_info >= (3, 5) # make sure we have Python 3.5+

from pyspark.sql import SparkSession, functions
from kafka import KafkaConsumer

spark = SparkSession.builder.appName('KafkaReadStream').getOrCreate()
assert spark.version >= '2.4' # make sure we have Spark 2.4+

spark.sparkContext.setLogLevel('WARN')
sc = spark.sparkContext

def main(topic):
    # get stream
    messages = spark.readStream.format('kafka') \
        .option('kafka.bootstrap.servers', '199.60.17.210:9092,199.60.17.193:9092') \
        .option('subscribe', topic).load()

    # get values and decode
    df_base = messages.select(functions.decode(messages['value'], 'utf-8').alias('value'))

    # split dataframe column to (x, y)
    df_cols = functions.split(df_base['value'], ' ')
    df_xy = df_base.withColumn('x', df_cols.getItem(0)) \
                   .withColumn('y', df_cols.getItem(1))

    # compute x, y, xy, x^2
    df_main = df_xy.select(df_xy['x'], df_xy['y'], (df_xy['x'] * df_xy['y']).alias('xy'), (df_xy['x'] * df_xy['x']).alias('x_sq'))

    # compute sigma values (n, x, y, xy, x^2)
    df_sigmas = df_main.select(
                functions.count(df_main['x']).alias('n'), \
                functions.sum(df_main['x']).alias('x'), \
                functions.sum(df_main['y']).alias('y'), \
                functions.sum(df_main['xy']).alias('xy'), \
                functions.sum(df_main['x_sq']).alias('x_sq') \
            )

    # compute value for beta
    df_Beta = df_sigmas.select(
                    df_sigmas['*'], \
                    ( \
                        (df_sigmas['xy'] - (1 / df_sigmas['n']) * (df_sigmas['x'] * df_sigmas['y'])) / \
                        (df_sigmas['x_sq'] - (1 / df_sigmas['n']) * (df_sigmas['x'] * df_sigmas['x'])) \
                    ).alias('beta'))

    # compute value for alpha
    df_result = df_Beta.select( \
                    df_Beta['beta'], \
                    ( \
                        (df_Beta['y'] / df_Beta['n']) - (df_Beta['beta'] * (df_Beta['x'] / df_Beta['n'])) \
                    ).alias('alpha'))

    # write to output
    stream = df_result.writeStream.outputMode("complete").format("console").start()
    stream.awaitTermination(600)

if __name__ == '__main__':
    topic = sys.argv[1]

    main(topic)
