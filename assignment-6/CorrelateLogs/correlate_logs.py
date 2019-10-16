from pyspark import SparkConf, SparkContext
from pyspark.sql import SparkSession, functions
import sys
import re

assert sys.version_info >= (3, 5) # make sure we have Python 3.5+

LINE_RE = re.compile(r'^(\S+) - - \[(\S+) [+-]\d+\] \"[A-Z]+ (\S+) HTTP/\d\.\d\" \d+ (\d+)$')
HOST = 1
BYTES = 4

def separate_columns(input_line):
    columns = LINE_RE.split(input_line)

    if (len(columns) > 4):
        yield (columns[HOST], (1, int(columns[BYTES])))


def create_data_points(value1, value2):
    requests_count = value1[0] + value2[0]
    bytes_sum = value1[1] + value2[1]
    return (requests_count, bytes_sum)


def get_sigma_values(datapoint):
    (host, (xi, yi)) = datapoint
    return (1, xi, pow(xi, 2), yi, pow(yi, 2), xi * yi)


def main(inputs):
    input_text = sc.textFile(inputs)

    init_logs = input_text.flatMap(separate_columns)
    logs_datapoint = init_logs.reduceByKey(create_data_points)
    sigma_values = logs_datapoint.map(get_sigma_values)

    df = sigma_values.toDF(['one', 'xi', 'xi_2', 'yi', 'yi_2', 'xiyi'])

    df_calc = df.select( \
                (functions.sum(df['one']) * functions.sum(df['xiyi']) - functions.sum(df['xi']) * functions.sum(df['yi'])).alias('top'), \
                (functions.sqrt(functions.sum(df['one']) * functions.sum(df['xi_2']) - functions.pow(functions.sum(df['xi']), 2)) * functions.sqrt(functions.sum(df['one']) * functions.sum(df['yi_2']) - functions.pow(functions.sum(df['yi']), 2))).alias('bottom') \
                )

    df_r = df_calc.select((df_calc['top'] / df_calc['bottom']).alias('r'))
    df_final = df_r.select(df_r['r'], functions.pow(df_r['r'], 2).alias('r^2'))

    df_final.show()

if __name__ == '__main__':
    conf = SparkConf().setAppName('CorrelateLogsPy')
    sc = SparkContext(conf=conf)
    sc.setLogLevel('WARN')
    spark = SparkSession(sc)
    assert sc.version >= '2.4'  # make sure we have Spark 2.4+
    inputs = sys.argv[1]
    main(inputs)
