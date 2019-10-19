from pyspark import SparkConf, SparkContext
from pyspark.sql import SparkSession, functions
import sys
import re
import math

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


def add_sigma_values(a,b):
    (one1, x1, x1_sq, y1, y1_sq, x1y1) = a
    (one2, x2, x2_sq, y2, y2_sq, x2y2) = b
    
    return (one1 + one2, x1 + x2, x1_sq + x2_sq, y1 + y2, y1_sq + y2_sq, x1y1 + x2y2)


def calc_r(a):
    (n, x, x_sq, y, y_sq, xy) = a
    
    numerator = (n * xy) - (x * y)    
    denominator_1st_sqrt = math.sqrt((n * x_sq - (x * x)))
    denominator_2nd_sqrt = math.sqrt((n * y_sq - (y * y)))
    
    return (numerator / (denominator_1st_sqrt * denominator_2nd_sqrt))


def main(inputs):
    input_text = sc.textFile(inputs)

    init_logs = input_text.flatMap(separate_columns)
    logs_datapoint = init_logs.reduceByKey(create_data_points)
    sigma_base_values = logs_datapoint.map(get_sigma_values)

    sum_sigma_values = sigma_base_values.reduce(add_sigma_values)
    r = calc_r(sum_sigma_values)

    print("\n\n\n---------------------------")
    print("r: %f , r^2: %f" % (r, math.pow(r, 2)))
    print("---------------------------\n\n")


if __name__ == '__main__':
    conf = SparkConf().setAppName('CorrelateLogsPy')
    sc = SparkContext(conf=conf)
    sc.setLogLevel('WARN')
    spark = SparkSession(sc)
    assert sc.version >= '2.4'  # make sure we have Spark 2.4+
    inputs = sys.argv[1]
    main(inputs)
