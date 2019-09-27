from pyspark import SparkConf, SparkContext
import sys
import random

assert sys.version_info >= (3, 5) # make sure we have Python 3.5+

NUM_SLICES = 8


def calc_sum(partition):
	random.seed()
	iteration_count = 0
	for i in range(len(partition)):
		sum = 0
		while (sum < 1):
			new_random = random.random()
			sum = sum + new_random
			iteration_count += 1
	yield iteration_count


def main(samples):
	samples_rdd = sc.parallelize(range(samples), NUM_SLICES)
	processed_rdd = samples_rdd.mapPartitions(calc_sum)
	total_iterations  = processed_rdd.reduce(lambda x,y: x + y)
	print(total_iterations / samples)


if __name__ == '__main__':
	conf = SparkConf().setAppName('EulerPy')
	sc = SparkContext(conf=conf)
	sc.setLogLevel('WARN')

	assert sc.version >= '2.4'  # make sure we have Spark 2.4+

	samples_input = int(sys.argv[1])
	main(samples_input)
