from pyspark import SparkConf, SparkContext
import sys
import re, string

assert sys.version_info >= (3, 5) # make sure we have Python 3.5+
wordsep = re.compile(r'[%s\s]+' % re.escape(string.punctuation))


def get_words(line):
	words = wordsep.split(line)
	for word in words:
		yield (word.lower(), 1)


def add(x, y):
	return x + y


def get_key(kv):
	return kv[0]


def output_format(kv):
	k, v = kv
	return '%s %i' % (k, v)


def main(inputs, output):
	text = sc.textFile(inputs)
	words = text.flatMap(get_words)
	filtered_words = words.filter(lambda x: x != "")
	word_count = filtered_words.reduceByKey(add)
	outdata = word_count.sortBy(get_key).map(output_format)
	outdata.saveAsTextFile(output)


if __name__ == '__main__':
	conf = SparkConf().setAppName('WordCountImprovedPy')
	sc = SparkContext(conf=conf)
	sc.setLogLevel('WARN')
	assert sc.version >= '2.4'  # make sure we have Spark 2.4+
	inputs = sys.argv[1]
	output = sys.argv[2]
	main(inputs, output)
