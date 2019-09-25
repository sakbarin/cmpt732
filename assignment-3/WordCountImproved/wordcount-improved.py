from pyspark import SparkConf, SparkContext
import sys
import re, string

inputs = sys.argv[1]
output = sys.argv[2]
wordsep = re.compile(r'[%s\s]+' % re.escape(string.punctuation))

conf = SparkConf().setAppName('WordCountImproved')
sc = SparkContext(conf=conf)

assert sys.version_info >= (3, 5)  # make sure we have Python 3.5+
assert sc.version >= '2.3'  # make sure we have Spark 2.3+

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

text = sc.textFile(inputs)

words = text.flatMap(get_words)
filtered_words = words.filter(lambda x: x != "")
word_count = filtered_words.reduceByKey(add)

outdata = word_count.sortBy(get_key).map(output_format)
outdata.saveAsTextFile(output)
