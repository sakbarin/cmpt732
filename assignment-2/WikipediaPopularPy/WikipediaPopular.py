from pyspark import SparkConf, SparkContext
import sys
import re, string

inputs = sys.argv[1]
output = sys.argv[2]

conf = SparkConf().setAppName('WikipediaPopularPy')
sc = SparkContext(conf=conf)

assert sys.version_info >= (3, 5)  # make sure we have Python 3.5+
assert sc.version >= '2.3'  # make sure we have Spark 2.3+

def separate_columns(line):
	columns = line.split()
	visit_date = columns[0]
	page_lang = columns[1]
	page_name = columns[2]
	visit_count = columns[3]
	page_size = columns[4]
	yield (visit_date, page_lang, page_name, visit_count, page_size)

def remove_unwantend_pages(page_info):
	if (page_info[1].lower() == "en" and page_info[2] != "Main_Page" and page_info[2].startswith("Special:") == False):
		return True
	else:
		return False

def get_max(a,b):
	if (int(a,10)>int(b,10)):
		return a
	return b

def get_key(kv):
	return kv[0]

def tab_separated(kv):
	return "%s \t %s" % (kv[0], kv[1])

text = sc.textFile(inputs)

page_visits = text.flatMap(separate_columns)
result_pages = page_visits.filter(remove_unwantend_pages)

pairs = result_pages.map(lambda x: (x[0], x[3]))
max_result = pairs.reduceByKey(get_max)

outdata = max_result.sortBy(get_key).map(tab_separated)
outdata.repartition(1).saveAsTextFile(output)
