from pyspark import SparkConf, SparkContext
import sys

# columns orders in tuple
VISIT_DATE = 0
PAGE_LANG = 1
PAGE_NAME = 2
VISIT_COUNT = 3
PAGE_SIZE = 4

# input and output path
inputs = sys.argv[1]
output = sys.argv[2]

# configuration
conf = SparkConf().setAppName('WikipediaPopularPy')
sc = SparkContext(conf=conf)

# assert
assert sys.version_info >= (3, 5)  # make sure we have Python 3.5+
assert sc.version >= '2.3'  # make sure we have Spark 2.3+


def separate_columns(text_line):
	columns = text_line.split()
	yield (columns[VISIT_DATE], columns[PAGE_LANG], columns[PAGE_NAME], columns[VISIT_COUNT], columns[PAGE_SIZE])

def cast_count_type(record):
	return (record[VISIT_DATE], record[PAGE_LANG], record[PAGE_NAME], int(record[VISIT_COUNT], 10), record[PAGE_SIZE])

def apply_constraints(page_info):
	return (page_info[PAGE_LANG].lower() == "en"
			and page_info[PAGE_NAME] != "Main_Page"
			and page_info[PAGE_NAME].startswith("Special:") == False)

def get_max(a,b):
	if (a[0] > b[0]):
		return a
	return b

def get_key(kv):
	return kv[0]

def tab_separated(kv):
	return "%s \t (%s, %s)" % (kv[0], kv[1][0], kv[1][1])

text = sc.textFile(inputs)

init_records = text.flatMap(separate_columns)
updated_records = init_records.map(cast_count_type)
filtered_records = updated_records.filter(apply_constraints)

init_pairs = filtered_records.map(lambda x: (x[VISIT_DATE], (x[VISIT_COUNT], x[PAGE_NAME])))
max_pairs = init_pairs.reduceByKey(get_max).coalesce(1) # remove coalesce(1) when you have a huge amount of data

outdata = max_pairs.sortBy(get_key).map(tab_separated)
outdata.saveAsTextFile(output)
