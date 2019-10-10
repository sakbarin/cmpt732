import sys
assert sys.version_info >= (3, 5) # make sure we have Python 3.5+

# initial config
from pyspark.sql import SparkSession, functions, types
spark = SparkSession.builder.appName('WeatherETL').getOrCreate()
assert spark.version >= '2.4' # make sure we have Spark 2.4+
spark.sparkContext.setLogLevel('WARN')
sc = spark.sparkContext

# constants schema
wikipedia_schema = types.StructType([
    types.StructField('page_language', types.StringType()),
    types.StructField('page_title', types.StringType()),
    types.StructField('visit_count', types.StringType()),
    types.StructField('page_size', types.StringType()),
])

# constants names
LANG_COL = "language"
TITLE_COL = "title"

# constants values
LANG_VAL = "en"
MAIN_PAGE_VAL = "Main_Page"
SPECIAL_PAGE_VAL = "Special:"

@functions.udf(returnType=types.StringType())
def path_to_hour(path):
    file_name_parts = path.split('/')
    file_name = file_name_parts[len(file_name_parts) - 1]
    return file_name[11: len(file_name) - 3]


def main(inputs, output):
    # read page visits
    raw_data = spark.read.csv(inputs, sep=' ', schema=comments_schema).withColumn('filename', functions.input_file_name())

    records_with_filename = raw_data.select('*', path_to_hour(raw_data['filename']).alias('datetime'))


    # filter the pages we like
    filter_1_lang = records.filter(records[LANGUAGE_COL] == 'en')
    filter_2_main = filter_1_lang.filter(not f_en_pages[TITLE_COL].startswith(MAIN_PAGE_VAL) == False)
    filter_3_spec = filter_2_main(f_main_page[TITLE_COL].startswith(SPECIAL_PAGE_VAL) == False)


    # group by
    reddit_groups = comments_kv.groupBy(comments_kv['subreddit'])

    # find averages
    reddit_averages = reddit_groups.agg(functions.avg(comments['score']))

    reddit_averages.explain()

    # write to output
    reddit_averages.write.csv(output, mode='overwrite')

if __name__ == '__main__':
    inputs = sys.argv[1]
    output = sys.argv[2]
    main(inputs, output)
