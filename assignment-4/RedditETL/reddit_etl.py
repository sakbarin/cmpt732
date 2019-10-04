from pyspark import SparkConf, SparkContext
import sys
import json

assert sys.version_info >= (3, 5) # make sure we have Python 3.5+


SUBREDDIT = 0
AUTHOR = 1
SCORE = 2


def separate_columns(line_of_text):
    json_object = json.loads(line_of_text)
    yield (json_object["subreddit"], json_object["author"], int(json_object["score"]))


def filter_subreddits_with_e(input_tuple):
    return 'e' in input_tuple[SUBREDDIT]


def filter_gte_0(input_tuple):
    return (input_tuple[SCORE] >= 0)


def filter_lt_0(input_tuple):
    return (input_tuple[SCORE] < 0)


def convert_to_json(kv):
    return json.dumps(kv)


def main(inputs, output):
    input_text = sc.textFile(inputs)

    comments = input_text.flatMap(separate_columns)

    comments_with_e = comments.filter(filter_subreddits_with_e).cache()
    comments_gte_0 = comments_with_e.filter(filter_gte_0)
    comments_lt_0 = comments_with_e.filter(filter_lt_0)

    output_data_gte_0 = comments_gte_0.map(convert_to_json)
    output_data_lt_0 = comments_lt_0.map(convert_to_json)

    output_data_gte_0.saveAsTextFile(output + '/positive')
    output_data_lt_0.saveAsTextFile(output + '/negative')


if __name__ == '__main__':
    conf = SparkConf().setAppName('RedditETL')
    sc = SparkContext(conf=conf)
    sc.setLogLevel('WARN')
    assert sc.version >= '2.4'  # make sure we have Spark 2.4+
    inputs = sys.argv[1]
    output = sys.argv[2]
    main(inputs, output)
