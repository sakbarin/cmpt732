from pyspark import SparkConf, SparkContext
import sys
import json

assert sys.version_info >= (3, 5) # make sure we have Python 3.5+


def separate_columns(line_of_text):
    json_object = json.loads(line_of_text)
    yield (json_object["subreddit"], (1, int(json_object["score"])))


def add_pairs(pair1, pair2):
    count = pair1[0] + pair2[0]
    sum = pair1[1] + pair2[1]
    return (count, sum)


def calc_averages(kv):
    key, value = (kv[0], kv[1])
    count = value[0]
    sum = value[1]
    return (key, (sum * 1.0) / count)


def get_key(kv):
    return  kv[0]


def tab_separated(kv):
	return "%s \t %.3f" % (kv[0], kv[1])


def main(inputs, output):
    input_text = sc.textFile(inputs)

    comments = input_text.flatMap(separate_columns)
    combined_comments = comments.reduceByKey(add_pairs)
    reddit_averages = combined_comments.map(calc_averages)
    output_data = reddit_averages.sortBy(get_key).map(tab_separated).coalesce(1)
    output_data.saveAsTextFile(output)


if __name__ == '__main__':
    conf = SparkConf().setAppName('RedditAveragePy')
    sc = SparkContext(conf=conf)
    sc.setLogLevel('WARN')
    assert sc.version >= '2.4'  # make sure we have Spark 2.4+
    inputs = sys.argv[1]
    output = sys.argv[2]
    main(inputs, output)
