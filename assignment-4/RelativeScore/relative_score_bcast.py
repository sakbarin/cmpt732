from pyspark import SparkConf, SparkContext
import sys
import json

assert sys.version_info >= (3, 5) # make sure we have Python 3.5+


def separate_columns(line_of_text):
    json_object = json.loads(line_of_text)
    yield (json_object["subreddit"], (1, int(json_object["score"]), json_object["author"]))


def add_pairs(pair1, pair2):
    count = pair1[0] + pair2[0]
    sum = pair1[1] + pair2[1]
    return (count, sum)


def calc_averages(kv):
    (key, (count, sum)) = (kv[0], kv[1])
    return (key, sum / count)


def calc_scores(kv, averages):
    (key, (one, score, author)) = (kv[0], kv[1])
    average = averages.value[key]
    return (author, score / average)


def get_value(kv):
    return  kv[1]


def convert_to_json(kv):
    return json.dumps(kv)


def main(inputs, output):
    input_text = sc.textFile(inputs)

    comments = input_text.flatMap(separate_columns).cache()
    combined_comments = comments.reduceByKey(add_pairs)
    reddit_averages = combined_comments.map(calc_averages)
    pos_reddit_averages = reddit_averages.filter(lambda x: x[1] > 0)

    averages = sc.broadcast(dict(pos_reddit_averages.collect()))

    author_scores = comments.map(lambda x: calc_scores(x, averages))
    output_data = author_scores.sortBy(get_value, ascending=False).map(convert_to_json)
    output_data.saveAsTextFile(output)


if __name__ == '__main__':
    conf = SparkConf().setAppName('RelativeScoreBCastPy')
    sc = SparkContext(conf=conf)
    sc.setLogLevel('WARN')
    assert sc.version >= '2.4'  # make sure we have Spark 2.4+
    inputs = sys.argv[1]
    output = sys.argv[2]
    main(inputs, output)
