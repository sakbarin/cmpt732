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
    return (key, sum / count) # ( subreddit , avg )


def calc_scores(kv):
    (key, (average, (score, author))) = (kv[0], kv[1]) # (subreddit, (average, (score, author)))
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
    pos_reddit_averages = reddit_averages.filter(lambda x: x[1] > 0) # (subreddit, average)

    comments_data = comments.map(lambda x: (x[0], (x[1][1], x[1][2]))) # (subreddit, (score, author))
    comments_joined = pos_reddit_averages.join(comments_data)

    author_scores = comments_joined.map(calc_scores)
    output_data = author_scores.sortBy(get_value, ascending=False).map(convert_to_json)
    output_data.saveAsTextFile(output)


if __name__ == '__main__':
    conf = SparkConf().setAppName('RelativeScorePy')
    sc = SparkContext(conf=conf)
    sc.setLogLevel('WARN')
    assert sc.version >= '2.4'  # make sure we have Spark 2.4+
    inputs = sys.argv[1]
    output = sys.argv[2]
    main(inputs, output)
