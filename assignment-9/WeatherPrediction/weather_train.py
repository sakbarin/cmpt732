import sys
assert sys.version_info >= (3, 5) # make sure we have Python 3.5+

from pyspark.sql import SparkSession, functions, types
spark = SparkSession.builder.appName('colour prediction').getOrCreate()
spark.sparkContext.setLogLevel('WARN')
assert spark.version >= '2.4' # make sure we have Spark 2.4+

from pyspark.ml import Pipeline
from pyspark.ml.feature import VectorAssembler, SQLTransformer
from pyspark.ml.regression import *
from pyspark.ml.evaluation import RegressionEvaluator


tmax_schema = types.StructType([
    types.StructField('station', types.StringType()),
    types.StructField('date', types.DateType()),
    types.StructField('latitude', types.FloatType()),
    types.StructField('longitude', types.FloatType()),
    types.StructField('elevation', types.FloatType()),
    types.StructField('tmax', types.FloatType()),
])


def main(inputs, output):
    # read data
    data = spark.read.csv(inputs, schema=tmax_schema)

    # prepare train and validation set
    train, validation = data.randomSplit([0.75, 0.25])
    train = train.cache()
    validation = validation.cache()

    # transform date to day-of-year
    stm_yesterday_tmax = 'SELECT    today.station as station, \
                                    dayofyear(today.date) as dayofyear, \
                                    today.latitude as latitude, \
                                    today.longitude as longitude, \
                                    today.elevation as elevation, \
                                    today.tmax as tmax, \
                                    yesterday.tmax as yesterday_tmax \
                            FROM __THIS__ as today \
                                INNER JOIN __THIS__ as yesterday \
                                    ON date_sub(today.date, 1) = yesterday.date AND today.station = yesterday.station'
    transformer = SQLTransformer(statement=stm_yesterday_tmax)

    # input columns
    assembler = VectorAssembler(inputCols=['dayofyear', 'latitude', 'longitude', 'elevation', 'yesterday_tmax'], outputCol='features')

    # output column
    regressor = GBTRegressor(featuresCol='features', labelCol='tmax', maxIter=20, maxDepth=10)

    # pipeline
    pipeline = Pipeline(stages=[transformer, assembler, regressor])

    # train model
    model = pipeline.fit(train)

    # make predictions
    predictions = model.transform(validation)
    predictions.show()

    # evaluate model
    r2_evaluator = RegressionEvaluator(predictionCol='prediction', labelCol='tmax', metricName='r2')
    r2 = r2_evaluator.evaluate(predictions)
    
    rmse_evaluator = RegressionEvaluator(predictionCol='prediction', labelCol='tmax', metricName='rmse')
    rmse = rmse_evaluator.evaluate(predictions)

    # save model
    model.write().overwrite().save(output)

    # print score
    print("r2: %f" % (r2))
    print("rmse: %f" % (rmse))


if __name__ == '__main__':
    inputs = sys.argv[1]
    output = sys.argv[2]

    main(inputs, output)
