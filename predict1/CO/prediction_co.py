from pyspark import SparkContext, SparkConf
from pyspark.ml.regression import LinearRegression
from pyspark.sql import SQLContext, Row
from pyspark.sql.types import IntegerType
from pyspark.sql.functions import UserDefinedFunction
from pyspark.ml.linalg import Vectors

import os
import pyspark_csv as pycsv

os.environ["SPARK_LOCAL_IP"] = "127.0.1.1" # set local IP

DATADIR = "./Desktop/BDP/predict1/CO/data" # data directory

def mySparkContext():
    """
    Sets the Spark Context
    """
    conf = (SparkConf()
            .setMaster("local")
            .setAppName("TodayCOPrediction")
            .set("spark.executor.memory", "4g"))
    sc = SparkContext(conf = conf)
    return sc

sc = mySparkContext()
sqlCtx = SQLContext(sc)
sc.addPyFile("Desktop/BDP/predict1/CO/pyspark_csv.py")

def loadDF(filename):
    """
    Load and parse filename as pyspark.sql.DataFrame
    using pyspark_csv.py
    """
    path = os.path.join(DATADIR, filename)
    plain = sc.textFile(path)
    df = pycsv.csvToDataFrame(sqlCtx, plain, sep=',')
    return df


#------------------------------------------------------------

if __name__ == "__main__":

    train = loadDF("train.csv")
    test = loadDF("test.csv")

    testNumber = test.select('Number').rdd.map(lambda x: x.Number)

    train = train.select('CO AQI','NO2 AQI', 'O3 AQI', 'SO2 AQI')
    test = test.select('NO2 AQI', 'O3 AQI', 'SO2 AQI')


    # format train for Linear Regression as (label, features)
    ntrain = train.rdd.map(lambda x: Row(label = float(x[0]) \
         ,features = Vectors.dense(x[1:]))).toDF().cache()
    ntest = test.rdd.map(lambda x: Row(features = Vectors.dense(x[0:]))).toDF()

    lr = LinearRegression(maxIter = 100, regParam = 0.4, elasticNetParam=0.7)
    model = lr.fit(ntrain)

    print("Coefficients: " + str(model.coefficients))
    print("Intercept: " + str(model.intercept))

    pred = model.transform(ntest).select('prediction').rdd.map(lambda x: x.prediction)

    # configure the submission format as follows
    submit = sqlCtx.createDataFrame(testNumber.zip(pred), ["Number", "CO AQI"])



    os.chdir(DATADIR)
    # file is small so can save pandas.DataFrame as csv
    submit.toPandas().to_csv("prediction.csv", index = False)
    # if not, should saveAsTextFile:
    # submit.rdd.saveAsTextFile("/home/ehsan/Python/PySpark/Titanic/data/prediction")
    sc.stop()
