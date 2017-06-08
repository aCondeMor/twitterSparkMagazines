
from __future__ import print_function
from pyspark import SparkContext
from pyspark.streaming import StreamingContext
from pyspark.streaming.kafka import KafkaUtils
import json
import random
import time


# Random generator of Madrid long. coordinates
def coord_long():


    longdec = random.uniform(-0.353348, -0.955310)

    longitud = -3 + longdec
    longitud = float("{0:.6f}".format(longitud))

    return longitud

# Random generator of Madrid lat. coordinates
def coord_latitud():
    latdec = random.uniform(0.184333, 0.602099)

    latitud = 40 + latdec
    latitud = float("{0:.6f}".format(latitud))
    return latitud


# Create Spark context
sc = SparkContext(appName="PythonStreamingDirectKafka")

# Create Streaming context
ssc = StreamingContext(sc, 10)

# Getting data from Kafka and topic "twitter"
kafkaStream = KafkaUtils.createStream(ssc, 'localhost:2181', 'spark-streaming', {'twitter':1})


# Parse the inbound message as json
parsed = kafkaStream.map(lambda v: json.loads(v[1]))

#Getting from Json the following data (user, geo coordinates, text)
# if geo coordinates is null, we generate it with the random coordinates generator
dataStream = parsed.map(lambda tweet: (tweet['geo']['coordinates'] if tweet['geo'] else coord_latitud(),tweet['geo']['coordinates'] if tweet['geo'] else coord_long()))


#Saving in HDFS
path = "hdfs://localhost:9000/spark/streaming/test/"
dataStream.repartition(1).saveAsTextFiles(path + str(time.time()))

ssc.start()
ssc.awaitTermination()