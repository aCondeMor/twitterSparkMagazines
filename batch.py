import os.path

from pyspark import SparkContext
from pyspark import sql
from pyspark.sql import Row
from pyspark.sql.types import *
import random



inputPathCSV = ('hdfs://localhost:9000/spark')
inputFileName = ('Glamour.csv')
inputStreamFile = ("hdfs://localhost:9000/spark/streaming/*/")

#create Spark context
sc = SparkContext(appName="distribution")

# getting from the file in HDFS the selling points for Madrid and Barcelona with sales for Glamour
# points with -1 copies received are not being served
sellingPoints = os.path.join(inputPathCSV, inputFileName)


#getting all the twitter coordinates from HDFS
twitter = sc.textFile(inputStreamFile)

# Random generator of Barcelona long. coordinates
def barc_long():


    longdec = random.uniform(0.0, 0.7)

    longitud = 1.73485 + longdec
    longitud = float("{0:.6f}".format(longitud))

    return longitud

# Random generator of Barcelona lat. coordinates
def Barc_latitud():

    latdec = random.uniform(0.216943, 0.770186)

    latitud = 41 + latdec
    latitud = float("{0:.6f}".format(latitud))
    return latitud

# Random generator of Madrid long. coordinates
def Mad_long():


    longdec = random.uniform(-0.353348, -0.955310)

    longitud = -3 + longdec
    longitud = float("{0:.6f}".format(longitud))

    return longitud

# Random generator of Madrid lat. coordinates
def Mad_latitud():
    latdec = random.uniform(0.184333, 0.602099)

    latitud = 40 + latdec
    latitud = float("{0:.6f}".format(latitud))
    return latitud



# Load a text file and convert each line to a Row.
# if the province is "Madrid" we generate coordinates of Madrid else the Barcelona ones
def toRow(entry):
    lines = sc.textFile(entry)
    parts = lines.map(lambda l: l.split(";"))
    rdd = parts.map(lambda p: Row(sellPoint=int (p[0]), province=p[1], \
                                 zipcode=int (p[2]), copiesDel=int(p[3]), unsolds=int(p[4]), \
                                 sales=int(p[5]),latitud = float (Mad_latitud() if p[1]=='MADRID' else Barc_latitud()),longitud = float (Mad_long() if p[1]=='MADRID' else barc_long())))
    return rdd

# Load the twitter data with the coordinates, taking care of the format of the string coordinates
def toRowTwitter(lines):
    parts = lines.map(lambda l: l.split(","))
    rdd = parts.map(lambda p: Row(latitud= float (p[0][1:len(p[0])].strip('[')), longitud = float (p[1][0:len(p[1])-1].strip(']'))))
    print rdd.collect()
    return rdd


#count the number of copies in selling points with zero sales
def nonSalesCount(rdd):
  return  rdd.map(lambda x: (x.sales, x.copiesDel)).filter(lambda s: (s[0] == 0))\
  .reduceByKey(lambda a,b:a+b).map(lambda f: (f[1])).take(1)


#list of selling points with zero sales
def nonSalesPoints(rdd):
  return  rdd.filter(lambda s: (s.sales == 0))

# list of available selling points in (Key,Value) format and key rounded to 3 decimals; the value is a {dictionary}
def availablePoints(rdd):
  return  rdd.filter(lambda s: (s.sales == -1)).map(lambda x: (("{0:.3f}".format(x.latitud))+("{0:.3f}".format(x.longitud)),{ 'sellPoint': x.sellPoint, 'longitud':x.longitud,'latitud':x.latitud}))


#list of the selling points with the magazine that sales more than zero
def salesPoints(rdd):
  return  rdd.filter(lambda s: (s.sales >0))


#join of the twitter points and the available points rounded to 3 decimals
def searchSellingPoints(twitterPoints, availablePoints):
    roundTweet = twitterPoints.map(lambda x: ("{0:.3f}".format(x.latitud),"{0:.3f}".format(x.longitud))).map(lambda x: (x[0]+x[1],1))\
    .reduceByKey(lambda a,b:a+b).sortBy(lambda(k,v): -v)

    roundedAvailable = availablePoints.map (lambda x: (x[0],x[1]))
    sellPointsDistributed = roundedAvailable.join(roundTweet)
    return sellPointsDistributed


#list of the selling points with the magazine that sales more than zero
actualMagPoints =toRow(sellingPoints)

#getting an RDD with all the selling points
allPoints = toRow(sellingPoints)

#getting an RDD witl all the twitter points
twitterPoints = toRowTwitter(twitter)

#getting an RDD with all the selling points that actually are not selling the magazine
newPoints = availablePoints(allPoints)


# comparing the selling points with the twitter points for getting the better points
finalList = searchSellingPoints(twitterPoints,newPoints)

# parsing from Row to list
final = finalList.map(lambda x: (x[1])).map(lambda y: (y[0]['longitud'],y[0]['latitud'], y[0]['sellPoint']))
print ('Listado total de puntos a distribuir:')
print final.collect()
print final.count()

# and the twitter points to a list too
outputtweets = twitterPoints.map(lambda x: (x.longitud, x.latitud))


# finally create s SQL dataframe for save as csv file for use the data in Carto
customSchema = StructType([ \
    StructField("long", FloatType(), True), \
    StructField("lat", FloatType(), True)])


sqlContext = sql.SQLContext(sc)
df = sqlContext.createDataFrame(outputtweets, customSchema)
df.coalesce(1).write.option("header", "true").csv("hdfs://localhost:9000/spark/localtwitter.csv")


finalPoints = final.map(lambda x: (x[0],x[1]))
df = sqlContext.createDataFrame(finalPoints, customSchema)
df.coalesce(1).write.option("header", "true").csv("hdfs://localhost:9000/spark/New_Selling_Points.csv")





