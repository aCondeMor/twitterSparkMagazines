# twitterSparkMagazines
Estimate selling points for magazines with twitter

The idea of this project is to use the twitter geolocalized that are talking of a concrete magazine in this example Glamour Magazine and check how many magazine's selling points are near of the hot zones without Glamour.

# Setup

For the project we will use: Flume, Hadoop, Kafka & Zookeeper, Spark (Streaming and Batch) and Carto.

# First Steps

- #kafka

Firstly we need to start Zookeeper

`/<zookeeper-path>/bin/zkServer.sh start`

Secondly we will start Kafka

`/<kafka-path>/bin/kafka-server-start.sh -daemon /<kafka-path>/config/server.properties`

Then we will create a Topic named Twitter

`/<kafka-path>/bin/kafka-topics.sh --zookeeper localhost:2181 --create --topic twitter --partitions 1 --replication-factor 1`

You can launch a consumer for see if kafka is receiving correctly the data

`/<bin/kafka-console-consumer.sh --bootstrap-server localhost:9092 --topic twitter --from-beginning`

- #Hadoop

After installing Hadoop correctly and configuring it propertly (core-site.xml, yarn-site.xml,hdfs-site.xml, mapred-site.xml) , we will start Yarn and HDFS

Formating Hadoop file system

`<hadoop-path>/hdfs namenode -format`

Starting HDFS

`<hadoop-path>/start-dfs.sh`

Starting YARN

`<hadoop-path>/start-yarn.sh`


I have solved an schema I/O HDFS error, giving permission to the temp path:

sudo chmod 750 /<hadoop-path>/tmp

Then we create all folders in HDFS and again give permission to access: 

`hadoop fs -mkdir /flume`

`hadoop fs -mkdir /flume/raw_data`

`hadoop fs -mkdir /spark`

`hadoop fs -mkdir /spark/streaming`

`hadoop fs -chmod -R 777 /flume`

`hadoop fs -chmod -R 777 /flume/raw_data`

`hadoop fs -chmod -R 777 /spark`

`hadoop fs -chmod -R 777 /spark/streaming`

Here we insert the file with all the selling points, some of the them sales the magazine the other not.(code, name, address, province, sales data, geo coordinates)

`hadoop fs -put Magazine.csv /spark`


- #Flume

With Flume we are going to extract the data from Twitter Api (necessary create an account) using a multiplex configuration.

    - source: twitter

    - 2 channels: Memory

    - 2 sinks: HDFS & Spark

![](https://media.licdn.com/mpr/mpr/AAEAAQAAAAAAAA3jAAAAJDgwZGM3YmYzLWRkYzItNDk4Ni05NDM5LWQyMDRmNThlMzM2Ng.png)
    

For start flume agent:

`/<Flume-path>/bin/flume-ng agent --conf conf --conf-file <Flume-path>/conf/conf_twitter --name twitter -Dflume.root.logger=INFO,console`


- #Spark

With spark we are going to work with streaming and finally with batch. The idea is to be getting tweets days before the magazine is going to be put on sale, and we do this with Spark Streaming and saving the results (Geo Coordinates) as the keywords and language are set in the flume configuration we don't need any more from the tweets in this concrete exercise.

When we are going to distribute the magazine, we will stop the streaming process and get from HDFS the geo coordinate tweet's information and also the file with all the info of the selling points.(code, name, address, province, sales data, geo coordinates)

As most of the tweets are without geo position and we neither have the geo position for the selling points, we have generate (Madrid aprox. ones) with a random process.
you can use [Json Data Generator](https://github.com/acesinc/json-data-generator) or creating your own one.

Starting spark streaming code:

`/<spark-path>/bin/spark-submit --jars /<spark-path>/lib/spark-streaming-kafka-assembly_2.10-1.6.2.jar /<user-path>/PycharmProjects/Twitter/streaming.py`

- # Carto

Finally in the Spark batch code we save the info of all the twitter points and the info of the new selling points in two csv files, and with these two files we generate a map with Carto

