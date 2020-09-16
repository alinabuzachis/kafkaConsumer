# kafkaConsumer

## Description
kafkaConsumer is a standalone Python application that consumes messages from a Kafka topic which contain information about users visiting a website at specific times. The script calculates the number of unique users that visited the website in 1 minute intervals and outputs the result.

## Installation Guide
### Prerequisites
Apache Kafka requires Java to run. You must have java installed on your system. Execute below command to install default OpenJDK on your system from the official PPA’s.

```
sudo apt update
sudo apt install default-jdk
```
### Install Apache Kafka on Ubuntu

Follow the below steps to install Apache Kafka.

#### Download Apache Kafka

Download Apache Kafka binary from the [official link](https://downloads.apache.org/kafka/). Below wget command helps you to download the Kafka 2.11 version from Apache distributions.

```
wget http://www-us.apache.org/dist/kafka/2.4.0/kafka_2.13-2.4.0.tgz
```
#### Extract the archive file

```
tar -xzf kafka_2.13-2.4.0.tgz
```
#### Move the folder to user/local directory

```
mv kafka_2.13-2.4.0.tgz /usr/local/kafka
```
### Setup

Kafka uses zookeeper to run, so we have to start zookeeper server prior to the Kafka server.

#### Start Zookeeper server
```
cd /usr/local/kafka/bin
./zookeeper-server-start.sh ../config/zookeeper.properties 
```
Now we are ready to start the Kafka server.
#### Start Kafka server

```
./kafka-server-start.sh ../config/server.properties 
```
#### Create Kafka Topic
```
./kafka-topics.sh --create --zookeeper localhost:2181 --replication-factor 1 --partitions 1 --topic hello-topicCreated topic "raw-data".
```
The `--replication-factor` tells how many copies fo the data will be created, as we are running with single instance just keep it as 1. The `--partitions` option helps us to provide the number of brokers which we want to split between. The `--topic` options allos us to specify the name of the topic. In this case, the Producer will publish the data on topic `raw-data`. 

#### Create Kafka Producer
Kafka producer is a process, which is responsible to put data into Kafka. The Kafka provides a command line client which will take the input from a file or from standard input and send it out to messages to the Kafka cluster. The producer that emits messages to the topic needs to be started separately.

#### Create Kafka Consumer

The Kafka consumer Class is implemented in `kafka-consumer.py`. The configuration parameters are specified inside`config.conf`. The consumer can be started using using `main.py` allowing to create a Consumer instance and then start it. To run the application is required Python 3.8 and kafka-python.
```
python main.py
```
