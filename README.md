# Kafka Exercises for Guest Lecture at the Frankfurt University of Applied Sciences

This repository contains a code-skeleton including TODO's and explanation of the solution.

## Prerequisites

For the exercise, we use the Confluent Platform. Confluent Platform is built on Apache Kafka and extends it with many useful features and tools.

The installation is done using Docker Compose. First, we download the necessary Docker Compose file. To do this, we use the following commands:

```bash
git clone https://github.com/confluentinc/cp-all-in-one
cd cp-all-in-one
git checkout 7.1.0-post
cd cp-all-in-one-community
```

In this directory you should now find the file docker-compose.yml. Now, we just need to download the necessary Docker images and start the Confluent Platform. This is easily done with a single command:

```bash
docker-compose up -d zookeeper broker
```

Several Docker images will be downloaded, so do not be surprised if it takes a few minutes for everything to download. When everything has gone through successfully, you should see the following output:

```
Starting zookeeper ... done
Creating broker    ... done
```

Zookeeper and Kafka Broker are now up and running.  

## Exercises

Clone the repository:
```
git clone https://github.com/NovatecConsulting/frauas-kafka-exercises.git
````

You find the exercises with the TODO's in the *main* branch in the files:
* [src/main/java/demo/KafkaProducerDemo.java](src/main/java/demo/KafkaProducerDemo.java)
* [src/main/java/demo/KafkaConsumerDemo.java](src/main/java/demo/KafkaConsumerDemo.java)

The solution is implemented in the branch *solutions*. In the branch *solutions-explanation* additional explanations are implemented.

For the complete exercise open the file [exercise.pdf](exercise.pdf).
