# kafka_performance_test
Simple speed test on librdkafka with C++ and kafka-node with nodejs

.cpp file compile in Ubuntu:
1. sudo apt install librdkafka-dev
2. sudo apt install pkg-config
```
g++ -std=c++17 $(pkg-config --cflags rdkafka++) my_consumer.cpp -o my_consumer $(pkg-config --libs rdkafka++)
g++ -std=c++17 $(pkg-config --cflags rdkafka++) pmain.cpp -o pmain $(pkg-config --libs rdkafka++) -lpthread
```
Cpp execution:
```
./pmain
//then input the number of data you want to test
//the topic is already set in the code

./my_consumer
//then it will show the manual to use this consumer
// e.g. ./my_consumer -g groupname -b brokerIps topicname
```
Nodejs :
```
// the brokers address, topic, and number of test data is set in config.js
node producer
node consumer
```
