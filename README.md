# kafka_performance_test
Simple speed test on librdkafka with C++ and kafka-node with nodejs

compile in Ubuntu:
1. sudo apt install librdkafka-dev
2. sudo apt install pkg-config

g++ -std=c++17 $(pkg-config --cflags rdkafka++) my_consumer.cpp -o my_consumer $(pkg-config --libs rdkafka++)
g++ -std=c++17 $(pkg-config --cflags rdkafka++) pmain.cpp -o pmain $(pkg-config --libs rdkafka++) -lpthread
