#include <iostream>
#include <string>
#include <cstdlib>
#include <cstdio>
#include <csignal>
#include <cstring>
#include <chrono>
#include <pthread.h>
#include <librdkafka/rdkafkacpp.h>

#if _AIX
#include <unistd.h>
#endif

#define MAX_THREAD 1

static volatile sig_atomic_t run = 1;
static void sigterm (int sig) { run = 0; }

//int number=0;

struct thread_parameter {
    pthread_t producer_thread;
    std::string pbroker;
    std::string ptopic;
    std::string pid;
    int randnum;
};

struct thread_parameter th_parameters[MAX_THREAD];

class ExampleDeliveryReportCb : public RdKafka::DeliveryReportCb {
public:
  void dr_cb (RdKafka::Message &message) {
    if (message.err())
      std::cerr << "% Message delivery failed: " << message.errstr() << std::endl;
    //else
        //std::cerr << number++ << std::endl;
      /*std::cerr << "% Message delivered " << (char*)message.payload() << "to topic " << message.topic_name() <<
        " [" << message.partition() << "] at offset " <<
        message.offset() << std::endl;*/
  }
};

std::string random_string(size_t len){
  const char char_set[] = 
    "0123456789"
    "ABCDEFGHIJKLMNOPQRSTUVWXYZ"
    "abcdefghijklmnopqrstuvwxyz";

  // len = (rand() % (len-10) )+ 10;
  std::string str(len, 0);
  for (int i=0; i<len; i++)
    str[i] = char_set[rand() % (sizeof(char_set) - 1)];
  return str;
}

//void create_producer(std::string pbroker, std::string ptopic, std::string pid, int randn){
void *increase_thread(void *data) {
    struct thread_parameter *now = (struct thread_parameter *)data;
    
    std::string brokers = now->pbroker;
    std::string topic = now->ptopic;
    std::string producer_id = now->pid;
    std::string errstr;
    bool prtflag = false;
    int randnum = now->randnum;
    int msg_cnt = 0;
    int msg_size = 0;
    int msg_len = 256000;
    
    ExampleDeliveryReportCb ex_dr_cb;
    RdKafka::Conf *conf = RdKafka::Conf::create(RdKafka::Conf::CONF_GLOBAL);
    if (conf->set("bootstrap.servers", brokers, errstr) != RdKafka::Conf::CONF_OK) {
        std::cerr << errstr << std::endl;
        exit(1);
    }
    if (conf->set("dr_cb", &ex_dr_cb, errstr) != RdKafka::Conf::CONF_OK) {
        std::cerr << errstr << std::endl;
        exit(1);
    }

    RdKafka::Producer *producer = RdKafka::Producer::create(conf, errstr);
    if (!producer) {
        std::cerr << "Failed to create producer: " << errstr << std::endl;
        exit(1);
    }

    signal(SIGINT, sigterm);
    signal(SIGTERM, sigterm);
    delete conf;

    std::cerr << "% " << producer_id << " starts to run randomized input." << std::endl;

    std::string line = "producer " + producer_id + " " + random_string(msg_len);

    auto start = std::chrono::high_resolution_clock::now();

    for (int i=0; i<randnum; i++) {
        //std::string line = "producer " + producer_id + " " + random_string(msg_len);

        if (line.empty()) {
        producer->poll(0);
        continue;
        }

        retry:
            RdKafka::ErrorCode err =
                producer->produce(
                                    topic,
                                    RdKafka::Topic::PARTITION_UA,
                                    RdKafka::Producer::RK_MSG_COPY,
                                    const_cast<char *>(line.c_str()), 
                                    line.size(),
                                    NULL, 
                                    0,
                                    0,
                                    NULL);

        if (err != RdKafka::ERR_NO_ERROR) {
            if (prtflag)
                std::cerr << "% Failed to produce to topic " << topic << ": " <<
                RdKafka::err2str(err) << std::endl;

            if (err == RdKafka::ERR__QUEUE_FULL) {
                std::cerr << "% Queue Full" << std::endl;
                producer->poll(1000);
                goto retry;
        }

        } else {
            if (prtflag)
                std::cerr << "% Enqueued message (" << line << ") with (" 
                    << line.size() << " bytes) " << "for topic " << topic << std::endl;
                msg_cnt++;
                msg_size += line.size();
        }

        producer->poll(0);
    }

    auto end = std::chrono::high_resolution_clock::now();
    std::cerr << "% " << producer_id << " flushes final messages..." << std::endl;
    producer->flush(10*1000);

    if (producer->outq_len() > 0) {
    //while (producer->outq_len() > 0) {    
    //    producer->flush(5*1000);
        std::cerr << "% " << producer->outq_len() <<
                " message(s) in " << producer_id << " were not delivered" << std::endl;
    }
    //auto end_flush = std::chrono::high_resolution_clock::now();
    std::cerr << "% " << producer_id << " is finished. (Message number: " << msg_cnt 
              << " , Bytes enqueued: " << msg_size << ")" << std::endl;

    auto duration = std::chrono::duration_cast<std::chrono::milliseconds>(end - start);
    std::cerr << producer_id << " uses " << duration.count() << " milliseconds in loop." << std::endl;

    //duration = std::chrono::duration_cast<std::chrono::milliseconds>(end_flush - end);
    //std::cerr << producer_id << " uses " << duration.count() << " milliseconds in flushing data." << std::endl;

    delete producer;
    pthread_exit(NULL);
}



int main() {
    int num;
    std::cin >> num;
    std::string brokers[MAX_THREAD];
    std::string topics[MAX_THREAD];
    std::string pid[MAX_THREAD];
    int randnum[MAX_THREAD];

    for (int i=0; i<MAX_THREAD; i++) {
        brokers[i] = "114.35.211.89:9094,114.35.211.89:9095,114.35.211.89:9096";
        topics[i] = "test";//"promotion";
        pid[i] = "producer" + std::to_string(i);
        randnum[i] = num;
    }

    for (int i=0; i<MAX_THREAD; i++) {
        th_parameters[i].pbroker = brokers[i];
        th_parameters[i].ptopic = topics[i];
        th_parameters[i].pid = pid[i];
        th_parameters[i].randnum = randnum[i];
    }

    for (int i=0; i<MAX_THREAD; i++) {
        if (-1 == pthread_create(&(th_parameters[i].producer_thread), 
                                 NULL,
                                 increase_thread,
                                 (void *)&th_parameters[i])) {
            std::cerr << "Create thread error" << std::endl;
            return -1;
        }
    }

    while (run) {}
    return 0;
}