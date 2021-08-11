// kafka-cpp.cpp : Этот файл содержит функцию "main". Здесь начинается и заканчивается выполнение программы.
//

#include <iostream>
#include <memory>
#include <csignal>
#include <Windows.h>
#include <stdio.h>
#include <condition_variable>
#include <mutex>

#include "producer.h"
#include "consumer.h"

inline const std::string Group_title = "default_group";

std::mutex Mutex;
std::condition_variable CV;



BOOL WINAPI CtrlHandler(DWORD dwCtrlType)
{
    switch (dwCtrlType)
    {
    case CTRL_C_EVENT:
        CV.notify_one();
        {
            std::unique_lock<std::mutex> l(Mutex);
            CV.wait(l);
        }
        return TRUE;

    case CTRL_CLOSE_EVENT:
        CV.notify_one();
        {
            std::unique_lock<std::mutex> l(Mutex);
            CV.wait(l);
        }
        return TRUE;

    default:
        return FALSE;
    }
}

void wait_for_ctrl_c() {
    std::unique_lock<std::mutex> l(Mutex);
    CV.wait(l);
}

int main(int argc, char** argv)
{
    if (argc != 3) {
        std::cerr << "Usage: " << argv[0] << " <brokers> <topic>" << std::endl;
        exit(1);
    }

    SetConsoleCtrlHandler(CtrlHandler, TRUE);

    std::string brokers = argv[1];
    std::string topic = argv[2];

    std::unique_ptr<Producer> producer = std::make_unique<Producer>(brokers, topic, 3000);
    std::unique_ptr<Consumer> consumer_1 = std::make_unique<Consumer>(brokers, topic, Group_title, "consumer_1");
    std::unique_ptr<Consumer> consumer_2 = std::make_unique<Consumer>(brokers, topic, Group_title, "consumer_2");
    std::unique_ptr<Consumer> consumer_3 = std::make_unique<Consumer>(brokers, topic, Group_title, "consumer_3");
    std::unique_ptr<Consumer> consumer_4 = std::make_unique<Consumer>(brokers, topic, Group_title, "consumer_4");

    producer->Start();
    consumer_1->Start();
    consumer_2->Start();
    consumer_3->Start();
    consumer_4->Start();

    wait_for_ctrl_c();

    producer->Stop();
    consumer_1->Stop();
    consumer_2->Stop();
    consumer_3->Stop();
    consumer_4->Stop();

    CV.notify_one();

    return 0;
}
