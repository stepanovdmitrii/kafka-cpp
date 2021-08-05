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

    producer->Start();

    wait_for_ctrl_c();

    producer->Stop();

    CV.notify_one();

    return 0;
}
