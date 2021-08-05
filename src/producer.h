#pragma once

#include <string>
#include <chrono>
#include <thread>
#include <condition_variable>
#include <atomic>
#include <memory>
#include <stdexcept>
#include <exception>
#include <iostream>
#include <mutex>

class Producer
{
private:
	const std::string _brokers;
	const std::string _topic;
	const std::chrono::milliseconds _interval;

	bool _running;
	bool _stoping;
	std::mutex _main_mutex;
	std::mutex _cond_mutex;
	std::condition_variable _conditional;
	std::unique_ptr<std::thread> _producer_thread;

	void send_message();
	void produce();
	void handle_exception(std::exception_ptr eptr);
public:
	explicit Producer(const std::string& brokers, const std::string& topic, const int64_t& interval_millisecond);
	void Start();
	void Stop();
};

