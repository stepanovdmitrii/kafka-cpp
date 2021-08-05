#include "producer.h"

void Producer::produce()
{
	for (;;) {
		try {
			std::unique_lock<std::mutex> guard(_cond_mutex);
			if (_stoping) {
				return;
			}
			_conditional.wait_for(guard, _interval);
			if (_stoping) {
				return;
			}
			send_message();
		}
		catch (...) {
			std::exception_ptr p = std::current_exception();
			handle_exception(p);
		}
	}
}

void Producer::Stop()
{
	std::unique_lock<std::mutex> l(_main_mutex);
	if (_running == false) {
		return;
	}
	{
		std::unique_lock<std::mutex> n(_cond_mutex);
		_stoping = true;
	}
	_conditional.notify_one();
	_producer_thread->join();
	_producer_thread.reset();
	_running = false;
	_stoping = false;
	std::cout << "producer stopped\n";
}

void Producer::send_message()
{
	std::cout << "producer sends message\n";
}

void Producer::handle_exception(std::exception_ptr eptr)
{
	try {
		if (eptr) {
			std::rethrow_exception(eptr);
		}
	}
	catch (const std::exception& e) {
		std::cout << "producer failed: " << e.what() << "\n";
	}
}

Producer::Producer(const std::string& brokers, const std::string& topic, const int64_t& interval_millisecond):
	_brokers(brokers), _topic(topic), _interval(interval_millisecond), _running(false), _stoping(false)

{
}

void Producer::Start()
{
	std::unique_lock<std::mutex> l(_main_mutex);
	if (_running) {
		throw std::runtime_error("producer is already running");
	}
	_producer_thread = std::make_unique<std::thread>([this] { this->produce(); });
	_running = true;
}
