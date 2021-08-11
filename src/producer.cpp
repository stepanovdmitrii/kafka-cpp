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
			if (false == _connected) {
				connect();
				_connected = true;
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

Producer::~Producer()
{
	Stop();
}

void Producer::connect()
{
	std::string errstr;
	_callback = std::make_unique<DeliveryReportCallback>();
	std::unique_ptr<RdKafka::Conf> config(RdKafka::Conf::create(RdKafka::Conf::CONF_GLOBAL));
	_config = std::move(config);
	if (_config->set("bootstrap.servers", _brokers, errstr) != RdKafka::Conf::CONF_OK) {
		throw std::runtime_error(errstr);
	}
	if (_config->set("dr_cb", _callback.get(), errstr) != RdKafka::Conf::CONF_OK) {
		throw std::runtime_error(errstr);
	}
	std::unique_ptr<RdKafka::Producer> ptr(RdKafka::Producer::create(_config.get(), errstr));
	_producer = std::move(ptr);
	if (!_producer) {
		throw std::runtime_error(errstr);
	}
	std::cout << "connection to kafka established\n";
}

void Producer::send_message()
{
	_producer->poll(0);
	++_counter;
	std::string message = "message #" + std::to_string(_counter);
	while (true)
	{
		RdKafka::ErrorCode er_code = _producer->produce(_topic, RdKafka::Topic::PARTITION_UA, RdKafka::Producer::RK_MSG_COPY,
			const_cast<char *>(message.c_str()), message.length(), NULL, 0, NULL, NULL);
		if (er_code == RdKafka::ERR_NO_ERROR) {
			std::cout << "message '" << message << "' send\n";
			return;
		}
		if (er_code == RdKafka::ERR__QUEUE_FULL) {
			std::cout << "internal queue is full, waiting before next attempt...\n";
			_producer->poll(1 * 1000);
			continue;
		}
		std::cerr << "failed to send message: " << er_code << "\n";
		_producer->poll(0);
		return;
	}
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
	_brokers(brokers), _topic(topic), _interval(interval_millisecond), _running(false), _stoping(false), _connected(false), _counter(0)

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
