#pragma once

#include <string>

class Consumer
{
private:
	std::string _brokers;
	std::string _topic;
	std::string _group;
	std::string _name;

public:
	explicit Consumer(const std::string& brokers, const std::string& topic, const std::string& group, const std::string& name);
	void Start();
	void Stop();
};

