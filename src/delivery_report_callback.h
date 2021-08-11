#pragma once

#include <iostream>

#include "librdkafka/rdkafkacpp.h"


class DeliveryReportCallback: public RdKafka::DeliveryReportCb
{
public:
	void dr_cb(RdKafka::Message& message) override;
};

