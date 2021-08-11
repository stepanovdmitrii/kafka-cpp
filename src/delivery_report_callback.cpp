#include "delivery_report_callback.h"


void DeliveryReportCallback::dr_cb(RdKafka::Message& message)
{
	std::cout << "message delivered for (" << message.len() << " bytes): " <<
		message.status() << ": " << message.errstr() << std::endl;
}
