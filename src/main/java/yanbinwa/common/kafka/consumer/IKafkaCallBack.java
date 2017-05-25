package yanbinwa.common.kafka.consumer;

import yanbinwa.common.kafka.message.KafkaMessage;

public interface IKafkaCallBack
{
    void handleOnData(KafkaMessage msg);
}
