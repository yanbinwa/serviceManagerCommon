package yanbinwa.common.kafka.consumer;

import yanbinwa.common.iInterface.ServiceLifeCycle;

public interface IKafkaConsumer extends ServiceLifeCycle
{
    public static final String BROKER_LIST_KEY = "brokerList";
    public static final String DESERIALIZER_CLASS_KEY = "deserializerClass";
    public static final String MAX_BLOCK_MS_KEY = "maxBlockMs";
    public static final String GROUP_ID_KEY = "groupId";
    public static final String LISTEN_TOPIC_KEY = "consumerTopic";
    public static final String ZOOKEEPER_HOST_PORT_KEY = "zookeeperHostport";
    
    public static final String DEFAULT_KAFKA_VALUE_DESERIALIZER_CLASS = "org.apache.kafka.common.serialization.StringDeserializer";
    public static final String DEFAULT_KAFKA_KEY_DESERIALIZER_CLASS = "org.apache.kafka.common.serialization.IntegerDeserializer";
    public static final String DEFAULT_KAFKA_PRODUCER_TYPE = "sync";
    
    public static final int KAFKA_POLL_TIMEOUT = 1000;
    public static final int WAIT_TOPIC_CREATE_RETRY_TIME = 3;
    public static final int WAIT_TOPIC_CREATE_INTERVAL = 100;
}
