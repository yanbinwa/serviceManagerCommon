package yanbinwa.common.kafka.producer;

import java.util.Map;
import java.util.Set;

import yanbinwa.common.iInterface.ServiceLifeCycle;
import yanbinwa.common.kafka.message.KafkaMessage;

public interface IKafkaProducer extends ServiceLifeCycle
{
    public static final String ZOOKEEPER_HOST_PORT_KEY = "zookeeperHostPort";
    public static final String PARTITION_NUM_KEY = "partitionNumKey";
    public static final int DEFAULT_PARTITION_NUM = 10;
    
    public static final String BROKER_LIST_KEY = "brokerList";
    public static final String SERIALIZER_CLASS_KEY = "serializerClass";
    public static final String MAX_BLOCK_MS = "maxBlockMs";
    public static final String BATCH_SIZE_KEY = "batchSize";
    public static final String RETRY_TIMES_KEY = "retries";
    
    public static final String DEFAULT_KAFKA_VALUE_SERIALIZER_CLASS = "org.apache.kafka.common.serialization.StringSerializer";
    public static final String DEFAULT_KAFKA_KEY_SERIALIZER_CLASS = "org.apache.kafka.common.serialization.IntegerSerializer";
    public static final String DEFAULT_KAFKA_PRODUCER_TYPE = "sync";
    public static final int DEFAULT_KAFKA_BATCH_SIZE = 200;
    public static final int DEFAULT_RETRY_TIMES = 0;
    
    public static final int KAFKA_PRODUCER_WORKER_QUEUE_SIZE = 100;
    //当kafka中queue为空时的等待时间
    public static final int KAFKA_PRODUCER_WORKER_QUEUE_SLEEP = 100;
    //当kafka连接中断后的timeout
    public static final int KAFKA_PRODUCER_TIMEOUT_SLEEP = 5000;
    
    boolean sendMessage(KafkaMessage msg);
    
    void updateTopicToPartitionSetMap(Map<String, Set<Integer>> topicToPartitionSetMap);
    
    String printTopicMapping();
}
