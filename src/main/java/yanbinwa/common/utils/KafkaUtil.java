package yanbinwa.common.utils;

import java.util.HashMap;
import java.util.Map;
import java.util.Properties;

import org.apache.kafka.common.security.JaasUtils;
import org.apache.log4j.Logger;

import kafka.admin.AdminUtils;
import kafka.utils.ZkUtils;
import yanbinwa.common.constants.CommonConstants;
import yanbinwa.common.kafka.consumer.IKafkaCallBack;
import yanbinwa.common.kafka.consumer.IKafkaConsumer;
import yanbinwa.common.kafka.consumer.IKafkaConsumerImpl;
import yanbinwa.common.kafka.producer.IKafkaProducer;
import yanbinwa.common.kafka.producer.IKafkaProducerImpl;

public class KafkaUtil
{
    public static final Logger logger = Logger.getLogger(KafkaUtil.class);
    
    @SuppressWarnings("unchecked")
    private static Map<String, Object> getKafkaProducerProperties(Map<String, Object> kafkaProperties)
    {
        if (kafkaProperties == null)
        {
            return null;
        }
        Object kafkaProducerPropertiesObj = kafkaProperties.get(CommonConstants.KAFKA_PRODUCERS_KEY);
        if (kafkaProducerPropertiesObj != null)
        {
            if (!(kafkaProducerPropertiesObj instanceof Map))
            {
                logger.error("kafkaProducerPropertiesObj shoud be Map " + kafkaProducerPropertiesObj);
                return null;
            }
            else
            {
                return (Map<String, Object>)kafkaProducerPropertiesObj;
            }
        }
        else
        {
            return null;
        }
    }
    
    public static Map<String, IKafkaProducer> createKafkaProducerMap(Map<String, Object>kafkaProperties)
    {
        Map<String, IKafkaProducer> kafkaProducerMap = new HashMap<String, IKafkaProducer>();
        Map<String, Object> kafkaProducerProperties = getKafkaProducerProperties(kafkaProperties);
        if (kafkaProducerProperties == null)
        {
            logger.info("kafka producer properties is null");
            return kafkaProducerMap;
        }
        for(Map.Entry<String, Object> entry : kafkaProducerProperties.entrySet())
        {
            if(entry.getValue() instanceof Map)
            {
                @SuppressWarnings("unchecked")
                Map<String, String> kafkaProperty = (Map<String, String>)entry.getValue();
                String topicGourp = entry.getKey();
                IKafkaProducer kafkaProducer = new IKafkaProducerImpl(kafkaProperty, topicGourp);
                kafkaProducerMap.put(topicGourp, kafkaProducer);
            }
            else
            {
                logger.error("kafka property shoud not be null " + entry.getKey());
                continue;
            }
        }
        return kafkaProducerMap;
    }
    
    @SuppressWarnings("unchecked")
    private static Map<String, Object> getKafkaConsumerProperties(Map<String, Object> kafkaProperties)
    {
        if (kafkaProperties == null)
        {
            return null;
        }
        Object kafkaConsumerPropertiesObj = kafkaProperties.get(CommonConstants.KAFKA_CONSUMERS_KEY);
        if (kafkaConsumerPropertiesObj != null)
        {
            if (!(kafkaConsumerPropertiesObj instanceof Map))
            {
                logger.error("kafkaConsumerPropertiesObj shoud be Map " + kafkaConsumerPropertiesObj);
                return null;
            }
            else
            {
                return (Map<String, Object>)kafkaConsumerPropertiesObj;
            }
        }
        else
        {
            return null;
        }
    }
    
    public static Map<String, IKafkaConsumer> createKafkaConsumerMap(Map<String, Object> kafkaProperties, IKafkaCallBack callback)
    {
        Map<String, IKafkaConsumer> kafkaConsumerMap = new HashMap<String, IKafkaConsumer>();
        Map<String, Object> kafkaConsumerProperties = getKafkaConsumerProperties(kafkaProperties);
        if (kafkaConsumerProperties == null || callback == null)
        {
            logger.info("kafka consumer properties is null or callback is null");
            return kafkaConsumerMap;
        }
        
        for(Map.Entry<String, Object> entry : kafkaConsumerProperties.entrySet())
        {
            if(entry.getValue() instanceof Map)
            {
                @SuppressWarnings("unchecked")
                Map<String, String> kafkaProperty = (Map<String, String>)entry.getValue();
                String consumerTopic = entry.getKey();
                IKafkaConsumer kafkaConsumer = new IKafkaConsumerImpl(kafkaProperty, consumerTopic, callback);
                kafkaConsumerMap.put(consumerTopic, kafkaConsumer);
            }
            else
            {
                logger.error("kafka property shoud not be null " + entry.getKey());
                continue;
            }
        }
        return kafkaConsumerMap;
    }
    
    /**
     * createZkClient(zkUrl: String, sessionTimeout: Int, connectionTimeout: Int)
     * 
     * @param kafkaProperties
     * @return
     */
    public static ZkUtils createZkUtils(Map<String, Object> zookeeperProperties)
    {
        String brokerServer = (String) zookeeperProperties.get(CommonConstants.ZOOKEEPER_HOSTPORT_KEY);
        if (brokerServer == null)
        {
            logger.error("brokerServer should not be null");
            return null;
        }
        int sessionTimeout = CommonConstants.ZOOKEEPER_SESSION_TIMEOUT_DEFAULT;
        Object sessionTimeoutObj = zookeeperProperties.get(CommonConstants.ZOOKEEPER_SESSION_TIMEOUT_KEY);
        if (sessionTimeoutObj != null && (sessionTimeoutObj instanceof Integer))
        {
            sessionTimeout = (Integer) sessionTimeoutObj;
        }
        
        int connectionTimeout = CommonConstants.ZOOKEEPER_CONNECTION_TIMEOUT_DEFAULT;
        Object connectionTimeoutObj = zookeeperProperties.get(CommonConstants.ZOOKEEPER_CONNECTION_TIMEOUT_KEY);
        if (connectionTimeoutObj != null && (connectionTimeoutObj instanceof Integer))
        {
            connectionTimeout = (Integer) connectionTimeoutObj;
        }
        return ZkUtils.apply(brokerServer, sessionTimeout, connectionTimeout, JaasUtils.isZkSecurityEnabled());
    }
    
    public static void closeZkUtils(ZkUtils zkUtils)
    {
        if (zkUtils != null)
        {
            zkUtils.close();
        }
    }
    
    /**
     * 
     * 直接修改zookeeper中的信息，所以传入的zookeeper和hostname和
     * 
     * zkUtils: ZkUtils, topic: String, partitions: Int, replicationFactor: Int, topicConfig: Properties = new Properties
     * 
     * @param kafkaProperties
     */
    public static void createTopic(ZkUtils zkUtils, Map<String, Object> topicProperties)
    {
        if (zkUtils == null)
        {
            logger.error("zkUtil is null");
            return;
        }
        
        String topic = (String) topicProperties.get(CommonConstants.KAFKA_TOPIC_KEY);
        if (topic == null)
        {
            logger.error("topic should not be null");
            return;
        }
        
        int partitions = CommonConstants.KAFKA_PARTITIONS_DEFAULT;
        Object partitionsObj = topicProperties.get(CommonConstants.KAFKA_PARTITIONS_KEY);
        if (partitionsObj != null && (partitionsObj instanceof Integer))
        {
            partitions = (Integer) partitionsObj;
        }
        
        int replicationFactor = CommonConstants.KAFKA_REPLICATION_FACTOR_DEFAULT;
        Object replicationFactorObj = topicProperties.get(CommonConstants.KAFKA_REPLICATION_FACTOR_KEY);
        if (replicationFactorObj != null && (replicationFactorObj instanceof Integer))
        {
            replicationFactor = (Integer) replicationFactorObj;
        }
        
        AdminUtils.createTopic(zkUtils, topic, partitions, replicationFactor, new Properties());
    }
    
    /**
     * 当需要deleteTopic生效时，需要将
     * 
     * delete.topic.enable=true 加入到server.properties中
     * 
     * @param zkUtils
     * @param topicProperties
     */
    public static void deleteTopic(ZkUtils zkUtils, Map<String, Object> topicProperties)
    {
        if (zkUtils == null)
        {
            logger.error("zkUtil is null");
            return;
        }
        
        String topic = (String) topicProperties.get(CommonConstants.KAFKA_TOPIC_KEY);
        if (topic == null)
        {
            logger.error("topic should not be null");
            return;
        }
        AdminUtils.deleteTopic(zkUtils, topic);
    }
    
    public static boolean isTopicExist(ZkUtils zkUtils, Map<String, Object> kafkaProperties)
    {
        String topic = (String) kafkaProperties.get(CommonConstants.KAFKA_TOPIC_KEY);
        return AdminUtils.topicExists(zkUtils, topic);
    }
}
