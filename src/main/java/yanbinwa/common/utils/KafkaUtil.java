package yanbinwa.common.utils;

import java.util.HashMap;
import java.util.Map;

import org.apache.log4j.Logger;

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
}
