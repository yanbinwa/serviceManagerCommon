package yanbinwa.common.kafka.consumer;

import java.util.ArrayList;
import java.util.Iterator;
import java.util.List;
import java.util.Map;
import java.util.Properties;

import org.apache.kafka.clients.consumer.ConsumerRecord;
import org.apache.kafka.clients.consumer.ConsumerRecords;
import org.apache.kafka.clients.consumer.KafkaConsumer;
import org.apache.log4j.Logger;

import yanbinwa.common.kafka.message.KafkaMessage;

public class IKafkaConsumerWorker
{
    private static final Logger logger = Logger.getLogger(IKafkaConsumerWorker.class);
    Properties props = null;
    IKafkaCallBack callback = null;
    String listenTopic = null;
    KafkaConsumer<Object, Object> consumer = null;
    boolean isRunning = false;
    Thread kafkaPollThread = null;
    
    public IKafkaConsumerWorker(Map<String, String> kafkaConsumerProperties, IKafkaCallBack callback)
    {
        if(kafkaConsumerProperties == null)
        {
            throw new IllegalArgumentException("kafkaConsumerProperties should not be null");
        }
        listenTopic = kafkaConsumerProperties.get(IKafkaConsumer.LISTEN_TOPIC_KEY);
        if (listenTopic == null)
        {
            throw new IllegalArgumentException("Listen topic should not be null");
        }
        
        this.callback = callback;
        
        props = new Properties();
        props.put("key.deserializer", IKafkaConsumer.DEFAULT_KAFKA_KEY_DESERIALIZER_CLASS);
        props.put("enable.auto.commit", "true");
        props.put("auto.commit.interval.ms", "1000");
        props.put("session.timeout.ms", "10000");
        
        String brokerList = (String)kafkaConsumerProperties.get(IKafkaConsumer.BROKER_LIST_KEY);
        if(null == brokerList)
        {
            throw new IllegalArgumentException("Block servers shoud not be empty");
        }
        props.put("bootstrap.servers", brokerList);
        
        String groupId = kafkaConsumerProperties.get(IKafkaConsumer.GROUP_ID_KEY);
        if(null == groupId)
        {
            throw new IllegalArgumentException("Group id shoud not be empty");
        }
        props.put("group.id", groupId);
        
        
        String deserializerClass = (String)kafkaConsumerProperties.get(IKafkaConsumer.DESERIALIZER_CLASS_KEY);
        if(null == deserializerClass)
        {
            deserializerClass = IKafkaConsumer.DEFAULT_KAFKA_VALUE_DESERIALIZER_CLASS;
        }
        props.put("value.deserializer", deserializerClass);
        
        start();
    }
    
    private void buildKafkaConsumer()
    {
        if (props == null)
        {
            logger.info("Kafka propertie should not be null");
            return;
        }
        logger.info("Kafka properties is " + props);
        consumer = new KafkaConsumer<Object, Object>(props);
    }
    
    public void start()
    {
        if (!isRunning)
        {
            logger.info("Kafka consumer start ...");
            isRunning = true;
            buildKafkaConsumer();
            kafkaPollThread = new Thread(new Runnable() {

                @Override
                public void run()
                {
                    pollKafkaMessage();
                }
                
            });
            kafkaPollThread.start();
        }
        else
        {
            logger.info("Kakfa consumer has already started");
        }
    }
    
    private void pollKafkaMessage()
    {
        logger.info("start poll kafka message");
        List<String> topicList = new ArrayList<String>();
        topicList.add(this.listenTopic);
        consumer.subscribe(topicList);
        logger.info("subscribe topiclist: " + topicList);
        
        while(isRunning)
        {
            ConsumerRecords<Object, Object> records = consumer.poll(IKafkaConsumer.KAFKA_POLL_TIMEOUT);
            if (records == null || records.isEmpty())
            {
                logger.info("the records is empty");
                continue;
            }
            Iterator<ConsumerRecord<Object, Object>> iterator = records.iterator();
            while(iterator.hasNext())
            {
                ConsumerRecord<Object, Object> record = iterator.next();
                KafkaMessage msg = new KafkaMessage();
                msg.setTopic(record.topic());
                msg.setPayLoad(record.value());
                Object key = record.key();
                if (key != null)
                {
                    msg.setPartitionCode((Integer)key);
                }
                logger.trace("Get msg from kafka: " + msg.toString());
                if (callback != null)
                {
                    callback.handleOnData(msg);
                }
            }
        }
        //这里要在consumer创建的线程中关闭，因为consumer不是线程安全的
        consumer.close();
        consumer = null;
    }
    
    public void shutdown()
    {
        if (isRunning)
        {
            isRunning = false;
            kafkaPollThread.interrupt();
            kafkaPollThread = null;
        }
        else
        {
            logger.info("Kakfa consumer has already stopped");
        }
    }
}
