package yanbinwa.common.kafka.producer;

import java.util.HashMap;
import java.util.Map;
import java.util.Set;

import org.apache.log4j.Logger;

import yanbinwa.common.kafka.message.KafkaMessage;

public class IKafkaProducerImpl implements IKafkaProducer
{
    private static final Logger logger = Logger.getLogger(IKafkaProducerImpl.class);
    
    Map<String, Set<Integer>> topicToPartitionSetMap = null;
    Map<Integer, String> partitionToTopicMap = new HashMap<Integer, String>();
    Map<String, String> kafkaProducerProperties = null;
    
    String producerName;
    boolean isRunning = false;
    IKafkaProducerWorker worker = null;
    
    int partitionMask = 0;
    
    public IKafkaProducerImpl(Map<String, String> kafkaProducerProperties, String producerName)
    {        
        this.kafkaProducerProperties = kafkaProducerProperties;
        this.producerName = producerName;
    }
    
    private void createProducerWorker()
    {
        if (worker != null)
        {
            worker.shutdown();
        }
        worker = new IKafkaProducerWorker(kafkaProducerProperties);
    }
    
    private void closeProducerWorker()
    {
        if (worker != null)
        {
            worker.shutdown();
            worker = null;
        }
    }
    
    @Override
    public boolean sendMessage(KafkaMessage msg)
    {
        if (worker == null)
        {
            logger.error("Kafka producer is null. Can not send msg: " + msg);
            return false;
        }
        int partitionKey = msg.getPartitionCode() % partitionMask;
        String topic = partitionToTopicMap.get(partitionKey);
        if (topic == null)
        {
            logger.error("Topic is null. Can not send msg: " + msg);
            return false;
        }
        msg.setTopic(topic);
        worker.sendMessage(msg);
        return true;
    }

    @Override
    public void updateTopicToPartitionSetMap(Map<String, Set<Integer>> topicToPartitionSetMap)
    {
        if (topicToPartitionSetMap == null)
        {
            logger.error("Update topic list should not be null");
            return;
        }
        this.topicToPartitionSetMap = topicToPartitionSetMap;
        buildPartitionToTopicMap();
    }

    @Override
    public String printTopicMapping()
    {
        if (partitionToTopicMap == null)
        {
            return null;
        }
        else
        {
            return partitionToTopicMap.toString();
        }
    }

    @Override
    public void start()
    {
        if (!isRunning)
        {
            if (topicToPartitionSetMap == null)
            {
                logger.error("Cound not start the producer " + producerName + " because the topicToPartitionSetMap is empty");
                return;
            }
            isRunning = true;
            createProducerWorker();
            buildPartitionToTopicMap();
        }
        else
        {
            logger.info("Kafka producer has already started");
        }
    }

    @Override
    public void stop()
    {
        if(isRunning)
        {
            isRunning = false;
            closeProducerWorker();
        }
        else
        {
            logger.info("Kafka producer has already stopped");
        }
    }
    
    private void buildPartitionToTopicMap()
    {
        partitionToTopicMap.clear();
        partitionMask = 0;
        for(Map.Entry<String, Set<Integer>> entry : topicToPartitionSetMap.entrySet())
        {
            String topic = entry.getKey();
            Set<Integer> partitionKeySet = entry.getValue();
            for (Integer partitionKey : partitionKeySet)
            {
                partitionToTopicMap.put(partitionKey, topic);
                partitionMask ++;
            }
        }
    }
}
