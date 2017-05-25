package yanbinwa.common.kafka.producer;

import java.util.ArrayList;
import java.util.HashMap;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.concurrent.locks.ReentrantLock;

import org.apache.log4j.Logger;

import yanbinwa.common.kafka.message.KafkaMessage;

public class IKafkaProducerImpl implements IKafkaProducer
{
    private static final Logger logger = Logger.getLogger(IKafkaProducerImpl.class);
    
    Map<String, List<Integer>> topicToPartitionListMap = new HashMap<String, List<Integer>>();
    Map<Integer, String> partitionToTopicMap = new HashMap<Integer, String>();
    Set<String> topicList = new HashSet<String>();
    ReentrantLock lock = new ReentrantLock();
    
    Map<String, String> kafkaProducerProperties = null;
    
    int partitionMask;
    String producerName;
    
    boolean isRunning = false;
    IKafkaProducerWorker worker = null;
    
    IKafkaProducerImpl(Map<String, String> kafkaProducerProperties, Set<String>topicList, String producerName)
    {
        this(kafkaProducerProperties, topicList, producerName, IKafkaProducer.DEFAULT_PARTITION_NUM);
    }
    
    IKafkaProducerImpl(Map<String, String> kafkaProducerProperties, Set<String>topicList, String producerName, int partitionMask)
    {
        if(topicList != null)
        {
            topicList.addAll(topicList);
            logger.debug("Create kafkaProducer with topicList: " + topicList);
        }
        this.kafkaProducerProperties = kafkaProducerProperties;
        this.partitionMask = partitionMask;
        this.producerName = producerName;
        this.topicList.addAll(topicList);
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
    
    /**
     * 这里通过topicList来初始化topicToPartitionListMap和partitionToTopicMap
     * 
     */
    private void buildPartitionToTopicMap()
    {
        logger.info("Start build partition to topic map");
        lock.lock();
        try
        {
            Set<String> preTopic = topicToPartitionListMap.keySet();
            List<String> addTopicList = new ArrayList<String>();
            for (String topic : topicList)
            {
                if (!preTopic.contains(topic))
                {
                    addTopicList.add(topic);
                }
            }
            List<String> delTopicList = new ArrayList<String>();
            for (String topic : preTopic)
            {
                if (!topicList.contains(topic))
                {
                    delTopicList.add(topic);
                }
            }
            if (addTopicList.isEmpty() && delTopicList.isEmpty())
            {
                logger.info("Topic does not change");
                return;
            }
            int exchangeTopicNum = Math.min(addTopicList.size(), delTopicList.size());
            int i = 0;
            for(; i < exchangeTopicNum; i ++)
            {
                String addTopic = addTopicList.get(i);
                String delTopic = delTopicList.get(i);
                List<Integer> partitionKeyList = topicToPartitionListMap.get(delTopic);
                if (partitionKeyList == null)
                {
                    //非常严重的错误
                    logger.info("Delete topic partitionList should not be empty");
                    continue;
                }
                for(Integer partitionKey : partitionKeyList)
                {
                    String oldTopic = partitionToTopicMap.get(partitionKey);
                    if (!oldTopic.equals(delTopic))
                    {
                        logger.info("Delete topic not equal in the partitionToTopicMap");
                        continue;
                    }
                    partitionToTopicMap.put(partitionKey, addTopic);
                }
                topicToPartitionListMap.put(addTopic, partitionKeyList);
            }
            
            if (addTopicList.size() > i)
            {
                addTopicToMapping(i, addTopicList);
            }
            else if (delTopicList.size() > i)
            {
                removeTopicToMapping(i, delTopicList);
            }
        }
        finally
        {
            lock.unlock();
        }
        
    }
    
    private void addTopicToMapping(int startIndex, List<String> addTopicList)
    {
        lock.lock();
        try
        {
            Set<String> curTopicList =  topicToPartitionListMap.keySet();
            List<Integer> avaliablePartitionKey = new ArrayList<Integer>();
            
            int totleTopicNum = curTopicList.size() + addTopicList.size();
            int partitionNumForEachTopic = partitionMask / totleTopicNum;
            int needMigratePartitionNum = partitionNumForEachTopic * addTopicList.size();
            if (curTopicList.size() == 0)
            {
                logger.info("Build topic to partitionKey map from beginning");
                for (int i = 0; i < this.partitionMask; i ++)
                {
                    avaliablePartitionKey.add(i);
                }
            }
            else
            {
                if (totleTopicNum > partitionMask)
                {
                    logger.info("The topic num: " + totleTopicNum + " is larger than the partitionNum: " + partitionMask);
                }
                int migratePartitionNum = 0;
                while(migratePartitionNum < needMigratePartitionNum)
                {
                    for(String topic : curTopicList)
                    {
                        List<Integer> partitionKeyList = topicToPartitionListMap.get(topic);
                        if (partitionKeyList.size() > partitionNumForEachTopic)
                        {
                            int partitionKey = partitionKeyList.get(0);
                            partitionKeyList.remove(0);
                            partitionToTopicMap.put(partitionKey, null);
                            avaliablePartitionKey.add(partitionKey);
                            logger.trace("Migrate the partitionKey: " + partitionKey + " from topic: " + topic);
                            migratePartitionNum ++;
                            if (migratePartitionNum >= needMigratePartitionNum)
                            {
                                break;
                            }
                        }
                    }
                }
            }
            for(String topic : addTopicList)
            {
                for(int i = 0; i < partitionNumForEachTopic; i ++)
                {   
                    int partitionKey = avaliablePartitionKey.get(0);
                    avaliablePartitionKey.remove(0);
                    partitionToTopicMap.put(partitionKey, topic);
                    logger.trace("Migrate the partitionKey: " + partitionKey + " to topic: " + topic);
                    List<Integer> partitionKeyList = topicToPartitionListMap.get(topic);
                    if (partitionKeyList == null)
                    {
                        partitionKeyList = new ArrayList<Integer>();
                        topicToPartitionListMap.put(topic, partitionKeyList);
                    }
                    partitionKeyList.add(partitionKey);
                }
            }
        }
        finally
        {
            lock.unlock();
        }
    }
    
    private void removeTopicToMapping(int startIndex, List<String> delTopicList)
    {
        lock.lock();
        try
        {
            Set<String> curTopicList =  topicToPartitionListMap.keySet();
            List<Integer> avaliablePartitionKey = new ArrayList<Integer>();
            for(String topic : delTopicList)
            {
                List<Integer> partitionKeyList = topicToPartitionListMap.get(topic);
                if (partitionKeyList == null)
                {
                    logger.error("Exist topic partition key list should not be null");
                    continue;
                }
                topicToPartitionListMap.remove(topic);
                for (int partitionKey : partitionKeyList)
                {
                    avaliablePartitionKey.add(partitionKey);
                    partitionToTopicMap.put(partitionKey, null);
                    logger.trace("Remove partitionKey: " + partitionKey + " from topic: " + topic);
                }
            }
            
            curTopicList = topicToPartitionListMap.keySet();
            int totleTopicNum = curTopicList.size();
            if (totleTopicNum == 0)
            {
                logger.info("There is no topic in producer " + producerName);
                return;
            }
            int partitionNumForEachTopic = partitionMask / totleTopicNum;
            //先保证每个topic有partitionNumForEachTopic个partition
            for(String topic : curTopicList)
            {
                List<Integer> partitionKeyList = topicToPartitionListMap.get(topic);
                int leastAddNum = partitionNumForEachTopic - partitionKeyList.size();
                for(int i = 0; i < leastAddNum; i ++)
                {
                    int partitionKey = avaliablePartitionKey.get(0);
                    avaliablePartitionKey.remove(0);
                    partitionKeyList.add(partitionKey);
                    partitionToTopicMap.put(partitionKey, topic);
                    logger.info("Add partition key: " + partitionKey + " to topic: " + topic);
                }
            }
            //将多余的partitionkey再分到某些topic上
            for(String topic : curTopicList)
            {
                if (avaliablePartitionKey.size() > 0)
                {
                    int partitionKey = avaliablePartitionKey.get(0);
                    avaliablePartitionKey.remove(0);
                    List<Integer> partitionKeyList = topicToPartitionListMap.get(topic);
                    partitionKeyList.add(partitionKey);
                    partitionToTopicMap.put(partitionKey, topic);
                    logger.info("Add partition key: " + partitionKey + " to topic: " + topic);
                }
            }
        }
        finally
        {
            lock.unlock();
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
    public void updateTopic(Set<String> updateTopicList)
    {
        if (updateTopicList == null)
        {
            logger.error("Update topic list should not be null");
            return;
        }
        List<String> addTopic = new ArrayList<String>();
        for(String topic : updateTopicList)
        {
            if(!topicList.contains(topic))
            {
                addTopic.add(topic);
            }
        }
        List<String> delTopic = new ArrayList<String>();
        for(String topic : topicList)
        {
            if(!updateTopicList.contains(topic))
            {
                delTopic.add(topic);
            }
        }
        updateTopicList(updateTopicList);
        if (!addTopic.isEmpty() || !delTopic.isEmpty())
        {
            buildPartitionToTopicMap();
        }
    }
    
    private void updateTopicList(Set<String> updateTopicList)
    {
        lock.lock();
        try
        {
            topicList.clear();
            topicList.addAll(updateTopicList);
        }
        finally
        {
            lock.unlock();
        }
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
            topicToPartitionListMap.clear();
            partitionToTopicMap.clear();
        }
        else
        {
            logger.info("Kafka producer has already stopped");
        }
    }
}
