package yanbinwa.common.kafka.producer;


import java.util.HashMap;
import java.util.HashSet;
import java.util.Map;
import java.util.Set;

import org.junit.Test;

import yanbinwa.common.kafka.message.KafkaMessage;

public class IKafkaProducerImplTest
{

    @Test
    public void test()
    {
        Map<String, String> kafkaProperties = new HashMap<String, String>();
        
        kafkaProperties.put(IKafkaProducer.BROKER_LIST_KEY, "192.168.56.17:9092");
        kafkaProperties.put(IKafkaProducer.MAX_BLOCK_MS, "1000");
        kafkaProperties.put(IKafkaProducer.RETRY_TIMES_KEY, "0");
        IKafkaProducer producer = new IKafkaProducerImpl(kafkaProperties, "testProducer");
        System.out.println("Show producer map: " + producer.printTopicMapping());
        
        Map<String, Set<Integer>> topicToPartitionKeyMap = new HashMap<String, Set<Integer>>();
        Set<Integer> topic1PartitionSet = new HashSet<Integer>();
        topic1PartitionSet.add(0);
        topic1PartitionSet.add(1);
        
        topicToPartitionKeyMap.put("wyb", topic1PartitionSet);
        
        Set<Integer> topic2PartitionSet = new HashSet<Integer>();
        topic2PartitionSet.add(2);
        topic2PartitionSet.add(3);
        
        topicToPartitionKeyMap.put("zcl", topic2PartitionSet);
        producer.updateTopicToPartitionSetMap(topicToPartitionKeyMap);
        
        System.out.println("Show producer map: " + producer.printTopicMapping());
        producer.start();
        KafkaMessage msg = new KafkaMessage(1, "Hello World");
        producer.sendMessage(msg);
        producer.sendMessage(msg);
        
        try
        {
            Thread.sleep(5000);
        } 
        catch (InterruptedException e)
        {
            e.printStackTrace();
        }
        
        producer.stop();
        producer.stop();
    }

}
