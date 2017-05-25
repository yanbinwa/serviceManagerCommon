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
        Set<String> topicList = new HashSet<String>();
        topicList.add("wyb");
        topicList.add("zjy");
        
        Map<String, String> kafkaProperties = new HashMap<String, String>();
        
        kafkaProperties.put(IKafkaProducer.BROKER_LIST_KEY, "192.168.56.17:9092");
        kafkaProperties.put(IKafkaProducer.MAX_BLOCK_MS, "1000");
        kafkaProperties.put(IKafkaProducer.RETRY_TIMES_KEY, "0");
        IKafkaProducer producer = new IKafkaProducerImpl(kafkaProperties, topicList, "testProducer");
        producer.start();
        System.out.println("Show producer map: " + producer.printTopicMapping());
        
        topicList.add("wzy");
        topicList.add("zcl");
        producer.updateTopic(topicList);
        
        System.out.println("Show producer map: " + producer.printTopicMapping());
        
        topicList.remove("zjy");
        topicList.remove("zcl");
        topicList.remove("wzy");
        producer.updateTopic(topicList);
        
        System.out.println("Show producer map: " + producer.printTopicMapping());
        
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
