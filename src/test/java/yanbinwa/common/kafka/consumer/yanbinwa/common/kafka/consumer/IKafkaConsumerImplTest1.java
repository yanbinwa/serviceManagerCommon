//package yanbinwa.common.kafka.consumer;
//
//import static org.junit.Assert.*;
//
//import java.util.HashMap;
//import java.util.HashSet;
//import java.util.Map;
//import java.util.Set;
//
//import org.junit.Test;
//
//import yanbinwa.common.kafka.message.KafkaMessage;
//import yanbinwa.common.kafka.producer.IKafkaProducer;
//import yanbinwa.common.kafka.producer.IKafkaProducerImpl;
//
//public class IKafkaConsumerImplTest1
//{
//
//    private void sendKafkaMessage(IKafkaProducer producer)
//    {
//        KafkaMessage msg = new KafkaMessage(1, "Hello World");
//        while(true)
//        {
//            producer.sendMessage(msg);
//            try
//            {
//                Thread.sleep(1000);
//            } 
//            catch (InterruptedException e)
//            {
//                e.printStackTrace();
//            }
//        }
//    }
//    
//    class IKafkaCallBackImplTest implements IKafkaCallBack
//    {
//        @Override
//        public void handleOnData(KafkaMessage msg)
//        {
//            System.out.println("Get the message " + msg);
//        }
//    }
//    
//    @Test
//    public void test()
//    {
//        Map<String, String> kafkaProducerProperties = new HashMap<String, String>();
//        
//        kafkaProducerProperties.put(IKafkaProducer.BROKER_LIST_KEY, "192.168.56.17:9092");
//        kafkaProducerProperties.put(IKafkaProducer.MAX_BLOCK_MS, "100000");
//        kafkaProducerProperties.put(IKafkaProducer.RETRY_TIMES_KEY, "0");
//        IKafkaProducer producer = new IKafkaProducerImpl(kafkaProducerProperties, "testProducer");
//        
//        Map<String, Set<Integer>> topicToPartitionKeyMap = new HashMap<String, Set<Integer>>();
//        Set<Integer> topic1PartitionSet = new HashSet<Integer>();
//        topic1PartitionSet.add(0);
//        topic1PartitionSet.add(1);
//        topicToPartitionKeyMap.put("wyb", topic1PartitionSet);
//        
//        producer.updateTopicToPartitionSetMap(topicToPartitionKeyMap);
//        producer.start();
//        
//        Thread sendThread = new Thread(new Runnable(){
//
//            @Override
//            public void run()
//            {
//                sendKafkaMessage(producer);
//            }
//            
//        });
//        
//        sendThread.start();
//        
//        Map<String, String> kafkaConsumerProperties = new HashMap<String, String>();
//        kafkaProperties.put(IKafkaConsumer.ZOOKEEPER_HOST_PORT_KEY, "192.168.56.17:2181");
//        kafkaConsumerProperties.put(IKafkaConsumer.BROKER_LIST_KEY, "192.168.56.17:9092");
//        kafkaConsumerProperties.put(IKafkaConsumer.MAX_BLOCK_MS_KEY, "1000");
//        kafkaConsumerProperties.put(IKafkaConsumer.GROUP_ID_KEY, "test");
//        kafkaConsumerProperties.put(IKafkaConsumer.LISTEN_TOPIC_KEY, "wyb");
//        IKafkaCallBack callback = new IKafkaCallBackImplTest();
//        IKafkaConsumer consumer = new IKafkaConsumerImpl(kafkaConsumerProperties, "testConsumer", callback);
//        consumer.start();
//        
//        try
//        {
//            sendThread.join();
//        } 
//        catch (InterruptedException e)
//        {
//            e.printStackTrace();
//        }
//    }
//
//}
