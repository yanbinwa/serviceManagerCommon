package yanbinwa.common.kafka.consumer;

import java.util.HashMap;
import java.util.Map;

import org.junit.Test;

import yanbinwa.common.kafka.message.KafkaMessage;

public class IKafkaConsumerImplTest
{    
    class IKafkaCallBackImplTest implements IKafkaCallBack
    {
        @Override
        public void handleOnData(KafkaMessage msg)
        {
            System.out.println("Get the message " + msg);
        }
    }
    
    @Test
    public void test()
    {
        Map<String, String> kafkaProperties = new HashMap<String, String>();
        kafkaProperties.put(IKafkaConsumer.ZOOKEEPER_HOST_PORT_KEY, "192.168.56.17:2181");
        kafkaProperties.put(IKafkaConsumer.BROKER_LIST_KEY, "192.168.56.17:9092");
        kafkaProperties.put(IKafkaConsumer.MAX_BLOCK_MS_KEY, "1000");
        kafkaProperties.put(IKafkaConsumer.GROUP_ID_KEY, "test");
        kafkaProperties.put(IKafkaConsumer.LISTEN_TOPIC_KEY, "wyb");
        IKafkaCallBack callback = new IKafkaCallBackImplTest();
        IKafkaConsumer consumer = new IKafkaConsumerImpl(kafkaProperties, "testConsumer", callback);
        consumer.start();
        
        try
        {
            Thread.sleep(10 * 1000);
        } 
        catch (InterruptedException e)
        {
            e.printStackTrace();
        }
        
        consumer.stop();
    }

}