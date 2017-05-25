package yanbinwa.common.kafka.consumer;

import java.util.Map;
import org.apache.log4j.Logger;

/**
 * 
 * Topic的创建是不需要显示执行的,当producer或者consumer执行时，就直接创建了
 * 
 * @author yanbinwa
 *
 */

public class IKafkaConsumerImpl implements IKafkaConsumer
{
    private static final Logger logger = Logger.getLogger(IKafkaConsumerImpl.class);
    String consumerName;
    String topic;
    
    Map<String, String> kafkaConsumerProperties = null;
    
    boolean isRunning = false;
    IKafkaConsumerWorker worker = null;
    IKafkaCallBack callback = null;
    
    public IKafkaConsumerImpl(Map<String, String> kafkaConsumerProperties, String consumerName, IKafkaCallBack callback)
    {
        this.kafkaConsumerProperties = kafkaConsumerProperties;
        this.consumerName = consumerName;
        this.callback = callback;
    }
    
    private void createConsumerWorker()
    {
        worker = new IKafkaConsumerWorker(kafkaConsumerProperties, callback);
    }
    
    private void closeConsumerWorker()
    {
        if (worker != null)
        {
            worker.shutdown();
            worker = null;
        }
    }
    
    @Override
    public void start()
    {
        if(!isRunning)
        {
            isRunning = true;
            createConsumerWorker();
        }
        else
        {
            logger.info("Kafka consumer has ready started");
        }
    }

    @Override
    public void stop()
    {
        if(isRunning)
        {
            isRunning = false;
            closeConsumerWorker();
        }
        else
        {
            logger.info("Kafka consumer has ready started");
        }
    }

}
