package yanbinwa.common.kafka.producer;

import java.util.Map;
import java.util.Properties;
import java.util.concurrent.ArrayBlockingQueue;
import java.util.concurrent.BlockingQueue;

import org.apache.kafka.clients.producer.Callback;
import org.apache.kafka.clients.producer.KafkaProducer;
import org.apache.kafka.clients.producer.ProducerRecord;
import org.apache.kafka.clients.producer.RecordMetadata;
import org.apache.kafka.common.errors.TimeoutException;
import org.apache.log4j.Logger;

import yanbinwa.common.kafka.message.KafkaMessage;


/**
 * 
 * 创建producer session，将数据转换为ProducerRecord，发送到特定的topic中
 * 
 * @author yanbinwa
 *
 */
public class IKafkaProducerWorker
{

    KafkaProducer<Object, Object> producer = null;
    private static final Logger logger = Logger.getLogger(IKafkaProducerWorker.class);
    KafkaCallBack callback = new KafkaCallBack();
    BlockingQueue<KafkaMessage> sendQueue = new ArrayBlockingQueue<KafkaMessage>(IKafkaProducer.KAFKA_PRODUCER_WORKER_QUEUE_SIZE);
    
    Properties props = null;
    Thread sendKafkaMessageThread = null;
    boolean isRunning = false;
    
    public IKafkaProducerWorker(Map<String, String> kafkaProducerProperties)
    {
        
        if(kafkaProducerProperties == null)
        {
            throw new IllegalArgumentException("kafkaProducerProperties should not be null");
        }
        
        props = new Properties();
        props.put("buffer.memory", 10000);
        props.put("acks", "all");
        props.put("key.serializer", IKafkaProducer.DEFAULT_KAFKA_KEY_SERIALIZER_CLASS);
        
        String brokerList = (String)kafkaProducerProperties.get(IKafkaProducer.BROKER_LIST_KEY);
        if(null == brokerList)
        {
            throw new IllegalArgumentException("No Zookeeper Connection Defined : " + kafkaProducerProperties);
        }
        props.put("bootstrap.servers", brokerList);
        
        String serializerClass = (String)kafkaProducerProperties.get(IKafkaProducer.SERIALIZER_CLASS_KEY);
        if(null == serializerClass)
        {
            serializerClass = IKafkaProducer.DEFAULT_KAFKA_VALUE_SERIALIZER_CLASS;
        }
        props.put("value.serializer", serializerClass);
        
        String batchSizeStr = kafkaProducerProperties.get(IKafkaProducer.BATCH_SIZE_KEY);
        int batchSize = IKafkaProducer.DEFAULT_KAFKA_BATCH_SIZE;
        if(null != batchSizeStr)
        {
            batchSize = Integer.parseInt(batchSizeStr);
        }
        props.put("batch.size", batchSize);
        
        String retriesStr = kafkaProducerProperties.get(IKafkaProducer.RETRY_TIMES_KEY);
        int retries = IKafkaProducer.DEFAULT_RETRY_TIMES;
        if(null != retriesStr)
        {
            retries = Integer.parseInt(retriesStr);
        }
        props.put("retries", retries);
        
        //option
        String maxBlockMsStr = (String)kafkaProducerProperties.get(IKafkaProducer.MAX_BLOCK_MS);
        if(null != maxBlockMsStr)
        {
            props.put("max.block.ms", Integer.parseInt(maxBlockMsStr));
        }
        
        start();
    }
    
    private void buildKafkaProducer()
    {
        if (props == null)
        {
            logger.info("Kafka propertie should not be null");
            return;
        }
        logger.info("Kafka properties is " + props);
        producer = new KafkaProducer<Object, Object>(props);
    }
    
    public void sendKafkaMessage()
    {
        logger.info("Start to send the kafka message...");
        while(isRunning)
        {
            KafkaMessage msg = sendQueue.peek();
            if (msg == null)
            {
                try
                {
                    Thread.sleep(IKafkaProducer.KAFKA_PRODUCER_WORKER_QUEUE_SLEEP);
                    continue;
                } 
                catch (InterruptedException e)
                {
                    if(!isRunning)
                    {
                        logger.info("Close the kafka producer worker thread");
                        return;
                    }
                    else
                    {
                        e.printStackTrace();
                    }
                }
            }
            logger.trace("Try to send msg " + msg);
            ProducerRecord<Object, Object> record = new ProducerRecord<Object, Object>(msg.getTopic(), msg.getPartitionCode(), msg.getPayLoad());
            producer.send(record, callback);
            //连接错误，需要重连
            if (callback.isTimeout())
            {
                producer.close();
                try
                {
                    Thread.sleep(IKafkaProducer.KAFKA_PRODUCER_TIMEOUT_SLEEP);
                } 
                catch (InterruptedException e)
                {
                    if(!isRunning)
                    {
                        logger.info("Close the kafka producer worker thread");
                    }
                    else
                    {
                        e.printStackTrace();
                    }
                }
                buildKafkaProducer();
                continue;
            }
            //发送正常
            else
            {
                sendQueue.poll();
            }
        }        
    }
    
    public void sendMessage(KafkaMessage msg)
    {   
        try
        {
            sendQueue.put(msg);
        } 
        catch (InterruptedException e)
        {
            e.printStackTrace();
        }
    }
    
    public void start()
    {
        if (!isRunning)
        {
            isRunning = true;
            buildKafkaProducer();
            sendKafkaMessageThread = new Thread(new Runnable() {

                @Override
                public void run()
                {
                    sendKafkaMessage();
                }
                
            });
            sendKafkaMessageThread.start();
        }
        else
        {
            logger.info("Kakfa producer has already started");
        }
    }
    
    public void shutdown()
    {
        if (isRunning)
        {
            isRunning = false;
            producer.close();
            producer = null;
            sendKafkaMessageThread.interrupt();
            sendKafkaMessageThread = null;
        }
        else
        {
            logger.info("Kakfa producer has already stopped");
        }
    }
    
    class KafkaCallBack implements Callback
    {
        
        boolean isTimeout = false;
        
        public boolean isTimeout()
        {
            return isTimeout;
        }
        
        @Override
        public void onCompletion(RecordMetadata metadata, Exception exception)
        {
            logger.trace("Metadata: " + metadata);
            if (exception instanceof TimeoutException)
            {
                isTimeout = true;
                logger.error("Exception: " + exception);
            }
            else
            {
                isTimeout = false;
            }
        }
    }
}
