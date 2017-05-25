package yanbinwa.common.kafka.message;

public class KafkaMessage
{
    int partitionCode;
    
    Object payLoad;
    
    String topic;
    
    public KafkaMessage()
    {
        
    }
    
    public KafkaMessage(int partitionCode, Object payLoad)
    {
        this.partitionCode = partitionCode;
        this.payLoad = payLoad;
    }
    
    public int getPartitionCode()
    {
        return this.partitionCode;
    }
    
    public void setPartitionCode(int partitionCode)
    {
        this.partitionCode = partitionCode;
    }
    
    public Object getPayLoad()
    {
        return this.payLoad;
    }
    
    public void setPayLoad(Object payLoad)
    {
        this.payLoad = payLoad;
    }
    
    public String getTopic()
    {
        return this.topic;
    }
    
    public void setTopic(String topic)
    {
        this.topic = topic;
    }
    
    @Override
    public String toString()
    {
        String msg = "partitionCode is: " + partitionCode + "; " + 
                     "payLoad is: " + payLoad + "; " +
                     "topic is: " + topic;
        return msg;
    }
}
