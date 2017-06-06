package yanbinwa.common.zNodedata;

import java.util.Map;
import java.util.Set;

import org.json.JSONObject;

import yanbinwa.common.constants.CommonConstants;

public class ZNodeDependenceDataWithKafkaTopic extends ZNodeDependenceData
{

    KafkaTopicMappingData kafkaTopicMappingData = new KafkaTopicMappingData();
    
    public ZNodeDependenceDataWithKafkaTopic(JSONObject obj)
    {
        super();
        loadFromJsonObject(obj);
    }
    
    public ZNodeDependenceDataWithKafkaTopic(Map<String, Set<ZNodeServiceData>> dependenceData, Map<String, Map<String, Set<Integer>>> topicGroupNameToTopicNameToPartitionKeyMap)
    {
        super(dependenceData);
        kafkaTopicMappingData = new KafkaTopicMappingData(topicGroupNameToTopicNameToPartitionKeyMap);
    }
    
    public JSONObject createJsonObject()
    {
        if(dependenceData == null || kafkaTopicMappingData == null)
        {
            return null;
        }
        
        JSONObject retObj = new JSONObject();
        retObj.put(CommonConstants.DATA_DEPENDENCE_DATA_KEY, super.createJsonObject());
        retObj.put(CommonConstants.DATA_KAFKA_TOPIC_DATA_KEY, kafkaTopicMappingData.createJsonObject());

        return retObj;
    }
    
    public KafkaTopicMappingData getKafkaTopicMappingData()
    {
        return kafkaTopicMappingData;
    }

    public void loadFromJsonObject(JSONObject obj)
    {
        JSONObject zNodeDependenceDataObj = obj.getJSONObject(CommonConstants.DATA_DEPENDENCE_DATA_KEY);
        super.loadFromJsonObject(zNodeDependenceDataObj);
        
        JSONObject kafkaTopicDataObj = obj.getJSONObject(CommonConstants.DATA_KAFKA_TOPIC_DATA_KEY);
        kafkaTopicMappingData.loadFromJsonObject(kafkaTopicDataObj);
    }
    
    @Override
    public boolean equals(Object obj)
    {
        if (obj == null)
        {
            return false;
        }
        if (!(obj instanceof ZNodeDependenceDataWithKafkaTopic))
        {
            return false;
        }
        ZNodeDependenceDataWithKafkaTopic other = (ZNodeDependenceDataWithKafkaTopic)obj;
        if(this.createJsonObject().toString().equals(other.createJsonObject().toString()))
        {
            return true;
        }
        else
        {
            return false;
        }
    }
    
    @Override
    public int hashCode()
    {
        return createJsonObject().toString().hashCode();
    }
    
    @Override
    public String toString()
    {
        return createJsonObject().toString();
    }
}
