package yanbinwa.common.zNodedata;

import java.util.Map;
import java.util.Set;

import org.apache.log4j.Logger;
import org.json.JSONObject;

import yanbinwa.common.constants.CommonConstants;

public class ZNodeDataUtil
{
    private static final Logger logger = Logger.getLogger(ZNodeDataUtil.class);
    
    public static ZNodeServiceData getZnodeData(JSONObject obj)
    {
        if (obj == null)
        {
            return null;
        }
        if(obj.has(CommonConstants.DATA_TOPIC_INFO_KEY))
        {
            return new ZNodeServiceDataWithKafkaTopicImpl(obj);
        }
        else
        {
            return new ZNodeServiceDataImpl(obj);
        }
    }
    
    public static ZNodeDependenceData getZNodeDependenceData(JSONObject obj)
    {
        if (obj == null)
        {
            return null;
        }
        if(!obj.has(CommonConstants.DATA_DEPENDENCE_DATA_KEY))
        {
            return new ZNodeDependenceData(obj);
        }
        else
        {
            if (obj.has(CommonConstants.DATA_KAFKA_TOPIC_DATA_KEY))
            {
                return new ZNodeDependenceDataWithKafkaTopic(obj);
            }
            else
            {
                logger.error("Does not support other type of dependence data");
                return null;
            }
        }
    }
    
    public static Map<String, Map<String, Set<Integer>>> getTopicGroupToTopicNameToPartitionKeyMap(ZNodeDependenceData depData)
    {
        if (depData == null || !(depData instanceof ZNodeDependenceDataWithKafkaTopic))
        {
            logger.error("depData should not be null or the type ZNodeDependenceDataWithKafkaTopic");
            return null;
        }
        ZNodeDependenceDataWithKafkaTopic zNodeDependenceDataWithKafkaTopic = (ZNodeDependenceDataWithKafkaTopic) depData;
        return zNodeDependenceDataWithKafkaTopic.getKafkaTopicMappingData().getTopicGroupNameToTopicNameToPartitionKeyMap();
    }
    
    public Map<String, Map<String, Set<Integer>>> getTopicPartitionKeyMap(ZNodeDependenceDataWithKafkaTopic zNodeDependenceDataWithKafkaTopic)
    {
        if (zNodeDependenceDataWithKafkaTopic == null)
        {
            return null;
        }
        return zNodeDependenceDataWithKafkaTopic.getKafkaTopicMappingData().getTopicGroupNameToTopicNameToPartitionKeyMap();
    }
}
