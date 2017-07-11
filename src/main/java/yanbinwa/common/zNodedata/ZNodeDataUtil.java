package yanbinwa.common.zNodedata;

import java.util.Map;
import java.util.Set;

import org.apache.log4j.Logger;
import org.json.JSONObject;

import yanbinwa.common.zNodedata.decorate.ZNodeDecorateType;
import yanbinwa.common.zNodedata.decorate.ZNodeDependenceDataDecorateKafka;
import yanbinwa.common.zNodedata.decorate.ZNodeDependenceDataDecorateRedis;

public class ZNodeDataUtil
{
    private static final Logger logger = Logger.getLogger(ZNodeDataUtil.class);
    
    public static ZNodeServiceData getZnodeData(JSONObject obj)
    {
        if (obj == null)
        {
            return null;
        }
        return new ZNodeServiceDataImpl(obj);
    }
    
    public static ZNodeDependenceData getZNodeDependenceData(JSONObject obj)
    {
        if (obj == null)
        {
            return null;
        }
        return new ZNodeDependenceDataImpl(obj);
    }
    
    public static Map<String, Map<String, Set<Integer>>> getTopicGroupToTopicNameToPartitionKeyMap(ZNodeDependenceData depData)
    {
        if (depData == null || !(depData.isContainedDecorate(ZNodeDecorateType.KAFKA)))
        {
            logger.error("depData should not be null or the type ZNodeDecorateType.KAFKA");
            return null;
        }
        ZNodeDependenceDataDecorateKafka zNodeDependenceDataDecorateKafka = (ZNodeDependenceDataDecorateKafka)depData.getDependenceDataDecorate(ZNodeDecorateType.KAFKA);
        return zNodeDependenceDataDecorateKafka.getTopicGroupNameToTopicNameToPartitionKeyMap();
    }
    
    public static Map<String, Set<Integer>> getRedisServiceNameToPartitionMap(ZNodeDependenceData depData)
    {
        if (depData == null || !(depData.isContainedDecorate(ZNodeDecorateType.REDIS)))
        {
            logger.error("depData should not be null or the type ZNodeDecorateType.REDIS " + depData);
            return null;
        }
        ZNodeDependenceDataDecorateRedis zNodeDependenceDataDecorateRedis = (ZNodeDependenceDataDecorateRedis)depData.getDependenceDataDecorate(ZNodeDecorateType.REDIS);
        return zNodeDependenceDataDecorateRedis.getRedisServiceNameToPartitionMap();
    }
}
