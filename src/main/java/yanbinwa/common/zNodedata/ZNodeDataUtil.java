package yanbinwa.common.zNodedata;

import java.util.Map;
import java.util.Set;

import org.apache.log4j.Logger;
import org.json.JSONObject;

import yanbinwa.common.constants.CommonConstants;
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
    
    public static ZNodeServiceData getZnodeData(Map<String, Object> propMap)
    {
        if(propMap == null)
        {
            return null;
        }
        return new ZNodeServiceDataImpl(new JSONObject(propMap));
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
    
    public static boolean validateServiceConfigProperties(ZNodeServiceData serviceData, JSONObject prop)
    {
        if (prop == null || serviceData == null)
        {
            logger.error("serviceData or prop should not be null");
            return false;
        }
        //Check serviceDataProperites
        if (!prop.has(CommonConstants.SERVICE_DATA_PROPERTIES_KEY))
        {
            logger.error("prop should contain element serviceDataProperites");
            return false;
        }
        JSONObject serviceDataProperitesObj = prop.getJSONObject(CommonConstants.SERVICE_DATA_PROPERTIES_KEY);
        if (serviceDataProperitesObj == null)
        {
            logger.error("serviceDataProperitesObj should not be null");
            return false;
        }
        ZNodeServiceData serviceDataTmp = ZNodeDataUtil.getZnodeData(serviceDataProperitesObj);
        if (serviceDataTmp == null && serviceDataTmp != serviceData)
        {
            logger.error("serviceDataTmp is null or not equal to serviceData. serviceDataTmp is: " + serviceDataTmp
                    + "; serviceData is: " + serviceData);
        }
        return true;
    }
}
