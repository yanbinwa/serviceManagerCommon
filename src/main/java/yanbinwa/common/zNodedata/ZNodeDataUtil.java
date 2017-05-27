package yanbinwa.common.zNodedata;

import java.util.HashMap;
import java.util.HashSet;
import java.util.Map;
import java.util.Set;

import org.apache.log4j.Logger;
import org.json.JSONArray;
import org.json.JSONObject;

import yanbinwa.common.constants.CommonConstants;

public class ZNodeDataUtil
{
    private static final Logger logger = Logger.getLogger(ZNodeDataUtil.class);
    
    public static ZNodeData getZnodeData(JSONObject obj)
    {
        if (obj == null)
        {
            return null;
        }
        if(obj.has(CommonConstants.DATA_CONSUMER_TOPIC_INFO_KEY))
        {
            return new ZNodeServiceDataWithKafkaTopic(obj);
        }
        else
        {
            return new ZNodeServiceData(obj);
        }
    }
    
    public static Map<String, Set<String>> getTopicGroupInfo(ZNodeDependenceData depData)
    {
        if (depData == null)
        {
            return null;
        }
        Map<String, Set<ZNodeData>> dependenceData = depData.getDependenceData();
        Map<String, Set<String>> topicGroupToTopicMap = new HashMap<String, Set<String>>();
        for(String serviceGroup : dependenceData.keySet())
        {
            Set<ZNodeData> serviceDataSet = dependenceData.get(serviceGroup);
            for(ZNodeData serviceData : serviceDataSet)
            {
                if(serviceData instanceof ZNodeServiceDataWithKafkaTopic)
                {
                    ZNodeServiceDataWithKafkaTopic zNodeServiceDataWithKafkaTopic = (ZNodeServiceDataWithKafkaTopic)serviceData;
                    String topicInfoStr = zNodeServiceDataWithKafkaTopic.getConsumerTopicInfo();
                    if (topicInfoStr == null)
                    {
                        logger.error("Topic info should not be null. ServiceGroup: " + serviceGroup + " and Service: " + serviceData);
                        continue;
                    }
                    JSONObject topicInfo = new JSONObject(topicInfoStr);
                    for(Object topicGroupObj : topicInfo.keySet())
                    {
                        if(topicGroupObj == null)
                        {
                            continue;
                        }
                        if (!(topicGroupObj instanceof String))
                        {
                            logger.error("Topic group should be String: " + topicGroupObj);
                            continue;
                        }
                        String topicGroup = (String)topicGroupObj;
                        JSONArray topicList = topicInfo.getJSONArray(topicGroup);
                        Set<String> topicSet = topicGroupToTopicMap.get(topicGroup);
                        if (topicSet == null)
                        {
                            topicSet = new HashSet<String>();
                            topicGroupToTopicMap.put(topicGroup, topicSet);
                        }
                        for(int i = 0; i < topicList.length(); i ++)
                        {
                            String topic = topicList.getString(i);
                            topicSet.add(topic);
                        }
                    }
                }
            }
        }
        return topicGroupToTopicMap;
    }
}
