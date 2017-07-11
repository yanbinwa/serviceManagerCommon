package yanbinwa.common.zNodedata.decorate;

import java.util.HashMap;
import java.util.HashSet;
import java.util.Map;
import java.util.Set;

import org.apache.log4j.Logger;
import org.json.JSONArray;
import org.json.JSONObject;

public class ZNodeDependenceDataDecorateKafka implements ZNodeDependenceDataDecorate
{
    private static final Logger logger = Logger.getLogger(ZNodeDependenceDataDecorateKafka.class);
    
    private Map<String, Map<String, Set<Integer>>> topicGroupNameToTopicNameToPartitionKeyMap = new HashMap<String, Map<String, Set<Integer>>>();
    
    @SuppressWarnings("unchecked")
    public ZNodeDependenceDataDecorateKafka(Object obj)
    {
        if (!(obj instanceof Map))
        {
            logger.error("ZNodeDependenceDataDecorateKafka input should be Map");
            return;
        }
        this.topicGroupNameToTopicNameToPartitionKeyMap = (Map<String, Map<String, Set<Integer>>>)obj;
    }
    
    public ZNodeDependenceDataDecorateKafka(JSONObject jsonObj)
    {
        loadFromJsonObject(jsonObj);
    }
    
    public ZNodeDependenceDataDecorateKafka()
    {
        
    }
    
    public Map<String, Map<String, Set<Integer>>> getTopicGroupNameToTopicNameToPartitionKeyMap()
    {
        return topicGroupNameToTopicNameToPartitionKeyMap;
    }
    
    @Override
    public JSONObject createJsonObject()
    {
        if (topicGroupNameToTopicNameToPartitionKeyMap == null)
        {
            return null;
        }
        JSONObject topicGroupNameToTopicNameToPartitionKeyMapObj = new JSONObject();
        for(Map.Entry<String, Map<String, Set<Integer>>> entry : topicGroupNameToTopicNameToPartitionKeyMap.entrySet())
        {
            String topicGroupName = entry.getKey();
            Map<String, Set<Integer>> topicNameToPartitionKeyMap = entry.getValue();
            if (topicNameToPartitionKeyMap == null)
            {
                continue;
            }
            JSONObject topicNameToPartitionKeyMapObj = new JSONObject();
            for(Map.Entry<String, Set<Integer>> entry1 : topicNameToPartitionKeyMap.entrySet())
            {
                String topicName = entry1.getKey();
                Set<Integer> partitionKeySet = entry1.getValue();
                if (partitionKeySet == null)
                {
                    continue;
                }
                JSONArray partitionKeyArray = new JSONArray();
                for(int partitionKey : partitionKeySet)
                {
                    partitionKeyArray.put(partitionKey);
                }
                topicNameToPartitionKeyMapObj.put(topicName, partitionKeyArray);
            }
            topicGroupNameToTopicNameToPartitionKeyMapObj.put(topicGroupName, topicNameToPartitionKeyMapObj);
        }
        return topicGroupNameToTopicNameToPartitionKeyMapObj;
    }

    @Override
    public void loadFromJsonObject(JSONObject obj)
    {
        if (obj == null)
        {
            return;
        }
        topicGroupNameToTopicNameToPartitionKeyMap.clear();
        for(String topicGroupName : obj.keySet())
        {
            JSONObject topicNameToPartitionKeyMapObj = obj.getJSONObject(topicGroupName);
            if (topicNameToPartitionKeyMapObj == null)
            {
                continue;
            }
            Map<String, Set<Integer>> topicNameToPartitionKeyMap = topicGroupNameToTopicNameToPartitionKeyMap.get(topicGroupName);
            if (topicNameToPartitionKeyMap == null)
            {
                topicNameToPartitionKeyMap = new HashMap<String, Set<Integer>>();
                topicGroupNameToTopicNameToPartitionKeyMap.put(topicGroupName, topicNameToPartitionKeyMap);
            }
            for(String topicName : topicNameToPartitionKeyMapObj.keySet())
            {
                JSONArray partitionKeyArray = topicNameToPartitionKeyMapObj.getJSONArray(topicName);
                if (partitionKeyArray == null)
                {
                    continue;
                }
                Set<Integer> partitionKeySet = topicNameToPartitionKeyMap.get(topicName);
                if (partitionKeySet == null)
                {
                    partitionKeySet = new HashSet<Integer>();
                    topicNameToPartitionKeyMap.put(topicName, partitionKeySet);
                }
                for(int i = 0; i < partitionKeyArray.length(); i ++)
                {
                    int partitionKey = partitionKeyArray.getInt(i);
                    partitionKeySet.add((Integer)partitionKey);
                }
            }
        }
    }
    
    @Override
    public boolean equals(Object obj)
    {
        if (obj == null)
        {
            return false;
        }
        if (! (obj instanceof ZNodeDependenceDataDecorateKafka))
        {
            return false;
        }
        ZNodeDependenceDataDecorateKafka other = (ZNodeDependenceDataDecorateKafka) obj;
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
        return this.createJsonObject().toString().hashCode();
    }
    
    @Override
    public String toString()
    {
        return this.createJsonObject().toString();
    }
}
