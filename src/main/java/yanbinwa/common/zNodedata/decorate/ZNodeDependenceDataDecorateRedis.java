package yanbinwa.common.zNodedata.decorate;

import java.util.HashMap;
import java.util.HashSet;
import java.util.Map;
import java.util.Set;

import org.apache.log4j.Logger;
import org.json.JSONArray;
import org.json.JSONObject;

public class ZNodeDependenceDataDecorateRedis implements ZNodeDependenceDataDecorate
{
    private static final Logger logger = Logger.getLogger(ZNodeDependenceDataDecorateRedis.class);
    
    Map<String, Set<Integer>> redisServiceNameToPartitionMap = new HashMap<String, Set<Integer>>();
    
    @SuppressWarnings("unchecked")
    public ZNodeDependenceDataDecorateRedis(Object obj)
    {
        if (!(obj instanceof Map))
        {
            logger.error("ZNodeDependenceDataDecorateRedis input should be map. But the input is " + obj);
            return;
        }
        this.redisServiceNameToPartitionMap = (Map<String, Set<Integer>>)obj;
    }
    
    public ZNodeDependenceDataDecorateRedis(JSONObject jsonObj)
    {
        loadFromJsonObject(jsonObj);
    }
    
    public ZNodeDependenceDataDecorateRedis()
    {
        
    }
    
    public Map<String, Set<Integer>> getRedisServiceNameToPartitionMap()
    {
        return redisServiceNameToPartitionMap;
    }

    @Override
    public JSONObject createJsonObject()
    {
        if (redisServiceNameToPartitionMap == null)
        {
            return null;
        }
        JSONObject redisServiceNameToPartitionMapObj = new JSONObject();
        for (Map.Entry<String, Set<Integer>> entry : redisServiceNameToPartitionMap.entrySet())
        {
            String redisServiceName = entry.getKey();
            Set<Integer> partitionKeySet = entry.getValue();
            JSONArray partitionKeyArray = new JSONArray();
            for (int partitionKey : partitionKeySet)
            {
                partitionKeyArray.put(partitionKey);
            }
            redisServiceNameToPartitionMapObj.put(redisServiceName, partitionKeyArray);
        }
        return redisServiceNameToPartitionMapObj;
    }

    @Override
    public void loadFromJsonObject(JSONObject obj)
    {
        if (obj == null)
        {
            return;
        }
        redisServiceNameToPartitionMap.clear();
        for (String redisServiceName : obj.keySet())
        {
            JSONArray partitionKeySetObj = obj.getJSONArray(redisServiceName);
            if (partitionKeySetObj == null)
            {
                logger.error("PartitionKeySet should not empty for redis service: " + redisServiceName);
                continue;
            }
            Set<Integer> partitionKeySet = redisServiceNameToPartitionMap.get(redisServiceName);
            if (partitionKeySet == null)
            {
                partitionKeySet = new HashSet<Integer>();
                redisServiceNameToPartitionMap.put(redisServiceName, partitionKeySet);
            }
            for(int i = 0; i < partitionKeySetObj.length(); i ++)
            {
                int partitionKey = partitionKeySetObj.getInt(i);
                partitionKeySet.add((Integer)partitionKey);
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
        if (! (obj instanceof ZNodeDependenceDataDecorateRedis))
        {
            return false;
        }
        ZNodeDependenceDataDecorateRedis other = (ZNodeDependenceDataDecorateRedis) obj;
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
