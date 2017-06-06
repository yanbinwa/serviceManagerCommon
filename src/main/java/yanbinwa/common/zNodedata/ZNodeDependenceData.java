package yanbinwa.common.zNodedata;

/**
 * key: service group
 * value: service belong to this servie group
 * 这里的dependence是对于一个service的，而不是所有的dependence信息
 */

import java.util.HashMap;
import java.util.HashSet;
import java.util.Map;
import java.util.Set;

import org.apache.log4j.Logger;
import org.json.JSONArray;
import org.json.JSONObject;

import yanbinwa.common.constants.CommonConstants;

public class ZNodeDependenceData implements ZNodeData
{

    private static final Logger logger = Logger.getLogger(ZNodeDependenceData.class);
    
    Map<String, Set<ZNodeServiceData>> dependenceData = new HashMap<String, Set<ZNodeServiceData>>();
    
    public ZNodeDependenceData(Map<String, Set<ZNodeServiceData>> dependenceData)
    {
        this.dependenceData = dependenceData;
    }
    
    public ZNodeDependenceData(JSONObject obj)
    {
        loadFromJsonObject(obj);
    }
    
    public ZNodeDependenceData()
    {
        
    }
    
    public Map<String, Set<ZNodeServiceData>> getDependenceData()
    {
        return dependenceData;
    }
    
    public JSONObject createJsonObject()
    {
        if(dependenceData == null)
        {
            return null;
        }
        
        JSONObject retObj = new JSONObject();
        for(Map.Entry<String, Set<ZNodeServiceData>> entry : dependenceData.entrySet())
        {
            Set<ZNodeServiceData> zNodeServiceList = entry.getValue();
            if (zNodeServiceList == null)
            {
                continue;
            }
            JSONArray objArr = new JSONArray();
            for(ZNodeServiceData zNodeService : zNodeServiceList)
            {
                if (zNodeService == null)
                {
                    continue;
                }
                objArr.put(zNodeService.createJsonObject());
            }
            retObj.put(entry.getKey(), objArr);
        }
        return retObj;
    }

    public void loadFromJsonObject(JSONObject obj)
    {
        if (obj == null)
        {
            dependenceData = null;
            return;
        }
        if (dependenceData == null)
        {
            dependenceData = new HashMap<String, Set<ZNodeServiceData>>();
        }
        for(Object keyObj : obj.keySet())
        {
            String key = null;
            if(keyObj instanceof String)
            {
                key = (String)keyObj;
            }
            else
            {
                logger.error("Key should be String. The error key is: " + keyObj);
                continue;
            }
            JSONArray valueObj = obj.getJSONArray(key);
            if (valueObj == null)
            {
                continue;
            }
            Set<ZNodeServiceData> zNodeServiceList = dependenceData.get(key);
            if(zNodeServiceList == null)
            {
                zNodeServiceList = new HashSet<ZNodeServiceData>();
                dependenceData.put(key, zNodeServiceList);
            }
            for(int i = 0; i < valueObj.length(); i ++)
            {
                JSONObject zNodeServiceObj = valueObj.getJSONObject(i);
                if (zNodeServiceObj == null)
                {
                    continue;
                }
                ZNodeServiceData zNodeData = null;
                if (zNodeServiceObj.has(CommonConstants.DATA_TOPIC_INFO_KEY))
                {
                    zNodeData = new ZNodeServiceDataWithKafkaTopicImpl(zNodeServiceObj);
                }
                else
                {
                    zNodeData = new ZNodeServiceDataImpl(zNodeServiceObj);
                }
                zNodeServiceList.add(zNodeData);
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
        if (!(obj instanceof ZNodeDependenceData))
        {
            return false;
        }
        ZNodeDependenceData other = (ZNodeDependenceData)obj;
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
