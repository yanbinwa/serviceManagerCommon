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
import yanbinwa.common.zNodedata.decorate.ZNodeDecorateType;
import yanbinwa.common.zNodedata.decorate.ZNodeDependenceDataDecorate;
import yanbinwa.common.zNodedata.decorate.ZNodeDependenceDataDecorateKafka;
import yanbinwa.common.zNodedata.decorate.ZNodeDependenceDataDecorateRedis;

public class ZNodeDependenceDataImpl implements ZNodeDependenceData
{

    private static final Logger logger = Logger.getLogger(ZNodeDependenceDataImpl.class);
    
    Map<String, Set<ZNodeServiceData>> dependenceData = new HashMap<String, Set<ZNodeServiceData>>();
    
    Map<ZNodeDecorateType, ZNodeDependenceDataDecorate> decorateMap = new HashMap<ZNodeDecorateType, ZNodeDependenceDataDecorate>();
    
    public ZNodeDependenceDataImpl(Map<String, Set<ZNodeServiceData>> dependenceData)
    {
        this.dependenceData = dependenceData;
    }
    
    public ZNodeDependenceDataImpl(JSONObject obj)
    {
        loadFromJsonObject(obj);
    }
    
    public ZNodeDependenceDataImpl()
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
        JSONObject dependenceObj = new JSONObject();
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
            dependenceObj.put(entry.getKey(), objArr);
        }
        retObj.put(CommonConstants.DATA_DEPENDENCE_DATA_KEY, dependenceObj);
        
        JSONObject decorateObj = new JSONObject();
        for (Map.Entry<ZNodeDecorateType, ZNodeDependenceDataDecorate> entry : decorateMap.entrySet())
        {
            ZNodeDecorateType type = entry.getKey();
            ZNodeDependenceDataDecorate decorate = entry.getValue();
            if (decorate == null)
            {
                logger.error("Decorate dependence obj is null for type: " + type.name());
                continue;
            }
            decorateObj.put(type.name(), decorate.createJsonObject());
        }
        
        retObj.put(CommonConstants.DATA_DECORATE_KEY, decorateObj);
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
        
        if (!obj.has(CommonConstants.DATA_DEPENDENCE_DATA_KEY))
        {
            logger.error("Dependence obj does not has the key " + CommonConstants.DATA_DEPENDENCE_DATA_KEY);
            return;
        }
        JSONObject dependenceObj = obj.getJSONObject(CommonConstants.DATA_DEPENDENCE_DATA_KEY);
        for(Object keyObj : dependenceObj.keySet())
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
            JSONArray valueObj = dependenceObj.getJSONArray(key);
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
                ZNodeServiceData zNodeData = new ZNodeServiceDataImpl(zNodeServiceObj);
                zNodeServiceList.add(zNodeData);
            }
        }
        
        if (!obj.has(CommonConstants.DATA_DECORATE_KEY))
        {
            logger.info("Decorate obj does not has the key " + CommonConstants.DATA_DECORATE_KEY);
            return;
        }
        JSONObject decorateMapObj = obj.getJSONObject(CommonConstants.DATA_DECORATE_KEY);
        for(String key : decorateMapObj.keySet())
        {
            ZNodeDecorateType type = null;
            try
            {
                type = ZNodeDecorateType.valueOf(key);
            }
            catch(IllegalArgumentException e)
            {
                logger.error("Unknown decorate type: " + key);
                continue;
            }
            JSONObject decorateObj = decorateMapObj.getJSONObject(key);
            ZNodeDependenceDataDecorate decorate = null;
            switch(type)
            {
            case KAFKA:
                decorate = new ZNodeDependenceDataDecorateKafka(decorateObj);
                break;
            case REDIS:
                decorate = new ZNodeDependenceDataDecorateRedis(decorateObj);
                break;
            }
            if (decorate != null)
            {
                decorateMap.put(type, decorate);
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

    @Override
    public void addDependenceDataDecorate(ZNodeDecorateType type, Object obj)
    {
        logger.info("add decorateObj is: " + obj + "; type is: " + type);
        ZNodeDependenceDataDecorate decorate = null;
        switch(type)
        {
        case KAFKA:
            decorate = new ZNodeDependenceDataDecorateKafka(obj);
            break;
        case REDIS:
            decorate = new ZNodeDependenceDataDecorateRedis(obj);
            break;
        }
        if (decorate != null)
        {
            decorateMap.put(type, decorate);
            logger.info("Add decorate: " + decorate);
        }
    }

    @Override
    public boolean isContainedDecorate(ZNodeDecorateType type)
    {
        return decorateMap.containsKey(type);
    }

    @Override
    public ZNodeDependenceDataDecorate getDependenceDataDecorate(ZNodeDecorateType type)
    {
        return decorateMap.get(type);
    }
}
