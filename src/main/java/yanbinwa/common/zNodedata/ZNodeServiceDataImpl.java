package yanbinwa.common.zNodedata;

import java.util.HashMap;
import java.util.Map;

import org.apache.log4j.Logger;
import org.json.JSONObject;

import yanbinwa.common.constants.CommonConstants;
import yanbinwa.common.zNodedata.decorate.ZNodeDecorateType;
import yanbinwa.common.zNodedata.decorate.ZNodeServiceDataDecorate;
import yanbinwa.common.zNodedata.decorate.ZNodeServiceDataDecorateKafka;
import yanbinwa.common.zNodedata.decorate.ZNodeServiceDataDecorateRedis;

public class ZNodeServiceDataImpl implements ZNodeServiceData
{
    
    private static final Logger logger = Logger.getLogger(ZNodeServiceDataImpl.class);
    
    String ip;
    String serviceName;
    String serviceGroupName;
    int port;
    String rootUrl;
    
    Map<ZNodeDecorateType, ZNodeServiceDataDecorate> decorateMap = new HashMap<ZNodeDecorateType, ZNodeServiceDataDecorate>();
    
    public ZNodeServiceDataImpl(String ip, String serviceGroupName, String serviceName, int port, String rootUrl)
    {
        this.ip = ip;
        this.serviceName = serviceName;
        this.serviceGroupName = serviceGroupName;
        this.port = port;
        this.rootUrl = rootUrl;
    }
    
    public ZNodeServiceDataImpl(JSONObject obj)
    {
        loadFromJsonObject(obj);
    }
    
    public ZNodeServiceDataImpl()
    {
        
    }
    
    public String getIp()
    {
        return ip;
    }
    
    public String getServiceName()
    {
        return serviceName;
    }
    
    public String getServiceGroupName()
    {
        return serviceGroupName;
    }
    
    public int getPort()
    {
        return port;
    }
    
    public String getRootUrl()
    {
        return rootUrl;
    }

    @Override
    public JSONObject createJsonObject()
    {
        JSONObject obj = new JSONObject();
        obj.put(CommonConstants.DATA_IP_KEY, this.ip);
        obj.put(CommonConstants.DATA_SERVICENAME_KEY, this.serviceName);
        obj.put(CommonConstants.DATA_SERVICEGROUPNAME_KEY, this.serviceGroupName);
        obj.put(CommonConstants.DATA_PORT_KEY, this.port);
        obj.put(CommonConstants.DATA_ROOTURL_KEY, this.rootUrl);
        
        JSONObject decorateObj = new JSONObject();
        for (Map.Entry<ZNodeDecorateType, ZNodeServiceDataDecorate> entry : decorateMap.entrySet())
        {
            ZNodeDecorateType type = entry.getKey();
            ZNodeServiceDataDecorate decorate = entry.getValue();
            if (decorate == null)
            {
                logger.error("Decorate obj is null for type: " + type.name() + " for service " + serviceName);
                continue;
            }
            decorateObj.put(type.name(), decorate.createJsonObject());
        }
        
        obj.put(CommonConstants.DATA_DECORATE_KEY, decorateObj);
        return obj;
    }

    @Override
    public void loadFromJsonObject(JSONObject obj)
    {
        this.ip = obj.getString(CommonConstants.DATA_IP_KEY);
        this.serviceName = obj.getString(CommonConstants.DATA_SERVICENAME_KEY);
        this.serviceGroupName = obj.getString(CommonConstants.DATA_SERVICEGROUPNAME_KEY);
        this.port = obj.getInt(CommonConstants.DATA_PORT_KEY);
        this.rootUrl = obj.getString(CommonConstants.DATA_ROOTURL_KEY);
        
        if (!obj.has(CommonConstants.DATA_DECORATE_KEY))
        {
            return;
        }
        JSONObject decorateMapObj = obj.getJSONObject(CommonConstants.DATA_DECORATE_KEY);
        
        for (String key : decorateMapObj.keySet())
        {
            ZNodeDecorateType type = null;
            try
            {
                type = ZNodeDecorateType.valueOf(key);
            }
            catch(IllegalArgumentException e)
            {
                logger.error("Unknown decorate type: " + key + " at service " + serviceName);
                continue;
            }
            JSONObject decorateObj = decorateMapObj.getJSONObject(key);
            ZNodeServiceDataDecorate decorate = null;
            
            switch(type)
            {
            case KAFKA:
                decorate = new ZNodeServiceDataDecorateKafka(decorateObj);
                break;
            case REDIS:
                decorate = new ZNodeServiceDataDecorateRedis(decorateObj);
                break;
            }
            
            if (decorate != null)
            {
                decorateMap.put(type, decorate);
                logger.debug("load decorate: " + decorate + " to service " + serviceName);
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
        if (!(obj instanceof ZNodeServiceDataImpl))
        {
            return false;
        }
        ZNodeServiceDataImpl other = (ZNodeServiceDataImpl)obj;
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

    @Override
    public void addServiceDataDecorate(ZNodeDecorateType type, Object obj)
    {
        ZNodeServiceDataDecorate decorate = null;
        switch(type)
        {
        case KAFKA:
            decorate = new ZNodeServiceDataDecorateKafka(obj);
            break;
        case REDIS:
            decorate = new ZNodeServiceDataDecorateRedis(obj);
            break;
        }
        if (decorate != null)
        {
            decorateMap.put(type, decorate);
            logger.info("Add decorate: " + decorate + " to service " + serviceName);
        }
    }

    @Override
    public boolean isContainedDecoreate(ZNodeDecorateType type)
    {
        return decorateMap.containsKey(type);
    }

    @Override
    public ZNodeServiceDataDecorate getServiceDataDecorate(ZNodeDecorateType type)
    {
        return decorateMap.get(type);
    }
}
