package yanbinwa.common.zNodedata;

import org.json.JSONObject;

import yanbinwa.common.constants.CommonConstants;

public final class ZNodeServiceDataWithKafkaTopic implements ZNodeData
{
    
    String ip;
    String serviceName;
    String serviceGroupName;
    int port;
    String rootUrl;
    String consumerTopicInfo;
    
    public ZNodeServiceDataWithKafkaTopic(String ip, String serviceGroupName, String serviceName, int port, String rootUrl, String consumerTopicInfo)
    {
        this.ip = ip;
        this.serviceGroupName = serviceGroupName;
        this.serviceName = serviceName;
        this.port = port;
        this.rootUrl = rootUrl;
        this.consumerTopicInfo = consumerTopicInfo;
    }
    
    public ZNodeServiceDataWithKafkaTopic(JSONObject obj)
    {
        loadFromJsonObject(obj);
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
    public String getIp()
    {
        return this.ip;
    }

    @Override
    public String getServiceName()
    {
        return this.serviceName;
    }

    @Override
    public String getServiceGroupName()
    {
        return this.serviceGroupName;
    }

    @Override
    public int getPort()
    {
        return this.port;
    }

    @Override
    public String getRootUrl()
    {
        return this.rootUrl;
    }
    
    public String getConsumerTopicInfo()
    {
        return this.consumerTopicInfo;
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
        obj.put(CommonConstants.DATA_CONSUMER_TOPIC_INFO_KEY, this.consumerTopicInfo);
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
        this.consumerTopicInfo = obj.getString(CommonConstants.DATA_CONSUMER_TOPIC_INFO_KEY);
    }
    
    @Override
    public boolean equals(Object obj)
    {
        if (obj == null)
        {
            return false;
        }
        if (!(obj instanceof ZNodeServiceDataWithKafkaTopic))
        {
            return false;
        }
        ZNodeServiceDataWithKafkaTopic other = (ZNodeServiceDataWithKafkaTopic)obj;
        if(this.createJsonObject().toString().equals(other.createJsonObject().toString()))
        {
            return true;
        }
        else
        {
            return false;
        }
    }
    
}
