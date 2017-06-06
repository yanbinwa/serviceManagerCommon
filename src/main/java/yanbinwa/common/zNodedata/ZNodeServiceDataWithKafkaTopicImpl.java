package yanbinwa.common.zNodedata;

import org.json.JSONObject;

import yanbinwa.common.constants.CommonConstants;

public final class ZNodeServiceDataWithKafkaTopicImpl extends ZNodeServiceDataImpl
{
    String topicInfo;
    
    public ZNodeServiceDataWithKafkaTopicImpl(String ip, String serviceGroupName, String serviceName, int port, String rootUrl, String topicInfo)
    {
        super(ip, serviceGroupName, serviceName, port, rootUrl);
        this.topicInfo = topicInfo;
    }
    
    public ZNodeServiceDataWithKafkaTopicImpl(JSONObject obj)
    {
        super();
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
    
    public String getTopicInfo()
    {
        return this.topicInfo;
    }
    
    @Override
    public JSONObject createJsonObject()
    {
        JSONObject obj = super.createJsonObject();
        obj.put(CommonConstants.DATA_TOPIC_INFO_KEY, this.topicInfo);
        return obj;
    }

    @Override
    public void loadFromJsonObject(JSONObject obj)
    {
        super.loadFromJsonObject(obj);
        this.topicInfo = obj.getString(CommonConstants.DATA_TOPIC_INFO_KEY);
    }
    
    @Override
    public boolean equals(Object obj)
    {
        if (obj == null)
        {
            return false;
        }
        if (!(obj instanceof ZNodeServiceDataWithKafkaTopicImpl))
        {
            return false;
        }
        ZNodeServiceDataWithKafkaTopicImpl other = (ZNodeServiceDataWithKafkaTopicImpl)obj;
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
