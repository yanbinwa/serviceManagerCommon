package yanbinwa.common.zNodedata.decorate;

import org.apache.log4j.Logger;
import org.json.JSONObject;

import yanbinwa.common.constants.CommonConstants;

public class ZNodeServiceDataDecorateKafka implements ZNodeServiceDataDecorate
{
    
    private static final Logger logger = Logger.getLogger(ZNodeServiceDataDecorateKafka.class);
    
    String topicInfo = null;
    
    public ZNodeServiceDataDecorateKafka(Object obj)
    {
        if (!(obj instanceof String))
        {
            logger.error("ZNodeServiceDataDecorateKafka input should be String, not " + obj);
            return;
        }
        this.topicInfo = (String) obj;
    }
    
    public ZNodeServiceDataDecorateKafka(JSONObject obj)
    {
        loadFromJsonObject(obj);
    }

    @Override
    public JSONObject createJsonObject()
    {
        JSONObject obj = new JSONObject();
        obj.put(CommonConstants.DATA_TOPIC_INFO_KEY, this.topicInfo);
        return obj;
    }

    @Override
    public void loadFromJsonObject(JSONObject obj)
    {
        this.topicInfo = obj.getString(CommonConstants.DATA_TOPIC_INFO_KEY);
    }
    
    public String getTopicInfo()
    {
        return this.topicInfo;
    }
    
    @Override
    public boolean equals(Object obj)
    {
        if (obj == null)
        {
            return false;
        }
        if (! (obj instanceof ZNodeServiceDataDecorateKafka))
        {
            return false;
        }
        ZNodeServiceDataDecorateKafka other = (ZNodeServiceDataDecorateKafka) obj;
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
