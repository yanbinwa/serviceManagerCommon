package yanbinwa.common.zNodedata.decorate;

import org.apache.log4j.Logger;
import org.json.JSONObject;

import yanbinwa.common.constants.CommonConstants;

public class ZNodeServiceDataDecorateRedis implements ZNodeServiceDataDecorate
{
    
    private static final Logger logger = Logger.getLogger(ZNodeServiceDataDecorateRedis.class);
    
    Boolean redisInfoNeed = false;
    
    public ZNodeServiceDataDecorateRedis(Object obj)
    {
        if (!(obj instanceof Boolean))
        {
            logger.error("ZNodeServiceDataDecorateRedis input object should be Boolean, but not " + obj);
            return;
        }
        this.redisInfoNeed = (Boolean) obj; 
    }

    public ZNodeServiceDataDecorateRedis(JSONObject obj)
    {
        loadFromJsonObject(obj);
    }
    
    @Override
    public JSONObject createJsonObject()
    {
        JSONObject obj = new JSONObject();
        obj.put(CommonConstants.DATA_REDIS_INFO_NEED_KEY, this.redisInfoNeed);
        return obj;
    }

    @Override
    public void loadFromJsonObject(JSONObject obj)
    {
        this.redisInfoNeed = obj.getBoolean(CommonConstants.DATA_REDIS_INFO_NEED_KEY);
    }
    
    public Boolean getRedisInfoNeed()
    {
        return this.redisInfoNeed;
    }
    
    @Override
    public boolean equals(Object obj)
    {
        if (obj == null)
        {
            return false;
        }
        if (! (obj instanceof ZNodeServiceDataDecorateRedis))
        {
            return false;
        }
        ZNodeServiceDataDecorateRedis other = (ZNodeServiceDataDecorateRedis) obj;
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
