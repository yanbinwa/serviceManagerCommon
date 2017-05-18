package yanbinwa.common.zNodedata;

import org.json.JSONObject;

import lombok.Builder;
import lombok.Getter;
import yanbinwa.common.constants.CommonConstants;

@Getter
@Builder
public final class ZNodeServiceData implements ZNodeData
{
    String ip;
    String serviceName;
    int port;
    String rootUrl;
    
    public ZNodeServiceData(String ip, String serviceName, int port, String rootUrl)
    {
        this.ip = ip;
        this.serviceName = serviceName;
        this.port = port;
        this.rootUrl = rootUrl;
    }
    
    public ZNodeServiceData(JSONObject obj)
    {
        loadFromJsonObject(obj);
    }
    
    public String getIp()
    {
        return ip;
    }
    
    public String getServiceName()
    {
        return serviceName;
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
        obj.put(CommonConstants.DATA_PORT_KEY, this.port);
        obj.put(CommonConstants.DATA_ROOTURL_KEY, this.rootUrl);
        return obj;
    }

    @Override
    public void loadFromJsonObject(JSONObject obj)
    {
        this.ip = obj.getString(CommonConstants.DATA_IP_KEY);
        this.serviceName = obj.getString(CommonConstants.DATA_SERVICENAME_KEY);
        this.port = obj.getInt(CommonConstants.DATA_PORT_KEY);
        this.rootUrl = obj.getString(CommonConstants.DATA_ROOTURL_KEY);
    }
    
    @Override
    public boolean equals(Object obj)
    {
        if (obj == null)
        {
            return false;
        }
        if (!(obj instanceof ZNodeServiceData))
        {
            return false;
        }
        ZNodeServiceData other = (ZNodeServiceData)obj;
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
