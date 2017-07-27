package yanbinwa.common.configClient;

import org.json.JSONObject;

import yanbinwa.common.iInterface.ServiceLifeCycle;

public interface ConfigClient extends ServiceLifeCycle
{
    public boolean isReady();
    
    public JSONObject getServiceConfigProperties();
}
