package yanbinwa.common.zNodedata;

import org.json.JSONObject;

public interface ZNodeData
{
    JSONObject createJsonObject();
    
    void loadFromJsonObject(JSONObject obj);  
}
