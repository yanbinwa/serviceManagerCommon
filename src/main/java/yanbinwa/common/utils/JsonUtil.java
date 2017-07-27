package yanbinwa.common.utils;

import java.util.Map;

import net.sf.json.JSONObject;

public class JsonUtil
{
    @SuppressWarnings("rawtypes")
    public static Map JsonStrToMap(String jsonStr)
    {
        if (jsonStr == null)
        {
            return null;
        }
        JSONObject jsonObject = JSONObject.fromObject(jsonStr);
        return (Map) jsonObject;
    }
}
