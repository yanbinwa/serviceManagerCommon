package yanbinwa.common.utils;

import java.util.Iterator;
import java.util.Map;
import java.util.Map.Entry;

public class MapUtil
{
    @SuppressWarnings("unchecked")
    public static boolean compareMap(Map<String, Object> map1, Map<String, Object> map2)
    {
        if (map1 == null || map2 == null)
        {
            return false;
        }
        Iterator<Entry<String, Object>> iter1 = map1.entrySet().iterator();
        while(iter1.hasNext())
        {
            Map.Entry<String, Object> entry1 = (Entry<String, Object>)iter1.next();
            String key1 = entry1.getKey();
            if (!map2.containsKey(key1))
            {
                return false;
            }
            
            Object value1 = entry1.getValue();
            Object value2 = map2.get(key1);
            if (value1 == null && value2 == null)
            {
                continue;
            }
            else if (value1 == null || value2 == null)
            {
                return false;
            }
            else if (value1 instanceof Map && value2 instanceof Map)
            {
                boolean ret = compareMap((Map<String, Object>) value1, (Map<String, Object>) value2);
                if (!ret)
                {
                    return false;
                }
            }
            else
            {
                if (!value1.equals(value2))
                {
                    return false;
                }
            }
        }
        return true;
    }
}
