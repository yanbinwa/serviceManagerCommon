package yanbinwa.common.utils;

import static org.junit.Assert.*;

import java.io.FileNotFoundException;
import java.util.HashMap;
import java.util.Map;

import org.junit.Test;

public class YamlUtilTest
{

    @Test
    public void test()
    {
        String fileName = "/Users/yanbinwa/Documents/workspace/springboot/serviceManager/serviceManagerCommon/src/test/file/YamlUtilTest.yaml";
        Map<String, Object> testMap = new HashMap<String, Object>();
        testMap.put("wyb", "123");
        Map<String, String> testMap1 = new HashMap<String, String>();
        testMap1.put("zcl", "456");
        testMap1.put("wzy", "789");
        testMap.put("wjy", testMap1);
        try
        {
            YamlUtil.setMapToFile(testMap, fileName);
            //Map testMap2 = YamlUtil.getMapFromFile(fileName);
            //System.out.println(testMap2);
        } 
        catch (FileNotFoundException e)
        {
            fail(e.getMessage());
        }
    }
}
