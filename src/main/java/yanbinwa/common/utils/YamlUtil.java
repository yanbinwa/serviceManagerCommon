package yanbinwa.common.utils;

import java.io.File;
import java.io.FileNotFoundException;
import java.util.HashMap;
import java.util.Map;

import org.apache.log4j.Logger;
import org.ho.yaml.Yaml;

public class YamlUtil
{
    private static final Logger logger = Logger.getLogger(YamlUtil.class);
    
    @SuppressWarnings("rawtypes")
    public static Map getMapFromFile(String fileName) throws FileNotFoundException
    {
        File yamlFile = new File(fileName);
        if (!yamlFile.exists())
        {
            logger.error("The yaml file is not exist: " + fileName);
            return null;
        }
        Map properties = null;
        properties = (Map) Yaml.loadType(yamlFile, HashMap.class);
        return properties;
    }
    
    @SuppressWarnings("rawtypes")
    public static void setMapToFile(Map map, String fileName) throws FileNotFoundException
    {
        File yamlFile = new File(fileName);
        Yaml.dump(map, yamlFile, false);
    }
}
