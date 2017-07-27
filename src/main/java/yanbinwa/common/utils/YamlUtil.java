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
    public static Map getMapFromFile(String fileName)
    {
        File yamlFile = new File(fileName);
        if (!yamlFile.exists())
        {
            logger.error("The yaml file is not exist: " + fileName);
            return null;
        }
        Map properties = null;
        try
        {
            properties = (Map) Yaml.loadType(yamlFile, HashMap.class);
        } 
        catch (FileNotFoundException e)
        {
            logger.error("Fail to load the yaml file: " + fileName);
            e.printStackTrace();
            return null;
        }
        return properties;
    }
}
