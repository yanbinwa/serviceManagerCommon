package yanbinwa.common.constants;

public class CommonConstants
{
    public static final int ZK_TIMEOUT = 5000;
    
    public static final String DATA_IP_KEY = "ip";
    public static final String DATA_SERVICENAME_KEY = "serviceName";
    public static final String DATA_SERVICEGROUPNAME_KEY = "serviceGroupName";
    public static final String DATA_PORT_KEY = "port";
    public static final String DATA_ROOTURL_KEY = "rootUrl";
    
    public static final String DATA_DECORATE_KEY = "decorate";
    public static final String DATA_TOPIC_INFO_KEY = "topicInfo";
    public static final String DATA_REDIS_INFO_NEED_KEY = "redisInfoNeed";
    
    public static final String DATA_DEPENDENCE_DATA_KEY = "dependenceData";
    public static final String DATA_KAFKA_TOPIC_DATA_KEY = "kafkaTopicData";
    
    public static final String KAFKA_CONSUMERS_KEY = "consumers";
    public static final String KAFKA_PRODUCERS_KEY = "producers";
    public static final int KAFKA_DEFAULT_PARTITION_NUM = 10;

    public static final int SERVICE_ACTIVE = 0;
    public static final int SERVICE_STANDBY = 1;
    
    public static final int SEQUENTIAL_SUFFIX_LENGTH = 10;
    
    public static final int HTTP_CONNECTION_TIMEOUT = 60000;
    public static final int HTTP_READ_TIMEOUT = 60000;
    
    public static final int REDIS_GET_CONNECTION_TIMEOUT = 1000;
    public static final int REDIS_CLOSE_RETRY_TIMES = 3;
    public static final int REDIS_CLOSE_RETRY_INTERVAL = 100;
    
}
