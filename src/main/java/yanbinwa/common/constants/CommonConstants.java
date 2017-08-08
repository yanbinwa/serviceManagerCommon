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
    
    public static final String SERVICE_IP = "ip";
    public static final String SERVICE_SERVICEGROUPNAME = "serviceGroupName";
    public static final String SERVICE_SERVICENAME = "serviceName";
    public static final String SERVICE_PORT = "port";
    public static final String SERVICE_ROOTURL = "rootUrl";
    public static final String SERVICE_TOPICINFO = "topicInfo";
    
    public static final String CONFZNODEPATH_KEY = "confZnodePath";
    public static final String CONFDEPLOYCHILDZNODEPATH_KEY = "deployChildZnodePath";
    public static final String REGZNODEPATH_KEY = "regZnodePath";
    public static final String REGZNODECHILDPATH_KEY = "regZnodeChildPath";
    public static final String DEPZNODEPATH_KEY = "depZnodePath";
    
    public static final String ZOOKEEPER_HOSTPORT_KEY = "zookeeperHostport";
    public static final String DEVICES_KEY = "devices";
    public static final String SERVICE_DATA_PROPERTIES_KEY = "serviceDataProperties";
    public static final String SERVICE_DEPENDENCE_PROPERTIES_KEY = "dependencyProperties";
    public static final String SERVICE_MONITOR_PROPERTIES_KEY = "monitorProperties";
    public static final String SERVICE_KAFKA_PROPERTIES_KEY = "kafkaProperties";
    
    public static final String ZOOKEEPER_SESSION_TIMEOUT_KEY = "sessionTimeout";
    public static final String ZOOKEEPER_CONNECTION_TIMEOUT_KEY = "connectionTimeout";
    
    public static final int ZOOKEEPER_SESSION_TIMEOUT_DEFAULT = 30000;
    public static final int ZOOKEEPER_CONNECTION_TIMEOUT_DEFAULT = 30000;
    
    public static final String KAFKA_TOPIC_KEY = "topic";
    public static final String KAFKA_PARTITIONS_KEY = "partitions";
    public static final String KAFKA_REPLICATION_FACTOR_KEY = "replicationFactor";
    
    public static final int KAFKA_PARTITIONS_DEFAULT = 1;
    public static final int KAFKA_REPLICATION_FACTOR_DEFAULT = 1;
    
    public static final int ZK_WAIT_INTERVAL = 10 * 1000;
    
}
