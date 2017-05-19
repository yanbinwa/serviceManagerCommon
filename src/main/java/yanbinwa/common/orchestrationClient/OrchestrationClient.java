package yanbinwa.common.orchestrationClient;

import yanbinwa.common.iInterface.ServiceLifeCycle;
import yanbinwa.common.zNodedata.ZNodeDependenceData;

public interface OrchestrationClient extends ServiceLifeCycle
{
    public static final String REGZNODEPATH_KEY = "regZnodePath";
    public static final String REGZNODECHILDPATH_KEY = "regZnodeChildPath";
    public static final String DEPZNODEPATH_KEY = "depZnodePath";
    public static final String ZOOKEEPER_HOSTPORT_KEY = "zookeeperHostport";
    
    public static final int ZKEVENT_QUEUE_TIMEOUT = 5000;
    public static final int ZKNODE_REGCHILDPATH_WAITTIME = 1000;
    public static final int ZK_SYNC_INTERVAL = 60 * 1000;
    
    boolean isReady();
    ZNodeDependenceData getDepData();
    OrchestrationZnodeState getZnodeState();
}
