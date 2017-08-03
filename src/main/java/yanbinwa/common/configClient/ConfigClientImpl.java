package yanbinwa.common.configClient;

import java.util.Map;
import java.util.concurrent.BlockingQueue;
import java.util.concurrent.LinkedBlockingQueue;
import java.util.concurrent.TimeUnit;

import org.apache.log4j.Logger;
import org.apache.zookeeper.KeeperException;
import org.apache.zookeeper.WatchedEvent;
import org.apache.zookeeper.Watcher;
import org.apache.zookeeper.ZooKeeper;
import org.json.JSONObject;

import yanbinwa.common.constants.CommonConstants;
import yanbinwa.common.orchestrationClient.OrchestrationClient;
import yanbinwa.common.orchestrationClient.OrchestrationZnodeState;
import yanbinwa.common.utils.ZkUtil;
import yanbinwa.common.zNodedata.ZNodeServiceData;

public class ConfigClientImpl implements ConfigClient
{

    private static final Logger logger = Logger.getLogger(ConfigClientImpl.class);
    
    /** 服务信息,传进来的 */
    private ZNodeServiceData zNodeServiceData;
    
    private ConfigCallBack callback;
    
    /** 存放监听到的Zookeeper信息 */
    private BlockingQueue<WatchedEvent> zookeeperEventQueue = new LinkedBlockingQueue<WatchedEvent>();
    
    /** zookeeper的连接参数 */
    private String zookeeperHostPort;
    
    /** ConfZnode */
    private String configZnodePath;
    
    private boolean isRunning = false;
    
    private Thread zookeeperThread = null;
    
    private Thread zookeeperSync = null;
    
    /** zookeeper client 的实例 */
    private ZooKeeper zk = null;
    
    private ZkWatcher zkWatcher = new ZkWatcher();
    
    /** 服务是否ready*/
    private boolean isReady = false;
    
    private JSONObject serviceConfigProperties = null;
    
    /** 
     * 这里是判断ConfigService ZNode是否存在，因为ConfigService ZNode是一个永久节点，
     * 一旦存在，就不会删除，这里可能会读到脏数据，即ConfigService还没有起来，读到了历史数据
     */
    OrchestrationZnodeState configZNodeState = OrchestrationZnodeState.OFFLINE;
    
    public ConfigClientImpl(ZNodeServiceData data, String zookeeperHostPort, Map<String, String> zNodeInfoMap)
    {
        this(data, null, zookeeperHostPort, zNodeInfoMap);
    }
    
    public ConfigClientImpl(ZNodeServiceData data, ConfigCallBack callback, 
            String zookeeperHostPort, Map<String, String> zNodeInfoMap)
    {
        this.zNodeServiceData = data;
        this.callback = callback;
        this.zookeeperHostPort = zookeeperHostPort;
        if(zNodeInfoMap == null)
        {
            configZnodePath = null;
        }
        else
        {
            configZnodePath = zNodeInfoMap.get(CommonConstants.CONFZNODEPATH_KEY);
        }
    }
    
    @Override
    public void start()
    {
        if (!isRunning)
        {
            isRunning = true;
            logger.info("Starting the config client ...");
            
            /** 连接Zookeeper，创建相应的Znode，并监听其它服务创建的Znode */
            zookeeperThread = new Thread(new Runnable() {

                @Override
                public void run()
                {
                    zookeeperEventHandler();
                }
                
            });
            zookeeperThread.start();
            
            /** 定期与zookeeper同步 */
            zookeeperSync = new Thread(new Runnable(){

                @Override
                public void run()
                {
                    syncWithZookeeper();
                }
                
            });
            zookeeperSync.start();
        }
        else
        {
            logger.info("config client has readly started...");
        }
    }

    @Override
    public void stop()
    {
        if (isRunning)
        {
            isRunning = false;
            logger.info("stopping the config client ...");
            
            if (zookeeperThread != null)
            {
                zookeeperThread.interrupt();
            }
            if (zookeeperSync != null)
            {
                zookeeperSync.interrupt();
            }
            
            isReady = false;
            configZNodeState = OrchestrationZnodeState.OFFLINE;
            serviceConfigProperties = null;
            handleSelfClosedEvent();
        }
        else
        {
            logger.info("config client has readly stopped...");
        }
    }
    
    @Override
    public boolean isReady()
    {
        return isReady;
    }

    @Override
    public JSONObject getServiceConfigProperties()
    {
        return serviceConfigProperties;
    }
    
    private void zookeeperEventHandler()
    {
        if(!buildZookeeperConnection())
        {
            logger.error("Can not connection to zookeeper");
            return;
        }
        
        try
        {
            if(ZkUtil.checkZnodeExist(zk, configZnodePath))
            {
                configZNodeState = OrchestrationZnodeState.ONLINE;
            }
            while(isRunning)
            {
                try
                {
                    if(configZNodeState == OrchestrationZnodeState.OFFLINE)
                    {
                        handerZookeeperEventOffLine();
                    }
                    else
                    {
                        handerZookeeperEventOnLine();
                    } 
                }
                catch(KeeperException.SessionExpiredException e)
                {
                    logger.info("zookeeper connection is expire. Need to reconnect");
                    if(!buildZookeeperConnection())
                    {
                        logger.error("Can not connection to zookeeper");
                        return;
                    }
                }
            }
        } 
        catch (KeeperException e)
        {
            e.printStackTrace();
        } 
        catch (InterruptedException e)
        {
            e.printStackTrace();
        }
    }
    
    private void waitingForZookeeper()
    {
        logger.info("Waiting for the zookeeper...");
        while(zk.getState() == ZooKeeper.States.CONNECTING && isRunning)
        {
            try
            {
                Thread.sleep(CommonConstants.ZK_WAIT_INTERVAL);
                logger.debug("Try to connection to zookeeper");
            } 
            catch (InterruptedException e)
            {
                if (!isRunning)
                {
                    logger.info("Stop this thread");
                }
                else
                {
                    e.printStackTrace();
                }
            }            
        }
        logger.info("Connected to the zookeeper " + zookeeperHostPort);
    }
    
    private void handerZookeeperEventOffLine() throws KeeperException
    {
        logger.info("start zookeeper event handler for off line ...");
        try
        {
            ZkUtil.watchZnodeChange(zk, configZnodePath, zkWatcher);
            while(isRunning && configZNodeState == OrchestrationZnodeState.OFFLINE)
            {
                WatchedEvent event = zookeeperEventQueue.poll(OrchestrationClient.ZKEVENT_QUEUE_TIMEOUT, 
                        TimeUnit.MILLISECONDS);
                if(event == null)
                {
                    continue;
                }
                logger.debug("Get zk event at off line mode: " + event.toString());
                if (event.getType() == Watcher.Event.EventType.None && event.getState() == Watcher.Event.KeeperState.SyncConnected)
                {
                    logger.info("Connected to zookeeper success!");
                    continue;
                }
                if(event.getType() == Watcher.Event.EventType.NodeCreated || 
                                event.getType() == Watcher.Event.EventType.NodeDataChanged)
                {
                    if(event.getPath().equals(configZnodePath))
                    {
                        configZNodeState = OrchestrationZnodeState.ONLINE;
                        logger.info("Znode is on line " + event.getPath());
                        return;
                    }
                    ZkUtil.watchZnodeChildeChange(zk, configZnodePath, zkWatcher);
                }                
            }
        } 
        catch (KeeperException e)
        {
            if(e instanceof KeeperException.SessionExpiredException)
            {
                throw e;
            }
            else
            {
                logger.error(e.getMessage());
            }
        } 
        catch (InterruptedException e)
        {
            if(!isRunning)
            {
                logger.info("Close this thread...");
            }
            else
            {
                logger.error(e.getMessage());
                e.printStackTrace();
            }
        }
        logger.info("end zookeeper event handler for off line ...");
    }
    
    private void handerZookeeperEventOnLine() throws KeeperException
    {
        logger.info("start zookeeper event handler for on line ...");
        try
        {
            if(!ZkUtil.checkZnodeExist(zk, this.configZnodePath))
            {
                Thread.sleep(OrchestrationClient.ZKNODE_REGCHILDPATH_WAITTIME);
                if (!ZkUtil.checkZnodeExist(zk, this.configZnodePath))
                {
                    configZNodeState = OrchestrationZnodeState.OFFLINE;
                    return;
                }
            }
            String configZnodeChildPath = getConfigZnodeChildPath();
            ZkUtil.watchZnodeChange(zk, configZnodeChildPath, zkWatcher);
            //先检查是否已经存在depZnodeChildPath了，如果存在了，可以先获取一下
            if(ZkUtil.checkZnodeExist(zk, configZnodeChildPath))
            {
                if(!this.isReady)
                {
                    handleConfigZnodeChildCreateEvent();
                }
            }
            while(isRunning && this.configZNodeState == OrchestrationZnodeState.ONLINE)
            {
                WatchedEvent event = zookeeperEventQueue.poll(OrchestrationClient.ZKEVENT_QUEUE_TIMEOUT, 
                        TimeUnit.MILLISECONDS);
                if(event == null)
                {
                    continue;
                }
                logger.debug("Get zk event at on line mode: " + event.toString());
                if (event.getType() == Watcher.Event.EventType.None && event.getState() == Watcher.Event.KeeperState.SyncConnected)
                {
                    logger.info("Connected to zookeeper success!");
                    continue;
                }
                String path = event.getPath();
                if(path == null || !path.equals(configZnodeChildPath))
                {
                    logger.error("Unknown event for online: " + event.toString());
                    continue;
                }
                if(event.getType() == Watcher.Event.EventType.NodeCreated)
                {
                    handleConfigZnodeChildCreateEvent();
                }
                else if(event.getType() == Watcher.Event.EventType.NodeDeleted)
                {
                    handleConfigZnodeChildDeleteEvent();
                }
                else if(event.getType() == Watcher.Event.EventType.NodeDataChanged)
                {
                    handleConfigZnodeChildChangeEvent();
                }
                ZkUtil.watchZnodeChange(zk, configZnodeChildPath, zkWatcher);
            }
        }
        catch (KeeperException e)
        {
            if(e instanceof KeeperException.SessionExpiredException)
            {
                serviceConfigProperties = null;
                configZNodeState = OrchestrationZnodeState.OFFLINE;
                throw e;
            }
            else
            {
                logger.error(e.getMessage());
            }
        } 
        catch (InterruptedException e)
        {
            if(!isRunning)
            {
                logger.info("Close this thread...");
            }
            else
            {
                e.printStackTrace();
            }
        }
        logger.info("end zookeeper event handler for on line ...");
    }
    
    private boolean buildZookeeperConnection()
    {
        if (zk != null)
        {
            try
            {
                ZkUtil.closeZk(zk);
            } 
            catch (InterruptedException e1)
            {
                logger.error("Fail to close the zookeeper connection at begin");
            }
        }
        zk = ZkUtil.connectToZk(zookeeperHostPort, zkWatcher);
        if (zk == null)
        {
            logger.error("Can not connect to zookeeper: " + zookeeperHostPort);
            return false;
        }
        if(zk.getState() == ZooKeeper.States.CONNECTING)
        {
            waitingForZookeeper();
        }
        return true;
    }
    
    private String getConfigZnodeChildPath()
    {
        return configZnodePath + "/" + zNodeServiceData.getServiceName();
    }
    
    private void handleConfigZnodeChildCreateEvent() throws KeeperException, InterruptedException
    {
        this.isReady = true;
        this.serviceConfigProperties = ZkUtil.getData(zk, getConfigZnodeChildPath());
        if(callback != null)
        {
            this.callback.handleServiceConfigChange(ServiceConfigState.CREATED);
        }
    }
    
    private void handleConfigZnodeChildDeleteEvent() throws KeeperException, InterruptedException
    {
        this.isReady = false;
        this.serviceConfigProperties = null;
        if(callback != null)
        {
            this.callback.handleServiceConfigChange(ServiceConfigState.DELETED);
        }
    }
    
    private void handleConfigZnodeChildChangeEvent() throws KeeperException, InterruptedException
    {
        this.serviceConfigProperties = ZkUtil.getData(zk, getConfigZnodeChildPath());
        if(callback != null)
        {
            this.callback.handleServiceConfigChange(ServiceConfigState.CHANGED);
        }
    }
    
    private void handleSelfClosedEvent()
    {
        if(callback != null)
        {
            this.callback.handleServiceConfigChange(ServiceConfigState.CLOSE); 
        }
    }
    
    private void syncWithZookeeper()
    {
        while(isRunning)
        {
            if (zk != null)
            {
                WatchedEvent event = new WatchedEvent(Watcher.Event.EventType.NodeDataChanged, null, this.getConfigZnodeChildPath());
                zookeeperEventQueue.offer(event);
            }
            try
            {
                Thread.sleep(OrchestrationClient.ZK_SYNC_INTERVAL);
            } 
            catch (InterruptedException e)
            {
                if(!isRunning)
                {
                    logger.info("Stop this thread");
                }
                else
                {
                    e.printStackTrace();
                }
            }
        }
    }
    
    class ZkWatcher implements Watcher
    {
        @Override
        public void process(WatchedEvent event)
        {
            zookeeperEventQueue.offer(event);
        }
    }
}
