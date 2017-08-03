package yanbinwa.common.orchestrationClient;

import java.util.Map;
import java.util.concurrent.BlockingQueue;
import java.util.concurrent.LinkedBlockingQueue;
import java.util.concurrent.TimeUnit;

import org.apache.log4j.Logger;
import org.apache.zookeeper.KeeperException;
import org.apache.zookeeper.WatchedEvent;
import org.apache.zookeeper.Watcher;
import org.apache.zookeeper.ZooKeeper;

import yanbinwa.common.utils.ZkUtil;
import yanbinwa.common.zNodedata.ZNodeDataUtil;
import yanbinwa.common.zNodedata.ZNodeDependenceData;
import yanbinwa.common.zNodedata.ZNodeServiceData;

public class OrchestrationClientImpl implements OrchestrationClient
{
    /** 服务信息 */
    ZNodeServiceData zNodeServiceData;
    
    /** 服务状太发送改变后的回调类 */
    OrchestartionCallBack callback;
    
    /** zookeeper的连接参数 */
    String zookeeperHostPort;
    
    /** 真实注册的regZnodeChild*/
    String regRealZnodeChildPath = null;
    
    /** 注册的Znode */
    String regZnodePath;
    
    /** 读取依赖的Znode */
    String depZnodePath;
    
    /** zookeeper client 的实例 */
    ZooKeeper zk = null;
    
    /** zookeeper watcher */
    Watcher zkWatcher = new ZkWatcher();
    
    /** 存放监听到的Zookeeper信息 */
    BlockingQueue<WatchedEvent> zookeeperEventQueue = new LinkedBlockingQueue<WatchedEvent>();
    
    /** 主要是*/
    Thread zookeeperThread = null;
    
    Thread zookeeperSync = null;
    
    /** 当前服务的状态 */
    OrchestrationZnodeState zNodeState = OrchestrationZnodeState.OFFLINE;
    
    /** 当前运行状态 */
    boolean isRunning = false;
    
    /** 服务是否ready*/
    boolean isReady = false;
    
    /** 该服务目前可以使用的依赖 */
    ZNodeDependenceData depData = null;
    
    private static final Logger logger = Logger.getLogger(OrchestrationClientImpl.class);
    
    public OrchestrationClientImpl(ZNodeServiceData data, String zookeeperHostPort, Map<String, String> zNodeInfoMap)
    {
        this(data, null, zookeeperHostPort, zNodeInfoMap);
    }
    
    public OrchestrationClientImpl(ZNodeServiceData data, OrchestartionCallBack callback, 
                               String zookeeperHostPort, Map<String, String> zNodeInfoMap)
    {
        this.zNodeServiceData = data;
        this.callback = callback;
        this.zookeeperHostPort = zookeeperHostPort;
        if(zNodeInfoMap == null)
        {
            regZnodePath = null;
            depZnodePath = null;
        }
        else
        {
            regZnodePath = zNodeInfoMap.get(OrchestrationClient.REGZNODEPATH_KEY);
            depZnodePath = zNodeInfoMap.get(OrchestrationClient.DEPZNODEPATH_KEY);
        }
    }
    
    @Override
    public void start()
    {
        if(!isRunning)
        {
            isRunning = true;
            logger.info("Starting the orchestartion client ...");
            
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
            logger.info("Orchestartion client has readly started...");
        }        
    }
    
    @Override
    public void stop()
    {
        if (isRunning)
        {
            isRunning = false;
            logger.info("stopping the orchestartion client ...");
            
            if (zookeeperThread != null)
            {
                zookeeperThread.interrupt();
            }
            if (zookeeperSync != null)
            {
                zookeeperSync.interrupt();
            }
            
            isRunning = false;
            isReady = false;
            zNodeState = OrchestrationZnodeState.OFFLINE;
            depData = null;
            handleSelfClosedEvent();
        }
        else
        {
            logger.info("Orchestartion client has readly stopped...");
        }
    }
    
    private void syncWithZookeeper()
    {
        while(isRunning)
        {
            if (zk != null)
            {
                WatchedEvent event = new WatchedEvent(Watcher.Event.EventType.NodeDataChanged, null, this.getDepZnodeChildPath());
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
    
    private String getRegZnodeChildPath()
    {
        return regZnodePath + "/" + zNodeServiceData.getServiceName();
    }
    
    private String getDepZnodeChildPath()
    {
        return depZnodePath + "/" + zNodeServiceData.getServiceGroupName();
    }
    
    private void setUpZnodeForOnline() throws KeeperException, InterruptedException
    {
        logger.info("setUpZnodeForOnline for: " + zNodeServiceData);
        regRealZnodeChildPath = ZkUtil.createEphemeralZNode(zk, getRegZnodeChildPath(), zNodeServiceData.createJsonObject(), false);
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
    
    private void waitingForZookeeper()
    {
        logger.info("Waiting for the zookeeper...");
        while(zk.getState() == ZooKeeper.States.CONNECTING && isRunning)
        {
            try
            {
                Thread.sleep(ZK_WAIT_INTERVAL);
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
    
    private void zookeeperEventHandler()
    {
        isRunning = true;
        if(!buildZookeeperConnection())
        {
            logger.error("Can not connection to zookeeper");
            return;
        }
        try
        {
            if(ZkUtil.checkZnodeExist(zk, regZnodePath) && ZkUtil.checkZnodeExist(zk, depZnodePath))
            {
                // 为什么是等于null，因为这个的regRealZnodeChildPath是创建子Node时返回的，如果没有创建该Node，就为null
                if(regRealZnodeChildPath == null || !ZkUtil.checkZnodeExist(zk, regRealZnodeChildPath))
                {
                    setUpZnodeForOnline();
                }
                zNodeState = OrchestrationZnodeState.ONLINE;
            }
            while(isRunning)
            {
                try
                {
                    if(zNodeState == OrchestrationZnodeState.OFFLINE)
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
                    logger.info("zookeeper is expired. need to reconnect");
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
            logger.error(e.getMessage());
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
            }
        }
        finally
        {
            if(zk != null)
            {
                try
                {
                    ZkUtil.closeZk(zk);
                    zk = null;
                } 
                catch (InterruptedException e)
                {
                    logger.error("Fail to close the zookeeper connection at begin");
                }
            }
        }
    }
    
    /**
     * 监听regZnode的创建或者数据修改的事件，一旦regZnode创建成功，就可以上线Znode了
     * @throws KeeperException 
     */
    private void handerZookeeperEventOffLine() throws KeeperException
    {
        logger.info("start zookeeper event handler for off line ...");
        try
        {
            ZkUtil.watchZnodeChange(zk, regZnodePath, zkWatcher);
            while(isRunning && zNodeState == OrchestrationZnodeState.OFFLINE)
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
                    if(event.getPath().equals(regZnodePath))
                    {
                        zNodeState = OrchestrationZnodeState.ONLINE;
                        logger.info("Znode is on line " + event.getPath());
                        return;
                    }
                    String path = event.getPath();
                    ZkUtil.watchZnodeChildeChange(zk, path, zkWatcher);
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
            }
        }
        logger.info("end zookeeper event handler for off line ...");
    }
    
    /**
     * 监听两种事件：
     * 
     * 1.depZnodeChild的创建，修改和删除事件
     * @throws KeeperException 
     * 
     */
    private void handerZookeeperEventOnLine() throws KeeperException
    {
        logger.info("Start zookeeper event handler for on line ...");
        try
        {
            //由offline转为online
            if(regRealZnodeChildPath == null || !ZkUtil.checkZnodeExist(zk, regRealZnodeChildPath))
            {
                //在checkZnodeExist时会抛异常：KeeperErrorCode = Session expired for /regManageNode/ServiceC
                //这里应该是zookeeper连接有问题了，所以需要进行重连
                //可能的情况是regZnode被删除了，但是regRealZnodeChildPath没有重置为null
                setUpZnodeForOnline();
            }
            if(!ZkUtil.checkZnodeExist(zk, this.depZnodePath))
            {
                Thread.sleep(OrchestrationClient.ZKNODE_REGCHILDPATH_WAITTIME);
                if (!ZkUtil.checkZnodeExist(zk, this.depZnodePath))
                {
                    zNodeState = OrchestrationZnodeState.OFFLINE;
                    return;
                }
            }
            String depZnodeChildPath = getDepZnodeChildPath();
            ZkUtil.watchZnodeChange(zk, depZnodeChildPath, zkWatcher);
            
            //先检查是否已经存在depZnodeChildPath了，如果存在了，可以先获取一下
            if(ZkUtil.checkZnodeExist(zk, depZnodeChildPath))
            {
                if(!this.isReady)
                {
                    handleDepZnodeChildCreateEvent();
                }
            }
            
            while(isRunning && this.zNodeState == OrchestrationZnodeState.ONLINE)
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
                if(path == null || !path.equals(depZnodeChildPath))
                {
                    logger.error("Unknown event for online: " + event.toString());
                    continue;
                }
                
                if(event.getType() == Watcher.Event.EventType.NodeCreated)
                {
                    handleDepZnodeChildCreateEvent();
                }
                else if(event.getType() == Watcher.Event.EventType.NodeDeleted)
                {
                    handleDepZnodeChildDeleteEvent();
                }
                else if(event.getType() == Watcher.Event.EventType.NodeDataChanged)
                {
                    handleDepZnodeChildDataChangeEvent();
                }
                ZkUtil.watchZnodeChange(zk, depZnodeChildPath, zkWatcher);
            }
        } 
        catch (KeeperException e)
        {
            if(e instanceof KeeperException.SessionExpiredException)
            {
                isReady = false;
                zNodeState = OrchestrationZnodeState.OFFLINE;
                depData = null;
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
        logger.info("End zookeeper event handler for on line ...");
    }
    
    private void handleDepZnodeChildCreateEvent() throws KeeperException, InterruptedException
    {
        this.isReady = true;
        this.depData = ZNodeDataUtil.getZNodeDependenceData(ZkUtil.getData(zk, getDepZnodeChildPath()));
        if(callback != null)
        {
            this.callback.handleServiceStateChange(OrchestrationServiceState.READY); 
        }
    }
    
    private void handleDepZnodeChildDeleteEvent() throws KeeperException, InterruptedException
    {
        this.isReady = false;
        this.depData = null;
        if(callback != null)
        {
            this.callback.handleServiceStateChange(OrchestrationServiceState.NOTREADY); 
        }
    }
    
    /** 这里可能接到了自己创建的dataChange event，从而触发 */
    private void handleDepZnodeChildDataChangeEvent() throws KeeperException, InterruptedException
    {
        //自己产生的，但是其实还没有创建对于的depZnode
        if (!ZkUtil.checkZnodeExist(zk, getDepZnodeChildPath()))
        {
            return;
        }
        boolean isSend = false;
        if (!isReady)
        {
            this.isReady = true;
            isSend = true;
        }
        ZNodeDependenceData data = ZNodeDataUtil.getZNodeDependenceData(ZkUtil.getData(zk, getDepZnodeChildPath()));
        if(isSend || !data.equals(this.depData))
        {
            this.depData = data;
            if(callback != null)
            {
                this.callback.handleServiceStateChange(OrchestrationServiceState.DEPCHANGE); 
            }
        }
    }
    
    private void handleSelfClosedEvent()
    {
        if(callback != null)
        {
            this.callback.handleServiceStateChange(OrchestrationServiceState.NOTREADY); 
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

    @Override
    public boolean isReady()
    {
        return this.isReady;
    }

    @Override
    public ZNodeDependenceData getDepData()
    {
        return this.depData;
    }

    @Override
    public OrchestrationZnodeState getZnodeState()
    {
        return this.zNodeState;
    }
    
}
