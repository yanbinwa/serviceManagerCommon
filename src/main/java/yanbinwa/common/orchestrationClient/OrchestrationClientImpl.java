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
    
    /** orchestartion服务的子node */
    String regZnodeChildPath;
    
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
    boolean isRunning = true;
    
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
            regZnodeChildPath = null;
        }
        else
        {
            regZnodePath = zNodeInfoMap.get(OrchestrationClient.REGZNODEPATH_KEY);
            depZnodePath = zNodeInfoMap.get(OrchestrationClient.DEPZNODEPATH_KEY);
            regZnodeChildPath = zNodeInfoMap.get(OrchestrationClient.REGZNODECHILDPATH_KEY);
        }
    }
    
    @Override
    public void start()
    {
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
    
    @Override
    public void stop()
    {
        isRunning = false;
        if (zookeeperThread != null)
        {
            zookeeperThread.interrupt();
        }
        if (zookeeperSync != null)
        {
            zookeeperSync.interrupt();
        }
    }
    
    private void syncWithZookeeper()
    {
        while(isRunning)
        {
            //五分钟同步一次
            WatchedEvent event = new WatchedEvent(Watcher.Event.EventType.NodeDataChanged, null, this.getDepZnodeChildPath());
            zookeeperEventQueue.offer(event);
            try
            {
                Thread.sleep(OrchestrationClient.ZK_SYNC_INTERVAL);
            } 
            catch (InterruptedException e)
            {
                logger.info("Close this thread");
            }
        }
    }
    
    private String getRegZnodeChildPath()
    {
        return regZnodePath + "/" + zNodeServiceData.getServiceName();
    }
    
    private String getDepZnodeChildPath()
    {
        return depZnodePath + "/" + zNodeServiceData.getServiceName();
    }
    
    private void setUpZnodeForOnline() throws KeeperException, InterruptedException
    {
        regRealZnodeChildPath = ZkUtil.createEphemeralZNode(zk, getRegZnodeChildPath(), zNodeServiceData.createJsonObject(), true);
    }
    
    private void zookeeperEventHandler()
    {
        isRunning = true;
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
        zk = ZkUtil.connectToZk(zookeeperHostPort, zkWatcher);
        if(zk == null)
        {
            logger.error("Can not connect to zookeeper: " + zookeeperHostPort);
            return;
        }
        try
        {
            if(ZkUtil.checkZnodeExist(zk, regZnodePath) && ZkUtil.checkZnodeExist(zk, depZnodePath))
            {
                setUpZnodeForOnline();
                zNodeState = OrchestrationZnodeState.ONLINE;
            }
            while(isRunning)
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
            
        } 
        catch (KeeperException e)
        {
            e.printStackTrace();
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
     */
    private void handerZookeeperEventOffLine()
    {
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
                        continue;
                    }
                    String path = event.getPath();
                    ZkUtil.watchZnodeChildeChange(zk, path, zkWatcher);
                }                
            }
        } 
        catch (KeeperException e)
        {
            e.printStackTrace();
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
    }
    
    /**
     * 监听两种事件：
     * 
     * 1.depZnodeChild的创建，修改和删除事件
     * 
     */
    private void handerZookeeperEventOnLine()
    {
        try
        {
            //由offline转为online
            if(!ZkUtil.checkZnodeExist(zk, getRegZnodeChildPath()))
            {
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
            
            //先检查是否已经存在depZnodeChildPath了
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
            e.printStackTrace();
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
    }
    
    private void handleDepZnodeChildCreateEvent() throws KeeperException, InterruptedException
    {
        this.isReady = true;
        this.depData = new ZNodeDependenceData(ZkUtil.getData(zk, getDepZnodeChildPath()));
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
        ZNodeDependenceData data = new ZNodeDependenceData(ZkUtil.getData(zk, getDepZnodeChildPath()));
        if(isSend || !data.equals(this.depData))
        {
            this.depData = data;
            if(callback != null)
            {
                this.callback.handleServiceStateChange(OrchestrationServiceState.DEPCHANGE); 
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
