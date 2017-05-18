package yanbinwa.common.utils;

import java.io.IOException;
import java.util.List;

import org.apache.log4j.Logger;
import org.apache.zookeeper.CreateMode;
import org.apache.zookeeper.KeeperException;
import org.apache.zookeeper.Watcher;
import org.apache.zookeeper.ZooDefs.Ids;
import org.apache.zookeeper.ZooKeeper;
import org.json.JSONObject;

import yanbinwa.common.constants.CommonConstants;

/**
 * 
 * 主要实现连接zookeeper，创建永久节点和临时节点，查看节点，监听节点以及删除节点
 * 
 * @author yanbinwa
 *
 */
public class ZkUtil
{
    private static final Logger logger = Logger.getLogger(ZkUtil.class);
    
    public static ZooKeeper connectToZk(String hostPort, Watcher watcher)
    {
        ZooKeeper zk = null;
        try
        {
            zk = new ZooKeeper(hostPort, CommonConstants.ZK_TIMEOUT, watcher);
        } 
        catch (IOException e)
        {
            logger.error("Can not connect to zk with " + hostPort);
        }
        return zk;
    }
    
    public static void closeZk(ZooKeeper zk) throws InterruptedException
    {
        zk.close();
    }
    
    public static boolean checkZnodeExist(ZooKeeper zk, String path) throws KeeperException, InterruptedException
    {
        if (zk.exists(path, null) == null)
        {
            return false;
        }
        else
        {
            return true;
        }
    }
    
    public static List<String> getChildren(ZooKeeper zk, String path) throws KeeperException, InterruptedException
    {
        return zk.getChildren(path, false);
    }
    
    public static JSONObject getData(ZooKeeper zk, String path) throws KeeperException, InterruptedException
    {
        byte[] data = zk.getData(path, false, null);
        if(data == null)
        {
            return null;
        }
        JSONObject obj = new JSONObject(new String(data));
        return obj;
    }
    
    public static void setData(ZooKeeper zk, String path, JSONObject obj) throws KeeperException, InterruptedException
    {
        byte[] data = obj.toString().getBytes();
        zk.setData(path, data, -1);
    }
    
    /**
     * 无论该节点是创建，删除，还是内容修改，都会调用watcher
     * 
     * 
     * @param zk
     * @param path
     * @param watcher
     * @throws KeeperException
     * @throws InterruptedException
     */
    public static void watchZnodeChange(ZooKeeper zk, String path, Watcher watcher) throws KeeperException, InterruptedException
    {
        zk.exists(path, watcher);
    }
    
    /**
     * 当前节点的删除，或者其子节点的增删，都会触发watcher
     * 
     * @param zk
     * @param path
     * @param watcher
     * @throws KeeperException
     * @throws InterruptedException
     */
    public static void watchZnodeChildeChange(ZooKeeper zk, String path, Watcher watcher) throws KeeperException, InterruptedException
    {
        zk.getChildren(path, watcher);
    }
    
    public static String createPersistentZNode(ZooKeeper zk, String path, JSONObject obj) throws KeeperException, InterruptedException
    {
        return createPersistentZNode(zk, path, obj, false);
    }
    
    public static String createPersistentZNode(ZooKeeper zk, String path, JSONObject obj, boolean isSequential) throws KeeperException, InterruptedException
    {
        byte[] data = obj.toString().getBytes();
        CreateMode mode = null;
        if(isSequential)
        {
            mode = CreateMode.PERSISTENT_SEQUENTIAL;
        }
        else
        {
            mode = CreateMode.PERSISTENT;
        }
        return zk.create(path, data, Ids.OPEN_ACL_UNSAFE, mode);
    }
    
    public static String createEphemeralZNode(ZooKeeper zk, String path, JSONObject obj) throws KeeperException, InterruptedException
    {
        return createEphemeralZNode(zk, path, obj, false);
    }
    
    public static String createEphemeralZNode(ZooKeeper zk, String path, JSONObject obj, boolean isSequential) throws KeeperException, InterruptedException
    {
        byte[] data = obj.toString().getBytes();
        CreateMode mode = null;
        if(isSequential)
        {
            mode = CreateMode.EPHEMERAL_SEQUENTIAL;
        }
        else
        {
            mode = CreateMode.EPHEMERAL;
        }
        return zk.create(path, data, Ids.OPEN_ACL_UNSAFE, mode);
    }
    
    public static void deleteZnode(ZooKeeper zk, String path) throws InterruptedException, KeeperException
    {
        zk.delete(path, -1);
    }
    
    public static String getNoneSequentialZnodeName(String path)
    {
        if(path == null)
        {
            return null;
        }
        String[] pathList = path.split("/");
        return pathList[pathList.length - 1];
    }
    
    public static String getSequentialZnodeName(String path)
    {
        if(path == null)
        {
            return null;
        }
        String[] pathList = path.split("/");
        String nodeName =  pathList[pathList.length - 1];
        if (nodeName.length() < CommonConstants.SEQUENTIAL_SUFFIX_LENGTH)
        {
            return null;
        }
        nodeName = nodeName.substring(0, nodeName.length() - CommonConstants.SEQUENTIAL_SUFFIX_LENGTH);
        return nodeName;
    }
    
}
