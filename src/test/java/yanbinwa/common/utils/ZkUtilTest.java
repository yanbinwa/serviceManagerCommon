package yanbinwa.common.utils;

import static org.junit.Assert.*;

import java.util.List;

import org.apache.zookeeper.KeeperException;
import org.apache.zookeeper.WatchedEvent;
import org.apache.zookeeper.Watcher;
import org.apache.zookeeper.ZooKeeper;
import org.json.JSONObject;
import org.junit.Test;

import yanbinwa.common.utils.ZkUtil;
import yanbinwa.common.zNodedata.ZNodeServiceData;

public class ZkUtilTest
{
    
    @Test
    public void connectToZk()
    {
        ZooKeeper zk = null;
        Watcher wt = new Watcher(){

            @Override
            public void process(WatchedEvent event)
            {
                System.out.println(event.toString());
            }
        };
        zk = ZkUtil.connectToZk("192.168.56.17:2181", wt);
        if (zk == null)
        {
            fail("Can not create znode");
        }
        if (zk.getState() == ZooKeeper.States.CONNECTING)
        {
            System.out.println("Can not connetion to zookeeper " + "192.168.56.17:2181");
            return;
        }
        try
        {            
            String path = "/manager";
            String childPath = "/manager/node1";
            
            String dataStr = "{\"maxResult\":\"10\"}";
            JSONObject data = new JSONObject(dataStr);
            
            String dataStr1 = "{\"maxResult\":\"9\"}";
            JSONObject data1 = new JSONObject(dataStr1);
            
            if(ZkUtil.checkZnodeExist(zk, path))
            {
                //fail("Path should not exist " + path);
                ZkUtil.deleteZnode(zk, path);
            }
                
            //可以重复加watcher
            ZkUtil.watchZnodeChange(zk, path, wt);
            ZkUtil.watchZnodeChange(zk, path, wt);
                        
            ZkUtil.createPersistentZNode(zk, path, data);
            
            try
            {
                ZkUtil.createPersistentZNode(zk, path, data);
            }
            catch(KeeperException e)
            {
                if(e.code() != KeeperException.Code.NODEEXISTS)
                {
                    fail("Error exception: " + e.getMessage());
                }
            }
                        
            List<String> children = ZkUtil.getChildren(zk, path);
            
            if (children.size() != 0)
            {
                fail("Path should not exist " + path);
            }
            
            ZkUtil.watchZnodeChildeChange(zk, path, wt);
            
            String trueChildPath1 = ZkUtil.createEphemeralZNode(zk, childPath, data, true);
            
            String trueChildPath2 = ZkUtil.createEphemeralZNode(zk, childPath, data, true);
            
            children = ZkUtil.getChildren(zk, path);
            
            if(children.size() == 0)
            {
                fail("Path should not be empty " + path);
            }
            else
            {
                System.out.println(children);
            }
            //返回为/manager/node10000000000 和 /manager/node10000000001
            System.out.println(trueChildPath1);
            System.out.println(trueChildPath2);
            
            ZkUtil.setData(zk, trueChildPath1, data1);
            
            JSONObject ret = ZkUtil.getData(zk, trueChildPath1);
            System.out.println(ret.toString());
            
            ZkUtil.watchZnodeChildeChange(zk, path, wt);
            
            //如果某个节点下还有子节点，那么该节点是无法删除的
            ZkUtil.deleteZnode(zk, trueChildPath1);
            ZkUtil.deleteZnode(zk, trueChildPath2);
            
            ZkUtil.deleteZnode(zk, path);
                        
            ZkUtil.closeZk(zk);
            
        } 
        catch (InterruptedException | KeeperException e)
        {
            e.printStackTrace();
            fail("Error");
        }
    }
    
    @Test
    public void getSequentialZnodeName()
    {
        String node1 = "/manager/node10000000000";
        String nodeName1 = ZkUtil.getSequentialZnodeName(node1);
        if(!nodeName1.equals("node1"))
        {
            fail("The nodename is not correct: " + nodeName1);
        }
        System.out.println("nodeName is: " + nodeName1);
        
        String node2 = "/manager/node2";
        String nodeName2 = ZkUtil.getNoneSequentialZnodeName(node2);
        if(!nodeName2.equals("node2"))
        {
            fail("The nodename is not correct: " + nodeName2);
        }
        System.out.println("nodeName is: " + nodeName2);
    }
    
    @Test
    public void zNodeServiceDataTest()
    {
        ZNodeServiceData zNodeData = new ZNodeServiceData("1", "1", 1, "1");
        System.out.println(zNodeData.getPort());
    }

}
