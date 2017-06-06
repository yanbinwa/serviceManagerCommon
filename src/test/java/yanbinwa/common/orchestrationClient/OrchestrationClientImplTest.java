package yanbinwa.common.orchestrationClient;

import static org.junit.Assert.*;

import java.util.HashMap;
import java.util.Map;

import org.junit.Test;

import yanbinwa.common.zNodedata.ZNodeDependenceData;
import yanbinwa.common.zNodedata.ZNodeServiceData;
import yanbinwa.common.zNodedata.ZNodeServiceDataImpl;

public class OrchestrationClientImplTest
{

    @Test
    public void test()
    {
        String zookeeperHostPort = "192.168.56.17:2181";
        Map<String, String> zNodeProperties = new HashMap<String, String>();
        zNodeProperties.put(OrchestrationClient.REGZNODEPATH_KEY, "/regManageNode");
        zNodeProperties.put(OrchestrationClient.DEPZNODEPATH_KEY, "/depManageNode");
        zNodeProperties.put(OrchestrationClient.REGZNODECHILDPATH_KEY, "/regManageNode/regManageChildNode");
//        ZNodeServiceData data1 = new ZNodeServiceData("localhost", "ServiceA", 8080, "/rootUrl");
//        OrchestrationClient client1 = new OrchestrationClientImpl(data1, zookeeperHostPort, zNodeProperties);
//        ZNodeServiceData data2 = new ZNodeServiceData("localhost", "ServiceB", 8080, "/rootUrl");
//        OrchestrationClient client2 = new OrchestrationClientImpl(data2, zookeeperHostPort, zNodeProperties);
//        ZNodeServiceData data3 = new ZNodeServiceData("localhost", "ServiceC", 8080, "/rootUrl");
//        OrchestrationClient client3 = new OrchestrationClientImpl(data3, zookeeperHostPort, zNodeProperties);
//        client1.start();
//        client2.start();
//        client3.start();
//        try
//        {
//            Thread.sleep(1000);       //1000 * 60 * 10
//        } 
//        catch (InterruptedException e)
//        {
//            fail("InterruptedException error");
//            e.printStackTrace();
//        }
//        boolean isReady = client1.isReady();
//        OrchestrationZnodeState znodeState = client1.getZnodeState();
//        if(isReady)
//        {
//            ZNodeDependenceData depData = client1.getDepData();
//            System.out.println("Dep data: " + depData + "; Is ready: " + isReady + "; Znode state: " + znodeState);
//        }
//        else
//        {
//            System.out.println("Is ready: " + isReady + "; Znode state: " + znodeState);
//        }
//        
//        client1.stop();
//        client2.stop();
//        client3.stop();
        
        ZNodeServiceData data3 = new ZNodeServiceDataImpl("localhost", "collection", "ServiceC", 8080, "/rootUrl");
        OrchestrationClient client3 = new OrchestrationClientImpl(data3, zookeeperHostPort, zNodeProperties);
        client3.start();
        try
        {
            Thread.sleep(1000 * 5);       //1000 * 60 * 10
        } 
        catch (InterruptedException e)
        {
            fail("InterruptedException error");
            e.printStackTrace();
        }
        boolean isReady = client3.isReady();
        OrchestrationZnodeState znodeState = client3.getZnodeState();
        if(isReady)
        {
            ZNodeDependenceData depData = client3.getDepData();
            System.out.println("Dep data: " + depData + "; Is ready: " + isReady + "; Znode state: " + znodeState);
        }
        else
        {
            System.out.println("Is ready: " + isReady + "; Znode state: " + znodeState);
        }
        
        client3.stop();
    }

}
