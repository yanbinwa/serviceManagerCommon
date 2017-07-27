//package yanbinwa.common.configClient;
//
//import java.util.HashMap;
//import java.util.Map;
//
//import org.junit.Test;
//
//import yanbinwa.common.zNodedata.ZNodeServiceData;
//import yanbinwa.common.zNodedata.ZNodeServiceDataImpl;
//
//public class ConfigClientImplTest
//{
//
//    class ConfigClientImplTestConfigCallback implements ConfigCallBack
//    {
//
//        @Override
//        public void handleServiceConfigChange(ServiceConfigState state)
//        {
//            System.out.println(state);
//        }
//        
//    }
//    
//    @Test
//    public void test() throws InterruptedException
//    {
//        ZNodeServiceData zNodeData = new ZNodeServiceDataImpl("172.18.0.21", "orchestration", "orchestration_standalone", 8091, "/iOrchestration");
//        Map<String, String> zNodeInfoMap = new HashMap<String, String>();
//        zNodeInfoMap.put("confZnodePath", "/confManageNode");
//        ConfigCallBack callBack = new ConfigClientImplTestConfigCallback();
//        ConfigClient client = new ConfigClientImpl(zNodeData, callBack, "192.168.56.17:2181", zNodeInfoMap);
//        client.start();
//        while(true)
//        {
//            Thread.sleep(1000);
//        }
//    }
//
//}
