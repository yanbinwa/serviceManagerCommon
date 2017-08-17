package yanbinwa.common.utils;

import static org.junit.Assert.*;

import java.util.HashMap;
import java.util.Map;

import org.junit.Test;

import kafka.common.TopicAlreadyMarkedForDeletionException;
import kafka.utils.ZkUtils;
import yanbinwa.common.constants.CommonConstants;
import yanbinwa.common.constants.CommonConstantsTest;

public class KafkaUtilTest
{

    @Test
    public void test()
    {
        String topicName = "wyb_new";
        Map<String, Object> prop = new HashMap<String, Object>();
        prop.put(CommonConstants.ZOOKEEPER_HOSTPORT_KEY, CommonConstantsTest.TEST_ZOOKEEPERHOSTPORT);
        prop.put(CommonConstants.KAFKA_TOPIC_KEY, topicName);
        ZkUtils zkUtils = KafkaUtil.createZkUtils(prop);
        try
        {
            boolean ret = KafkaUtil.isTopicExist(zkUtils, prop);
            if (ret)
            {
                System.out.println("topic " + topicName + " has exist, need to delete first");
                KafkaUtil.deleteTopic(zkUtils, prop);
            }
            else
            {
                System.out.println("topic " + topicName + " does not exist");
            }
            KafkaUtil.createTopic(zkUtils, prop);
            ret = KafkaUtil.isTopicExist(zkUtils, prop);
            Thread.sleep(200);
            if (!ret)
            {
                fail("fail to test create or delete topic");
            }
            else
            {
                System.out.println("topic " + topicName + " has been create successful");
            }
            KafkaUtil.deleteTopic(zkUtils, prop);
            Thread.sleep(200);      //这里非常重要，需要给zookeeper一些时间来删除
            ret = KafkaUtil.isTopicExist(zkUtils, prop);
            if (ret)
            {
                fail("fail to test create or delete topic");
            }
        }
        catch(TopicAlreadyMarkedForDeletionException | InterruptedException e)
        {
            e.printStackTrace();
        }
        finally
        {
            KafkaUtil.closeZkUtils(zkUtils);
        }
    }

}
