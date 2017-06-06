package yanbinwa.common.kafka.zNodedata;


import org.json.JSONArray;
import org.json.JSONObject;
import org.junit.Test;

public class ZNodeDataTest
{

    @Test
    public void test()
    {
        JSONObject topicInfo = new JSONObject();
        JSONArray producerTopicGroup = new JSONArray();
        producerTopicGroup.put("cacheTopic");
        topicInfo.put("producers", producerTopicGroup);
        
        JSONObject topicGroupToTopic = new JSONObject();
        JSONArray topics = new JSONArray();
        topics.put("cacheTopic_1");
        topicGroupToTopic.put("cacheTopic", topics);
        topicInfo.put("consumers", topicGroupToTopic);
        System.out.println(topicInfo.toString());
    }

}
