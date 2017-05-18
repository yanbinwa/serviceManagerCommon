package yanbinwa.common.utils;

import org.junit.Test;

import yanbinwa.common.http.HttpResult;

public class HttpUtilTest
{

    @Test
    public void test()
    {
        String url = "http://localhost:8080/iOrchestration/getReadyService";
        String type = "GET";
        String payLoad = null;
        HttpResult ret = HttpUtil.httpRequest(url, payLoad, type);
        System.out.println(ret);
    }

}
