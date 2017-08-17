package yanbinwa.common.ssh;

import static org.junit.Assert.*;

import java.io.IOException;

import org.junit.Test;

import yanbinwa.common.constants.CommonConstantsTest;

public class ScpClientTest
{

    @Test
    public void test()
    {
        ScpClient client = new ScpClient(CommonConstantsTest.TEST_SERVER_IP, "root", "emotibot");
                
        String localFile = "/Users/emotibot/Documents/workspace/serviceManager/serviceManagerCommon/README.md";
        String remoteFile = "/tmp";
        try
        {
            client.putFile(localFile, remoteFile);
        } 
        catch (IOException e)
        {
            e.printStackTrace();
            fail(e.getMessage());
        }
    }
}
