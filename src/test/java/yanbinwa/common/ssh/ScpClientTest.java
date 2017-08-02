package yanbinwa.common.ssh;

import static org.junit.Assert.*;

import java.io.IOException;

import org.junit.Test;

public class ScpClientTest
{

    @Test
    public void test()
    {
        ScpClient client = new ScpClient("192.168.56.17", "root", "Wyb13403408973");
                
        String localFile = "/Users/yanbinwa/Documents/workspace/springboot/serviceManager/serviceManagerCommon/README.md";
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
