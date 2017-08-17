package yanbinwa.common.ssh;

import static org.junit.Assert.*;

import org.junit.Test;

import yanbinwa.common.constants.CommonConstantsTest;

public class RemoteShellExecutorTest
{

    @Test
    public void test()
    {
        String cmd = "/bin/sh /root/yanbinwa/test/remoteShellExecutorTest.sh";
        RemoteShellExecutor executor = new RemoteShellExecutor(CommonConstantsTest.TEST_SERVER_IP, "root", "emotibot");
        try
        {
            executor.exec(cmd);
        } 
        catch (Exception e)
        {
            fail(e.getMessage());
        }
    }

}
