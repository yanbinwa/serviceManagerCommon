package yanbinwa.common.ssh;

import static org.junit.Assert.*;

import org.junit.Test;

public class RemoteShellExecutorTest
{

    @Test
    public void test()
    {
        String cmd = "/bin/sh /root/yanbinwa/test/simpleShell.sh";
        RemoteShellExecutor executor = new RemoteShellExecutor("192.168.56.17", "root", "Wyb13403408973");
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
