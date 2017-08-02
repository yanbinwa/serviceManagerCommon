package yanbinwa.common.ssh;

import java.io.IOException;
import java.io.InputStream;
import java.nio.charset.Charset;

import org.apache.commons.io.IOUtils;
import org.apache.log4j.Logger;

import ch.ethz.ssh2.ChannelCondition;
import ch.ethz.ssh2.Connection;
import ch.ethz.ssh2.Session;
import ch.ethz.ssh2.StreamGobbler;

public class RemoteShellExecutor
{
    
    @SuppressWarnings("unused")
    private static final Logger logger = Logger.getLogger(RemoteShellExecutor.class);
    private static final int TIME_OUT = 1000 * 5 * 60;
    
    private Connection conn = null;
    private String ip = null;
    private String userName = null;
    private String password = null;
    private String charset = Charset.defaultCharset().toString();
    
    public RemoteShellExecutor(String ip, String userName, String password)
    {
        this.ip = ip;
        this.userName = userName;
        this.password = password;
    }
    
    private boolean login() throws IOException
    {
        conn = new Connection(ip);
        conn.connect();
        return conn.authenticateWithPassword(userName, password);
    }
    
    public SshResult exec(String cmds) throws Exception
    {
        InputStream stdOut = null;
        InputStream stdErr = null;
        SshResult sshResult = null;
        try
        {
            if (login())
            {
                Session session = conn.openSession();
                session.execCommand(cmds);
                stdOut = new StreamGobbler(session.getStdout());
                String outStr = processStream(stdOut, charset);
                stdErr = new StreamGobbler(session.getStderr());
                String outErr = processStream(stdErr, charset);
                session.waitForCondition(ChannelCondition.EXIT_STATUS, TIME_OUT);
                int ret = session.getExitStatus();
                sshResult = new SshResult(outStr, outErr, ret);
            }
            return sshResult;
        }
        finally
        {
            if (conn != null)
            {
                conn.close();
            }
            IOUtils.closeQuietly(stdOut);
            IOUtils.closeQuietly(stdErr);
        }
    }
    
    private String processStream(InputStream in, String charset) throws Exception 
    {
        byte[] buf = new byte[1024];
        StringBuilder sb = new StringBuilder();
        while (in.read(buf) != -1)
        {
            sb.append(new String(buf, charset));
        }
        return sb.toString();
    }
}
