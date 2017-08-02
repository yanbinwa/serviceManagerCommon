package yanbinwa.common.ssh;

import java.io.IOException;

import org.apache.log4j.Logger;

import ch.ethz.ssh2.Connection;
import ch.ethz.ssh2.SCPClient;

public class ScpClient
{
    private static final Logger logger = Logger.getLogger(ScpClient.class);
    
    private Connection conn = null;
    private String ip = null;
    private String userName = null;
    private String password = null;
    
    public ScpClient(String IP, String userName, String passward) {
        this.ip = IP;
        this.userName = userName;
        this.password = passward;
    }
    
    private boolean login() throws IOException
    {
        conn = new Connection(ip);
        conn.connect();
        return conn.authenticateWithPassword(userName, password);
    }
    
    public void getFile(String remoteFile, String localDir) throws IOException {
        try 
        {
            if (login())
            {
                SCPClient client = new SCPClient(conn);
                client.get(remoteFile, localDir);
                logger.trace("copy remoteFile " + remoteFile + " to localDir " + localDir);
            }
        } 
        finally 
        {
            if (conn != null)
            {
                conn.close();
            }
        }
    }
    
    public void putFile(String localFile, String remoteDir) throws IOException
    {
        try 
        {
            if (login())
            {
                SCPClient client = new SCPClient(conn);
                client.put(localFile, remoteDir);
                logger.trace("copy localFile " + localFile + " to remoteDir " + remoteDir);
            }
        } 
        finally 
        {
            if (conn != null)
            {
                conn.close();
            }
        }
    }
}
