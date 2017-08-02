package yanbinwa.common.ssh;

public class SshResult
{
    private String stdOut = null;
    
    private String stdErr = null;
    
    private int stdState = -1;
    
    public SshResult(String stdOut, String stdErr, int stdState)
    {
        this.stdOut = stdOut;
        this.stdErr = stdErr;
        this.stdState = stdState;
    }
    
    public String getStdOut()
    {
        return this.stdOut;
    }
    
    public String getStdErr()
    {
        return this.stdErr;
    }
    
    public int getStdState()
    {
        return this.stdState;
    }
    
    @Override
    public String toString()
    {
        StringBuilder ret = new StringBuilder();
        ret.append("stdOut : " + stdOut).append("; ")
           .append("stdErr : " + stdErr).append("; ")
           .append("stdState : " + stdState);
        
        return ret.toString();
    }
}
