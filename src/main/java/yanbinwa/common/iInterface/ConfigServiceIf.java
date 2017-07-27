package yanbinwa.common.iInterface;

public interface ConfigServiceIf
{
    
    /**
     * When service get the config info, it will call the method to start work
     */
    public void startWork();
    
    /**
     * When service find the config znode is delete, it will call the method to stop work
     */
    public void stopWork();
}
