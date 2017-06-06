package yanbinwa.common.zNodedata;

public interface ZNodeServiceData extends ZNodeData
{
    String getIp();
    
    String getServiceName();
    
    String getServiceGroupName();
    
    int getPort();
    
    String getRootUrl();
}
