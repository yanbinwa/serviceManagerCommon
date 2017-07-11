package yanbinwa.common.zNodedata;

import yanbinwa.common.zNodedata.decorate.ZNodeDecorateType;
import yanbinwa.common.zNodedata.decorate.ZNodeServiceDataDecorate;

public interface ZNodeServiceData extends ZNodeData
{   
    String getIp();
    
    String getServiceName();
    
    String getServiceGroupName();
    
    int getPort();
    
    String getRootUrl();
    
    void addServiceDataDecorate(ZNodeDecorateType type, Object obj);
    
    boolean isContainedDecoreate(ZNodeDecorateType type);
    
    ZNodeServiceDataDecorate getServiceDataDecorate(ZNodeDecorateType type);
}
