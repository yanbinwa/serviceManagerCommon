package yanbinwa.common.zNodedata;

import java.util.Map;
import java.util.Set;

import yanbinwa.common.zNodedata.decorate.ZNodeDecorateType;
import yanbinwa.common.zNodedata.decorate.ZNodeDependenceDataDecorate;

public interface ZNodeDependenceData extends ZNodeData
{
    public Map<String, Set<ZNodeServiceData>> getDependenceData();
    
    void addDependenceDataDecorate(ZNodeDecorateType type, Object obj);
    
    boolean isContainedDecorate(ZNodeDecorateType type);
    
    ZNodeDependenceDataDecorate getDependenceDataDecorate(ZNodeDecorateType type);
}
