package yanbinwa.common.file;

import yanbinwa.common.iInterface.ServiceLifeCycle;

public interface FileWatcher extends ServiceLifeCycle
{
    
    public static final Long INIT_FILE_CHANGE_TIMESTAMP = -1L;
    public static final int FILE_WATCHER_INTERVAL = 5000;
    
    void registerFile(String filePath);
    
    void unRegisterFile(String filePath);
}
