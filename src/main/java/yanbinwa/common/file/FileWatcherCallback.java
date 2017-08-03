package yanbinwa.common.file;

import java.util.Map;

public interface FileWatcherCallback
{
    void handleFileStatueChange(Map<String, FileStatus> fileNameToFileStateMap);
    
    void handleFileWatcherClose();
}
