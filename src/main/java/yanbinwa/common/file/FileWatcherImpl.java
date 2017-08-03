package yanbinwa.common.file;

import java.io.File;
import java.util.HashMap;
import java.util.Map;

import org.apache.log4j.Logger;

/*
 * 监控多个文件的创建，修改和删除，并通过callback发送结果
 * （返回应该是一个Map，key为文件，value为其文件的状态）
 * 
 * 内部结构是一个Map，key为文件，value为该文件的修改时间，初始时间是-1，会定期去遍历所有register的
 * 文件
 * 
 * 1. 如果初始时间为-1，则要查看文件是否存在，如果存在了，说明该文件被创建了，发送create状态
 * 
 * 2. 如果初始时间不为-1，则查看文件是否存在，如果不存在了，说明该文件被销毁了，发送delete状态
 * 
 * 3. 如果初始时间不为-1，并且文件存在，查看其修改时间，如果与初始时间不一致，说明文件被改动了，发送change状态
 * 
 */
public class FileWatcherImpl implements FileWatcher
{

    private static final Logger logger = Logger.getLogger(FileWatcherImpl.class);
    
    private Map<String, Long> fileNameToChangeTimestamp = new HashMap<String, Long>();
    
    FileWatcherCallback callback = null;
    
    boolean isRunning = false;
    
    Thread fileWatcherThread = null;
    
    public FileWatcherImpl(FileWatcherCallback callback)
    {
        this.callback = callback;
    }
    
    @Override
    public void registerFile(String filePath)
    {
        fileNameToChangeTimestamp.put(filePath, INIT_FILE_CHANGE_TIMESTAMP);
    }

    @Override
    public void unRegisterFile(String filePath)
    {
        fileNameToChangeTimestamp.remove(filePath);
    }

    @Override
    public void start()
    {
        if (!isRunning)
        {
            logger.info("start FileWatcher");
            isRunning = true;
            fileWatcherThread = new Thread(new Runnable(){

                @Override
                public void run()
                {
                    watcherFiles();
                }
                
            });
            fileWatcherThread.start();
        }
        else
        {
            logger.info("FileWatcher is already start");
        }
    }

    @Override
    public void stop()
    {
        if (isRunning)
        {
            logger.info("stop FileWatcher");
            isRunning = false;
            fileWatcherThread.interrupt();
            fileWatcherThread = null;
            fileNameToChangeTimestamp.clear();
            if (callback != null)
            {
                callback.handleFileWatcherClose();
            }
        }
        else
        {
            logger.info("FileWatcher is already stop");
        }
    }  
    
    private void watcherFiles()
    {
        logger.info("start to watcher file");
        while(isRunning)
        {
            Map<String, FileStatus> watchResult = new HashMap<String, FileStatus>();
            for (Map.Entry<String, Long> entry : fileNameToChangeTimestamp.entrySet())
            {
                String filePath = entry.getKey();
                long fileChangeTimestamp = entry.getValue();
                if (fileChangeTimestamp == INIT_FILE_CHANGE_TIMESTAMP)
                {
                    boolean ret = isFileExist(filePath);
                    if (ret)
                    {
                        long lastChangeTimestamp = getFileLastChangeTimestamp(filePath);
                        fileNameToChangeTimestamp.put(filePath, lastChangeTimestamp);
                        watchResult.put(filePath, FileStatus.CREATE);
                    }
                }
                else
                {
                    boolean ret = isFileExist(filePath);
                    if (!ret)
                    {
                        fileNameToChangeTimestamp.put(filePath, INIT_FILE_CHANGE_TIMESTAMP);
                        watchResult.put(filePath, FileStatus.DELETE);
                    }
                    else
                    {
                        long lastChangeTimestamp = getFileLastChangeTimestamp(filePath);
                        if (lastChangeTimestamp != fileChangeTimestamp)
                        {
                            fileNameToChangeTimestamp.put(filePath, lastChangeTimestamp);
                            watchResult.put(filePath, FileStatus.CHANGE);
                        }
                    }
                }
            }
            if (!watchResult.isEmpty() && callback != null)
            {
                callback.handleFileStatueChange(watchResult);
            }
            try
            {
                Thread.sleep(FILE_WATCHER_INTERVAL);
            } 
            catch (InterruptedException e)
            {
                if (!isRunning)
                {
                    logger.info("fileWatcherThread is interepter");
                }
                else
                {
                    logger.error(e.getMessage());
                }
            }
        }
    }
    
    private boolean isFileExist(String filePath)
    {
        File f = new File(filePath);
        return f.exists();
    }
    
    private long getFileLastChangeTimestamp(String filePath)
    {
        File f = new File(filePath);
        return f.lastModified();
    }
    
}
