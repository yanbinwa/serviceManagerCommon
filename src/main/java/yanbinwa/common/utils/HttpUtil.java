package yanbinwa.common.utils;

import java.io.IOException;
import java.io.InputStream;
import java.io.OutputStreamWriter;
import java.net.HttpURLConnection;
import java.net.URL;

import org.apache.commons.io.IOUtils;
import org.apache.log4j.Logger;

import yanbinwa.common.constants.CommonConstants;
import yanbinwa.common.http.HttpMethod;
import yanbinwa.common.http.HttpResult;

public class HttpUtil
{
    public static final Logger logger = Logger.getLogger(HttpUtil.class);
    
    public static HttpURLConnection getHttpConnection(String url, String type)
    {
        URL uri = null;
        HttpURLConnection con = null;
        try
        {
            uri = new URL(url);
            con = (HttpURLConnection) uri.openConnection();
            con.setRequestMethod(type);
            con.setDoOutput(true);
            con.setDoInput(true);
            con.setConnectTimeout(CommonConstants.HTTP_CONNECTION_TIMEOUT);
            con.setReadTimeout(CommonConstants.HTTP_READ_TIMEOUT);
            con.setRequestProperty("Content-Type", "application/json");
        }
        catch(Exception e)
        {
            logger.error( "connection i/o failed" );
        }
        return con;
    }
    
    public static HttpResult httpRequest(String url, String payLoad, String type)
    {
        HttpURLConnection con = getHttpConnection(url, type);
        HttpResult ret = new HttpResult();
        
        if (con == null)
        {
            logger.error("the http connection is null");
            return null;
        }
        
        try
        {
            if (payLoad != null && payLoad != "")
            {
                OutputStreamWriter out = new OutputStreamWriter(con.getOutputStream());
                out.write(payLoad);  
                out.flush();  
                out.close(); 
            }
            
            con.connect();
            
            InputStream inputStream = con.getInputStream();  
            String encoding = con.getContentEncoding();  
            String responseMessage = IOUtils.toString(inputStream, encoding);  
            int stateCode = con.getResponseCode();
            ret.setResponse(responseMessage);
            ret.setStateCode(stateCode);
            
        } 
        catch (IOException e)
        {
            logger.error(e.getMessage());
            ret.setStateCode(-1);
            return ret;
        } 
        finally
        {
            con.disconnect();
        }
        return ret;
    }
    
    public static String getHttpMethodStr(HttpMethod method)
    {
        if (method == null)
        {
            return null;
        }
        String methodStr = null;
        switch(method)
        {
        case GET:
            methodStr = "GET";
            break;
            
        case POST:
            methodStr = "POST";
            break;
            
        case PUT:
            methodStr = "PUT";
            break;
            
        case DEL:
            methodStr = "DELETE";
            break;
            
        default:
            break;
        }
        
        return methodStr;
    }
}
