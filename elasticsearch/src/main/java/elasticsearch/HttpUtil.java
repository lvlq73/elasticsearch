package elasticsearch;

import com.ning.http.client.*;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.InputStream;
import java.util.Map;
import java.util.Set;

/**
 * http请求工具类
 */
public class HttpUtil {
    private static Logger logger = LoggerFactory.getLogger(HttpUtil.class);

    private static final  String defualContentType="application/json;charset=utf-8";
    private static final  String defualAccepType="application/json;charset=utf-8";

    private static AsyncHttpClient client;

    /**
     * 初始化配置
     */
    public static void initConfig(){
        AsyncHttpClientConfig.Builder configBuilder=new AsyncHttpClientConfig.Builder();
        configBuilder.setMaxConnections(100);
        /*使用默认值：*/
        configBuilder.setConnectTimeout(3000);
        configBuilder.setReadTimeout(5000);
        configBuilder.setRequestTimeout(6000);
        client= getClient(configBuilder);
    }

    private static  AsyncHttpClient getClient(AsyncHttpClientConfig.Builder configBuilder){
            if(client==null){
                client=new AsyncHttpClient(configBuilder.build());
            }
            return client;
    }

    /**
     * get请求
     * @param url
     * @return
     */
    public static String httpResFulGET(String url){
        return httpResFulReturnString(url,"GET",defualContentType,defualAccepType,null);
    }

    /**
     * put请求
     * @param url
     * @param data
     * @return
     */
    public static String httpResFulPUT(String url,String data){
        return httpResFulReturnString(url,"GET",defualContentType,defualAccepType,data);
    }

    /**
     * post请求
     * @param url
     * @param data
     * @return
     */
    public static String httpResFulPost(String url,String data){
        return httpResFulReturnString(url,"POST",defualContentType,defualAccepType,data);
    }

    /**
     * delete请求
     * @param url
     * @return
     */
    public static String httpResFulDelete (String url){
        return httpResFulReturnString(url,"DELETE ",defualContentType,defualAccepType,null);
    }

    /**
     * 自定义请求方式
     * @param url
     * @param method
     * @param data
     * @return
     */
    public static String httpResFul(String url,String method,String data){
        return httpResFulReturnString(url,method,defualContentType,defualAccepType,data);
    }

    /**
     * http请求
     * @param url
     * @param method 请求方式 例如post 或 get
     * @param contentType 请求的数据格式
     * @param accept 接收的数据格式
     * @param data 数据
     * @return
     */
    public static String httpResFulReturnString(String url,String method,String contentType,String accept,Object data){
        initConfig();
        String result = null;
        logger.info("url: " + url);
        logger.info("request: " + data);
        try {
                RequestBuilder mRequestBuilder = new RequestBuilder();
                mRequestBuilder.setUrl(url);
                mRequestBuilder.setMethod(method);
                mRequestBuilder.addHeader("Content-Type",StringUtil.isEmpty(contentType)?defualContentType:contentType);
                mRequestBuilder.addHeader("Accept",StringUtil.isEmpty(accept)?defualAccepType:accept);
               if(data!=null){
                   if(data instanceof  Map){
                       Map  map = (Map) data;
                       if(map!=null&&map.size()>0){
                           Set<String> keys = map.keySet();
                           for(String key:keys){
                               mRequestBuilder.addFormParam(key, (String) map.get(key));
                           }
                       }
                   }else if(data instanceof String){
                       mRequestBuilder.setBody((String)data);
                   }else if(data instanceof byte[]){
                       mRequestBuilder.setBody((byte[])data);
                   }
               }
                Request request = mRequestBuilder.build();
                ListenableFuture<Response> rp = client.executeRequest(request,new AsyncCompletionHandler(){
                    public Object onCompleted(Response response) throws Exception {
                        return response;
                    }
                });

                Response response = rp.get();
                result =  response.getResponseBody();
                logger.info("response: " + response);
        } catch (Exception e) {
             logger.error(e.getMessage());
        }finally {
            if (client != null) {
                client.close();
            }
        }
        return result;
    }

    /**
     * get请求
     * @param url
     * @return
     */
    public static byte[] httpResFulReturnByteGET(String url){
        return httpResFulReturnByte(url,"GET",defualContentType,defualAccepType,null);
    }
    /**
     * put请求
     * @param url
     * @param data
     * @return
     */
    public static byte[] httpResFulReturnBytePUT(String url,String data){
        return httpResFulReturnByte(url,"GET",defualContentType,defualAccepType,data);
    }
    /**
     * post请求
     * @param url
     * @param data
     * @return
     */
    public static byte[] httpResFulReturnBytePost(String url,String data){
        return httpResFulReturnByte(url,"POST",defualContentType,defualAccepType,data);
    }
    /**
     * delete请求
     * @param url
     * @return
     */
    public static byte[] httpResFulReturnByteDelete (String url){
        return httpResFulReturnByte(url,"DELETE ",defualContentType,defualAccepType,null);
    }
    /**
     * 自定义请求方式
     * @param url
     * @param method
     * @param data
     * @return
     */
    public static byte[] httpResFulReturnByte(String url,String method,String data){
        return httpResFulReturnByte(url,method,defualContentType,defualAccepType,data);
    }
    /**
     * http请求
     * @param url
     * @param method 请求方式 例如post 或 get
     * @param contentType 请求的数据格式
     * @param accept 接收的数据格式
     * @param data 数据
     * @return
     */
    public static byte[]  httpResFulReturnByte(String url,String method,String contentType,String accept,Object data){
        initConfig();
        byte[]  result = null;
        logger.info("url: " + url);
        logger.info("request: " + data);
        try {
            RequestBuilder mRequestBuilder = new RequestBuilder();
            mRequestBuilder.setUrl(url);
            mRequestBuilder.setMethod(method);
            mRequestBuilder.addHeader("Content-Type",StringUtil.isEmpty(contentType)?defualContentType:contentType);
            mRequestBuilder.addHeader("Accept",StringUtil.isEmpty(accept)?defualAccepType:accept);
            if(data!=null){
                if(data instanceof  Map){
                    Map  map = (Map) data;
                    if(map!=null&&map.size()>0){
                        Set<String> keys = map.keySet();
                        for(String key:keys){
                            mRequestBuilder.addFormParam(key, (String) map.get(key));
                        }
                    }
                }else if(data instanceof String){
                    mRequestBuilder.setBody((String)data);
                }else if(data instanceof byte[]){
                    mRequestBuilder.setBody((byte[])data);
                }
            }
            Request request = mRequestBuilder.build();
            ListenableFuture<Response> rp = client.executeRequest(request,new AsyncCompletionHandler(){
                public Object onCompleted(Response response) throws Exception {
                    return response;
                }
            });

            Response response = rp.get();
            result =  response.getResponseBodyAsBytes();
            logger.info("response: " + response);
        } catch (Exception e) {
            logger.error(e.getMessage());
        }finally {
            if (client != null) {
                client.close();
            }
        }
        return result;
    }
    //post请求方法(测试用)
    @Deprecated
    public  String sendPost(String url, String data) {
        String result = null;
        logger.info("url: " + url);
        logger.info("request: " + data);
        try {
            try {
                RequestBuilder mRequestBuilder = new RequestBuilder();
                mRequestBuilder.setUrl(url);
                mRequestBuilder.setMethod("GET");
                mRequestBuilder.addHeader("Content-Type","application/json;charset=utf-8");
                mRequestBuilder.addHeader("Accept","text/json");
               // mRequestBuilder.addHeader("Charset","UTF-8");
                mRequestBuilder.setBody(data);
                Request request = mRequestBuilder.build();
                ListenableFuture<Response> rp = this.client.executeRequest(request,new AsyncCompletionHandler(){
                    public Object onCompleted(Response response) throws Exception {
                        return response;
                    }
                });
                Response response = rp.get();
                result =  response.getResponseBody();
                logger.info("response: " + response);
            } finally {
                if (client != null) {
                    client.close();
                }
            }
        } catch (Exception e) {
            e.printStackTrace();
        }
        return result;
    }
}
