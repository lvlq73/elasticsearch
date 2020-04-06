package elasticsearch;

import org.elasticsearch.client.transport.TransportClient;
import org.elasticsearch.common.settings.Settings;
import org.elasticsearch.common.transport.TransportAddress;
import org.elasticsearch.transport.client.PreBuiltTransportClient;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.net.InetAddress;
import java.util.List;
/**
 * elasticsearch初始化配置类
 * lvlianqi
 */
public class ElasticsearchConfig {

    private static Logger logger = LoggerFactory.getLogger(ElasticsearchConfig.class);

    private static String host="192.168.2.100"; // 服务器地址
    private static List<String> hosts;//集群多服务器地址
    private static int port=9300; // 端口
    private static String CLUSTER_NAME; // 集群名称
    private static TransportClient client=null; //客户端传输

    /**
     * 连接
     * @param host 服务器地址
     * @param port 端口
     */
    public static TransportClient getCient(String host,int port){
        ElasticsearchConfig.hosts = hosts;
        ElasticsearchConfig.port = port;
        return getCient();
    }

    /**
     *  集合连接
     * @param hosts 服务器地址集合
     * @param port 端口
     * @param CLUSTER_NAME 集群名称
     */
    public static TransportClient getCient(List<String> hosts,int port,String CLUSTER_NAME){
        ElasticsearchConfig.hosts = hosts;
        ElasticsearchConfig.port = port;
        ElasticsearchConfig.CLUSTER_NAME = CLUSTER_NAME;
        return getCient();
    }

    /**
     * 获取客户端传输对象
     * @return
     * @throws Exception
     */
    public static TransportClient getCient() {
        try{
            if(client==null){
                if(StringUtil.isEmpty(CLUSTER_NAME)){
                    client=new PreBuiltTransportClient(Settings.EMPTY)
                            .addTransportAddress(new TransportAddress(InetAddress.getByName(host),port));
                }else{
                    //配置集群名
                    Settings.Builder settings=Settings.builder().put("cluster.name",CLUSTER_NAME);
                    client=new PreBuiltTransportClient(settings.build());
                    if(!CollectionUtil.isEmpty(hosts)){
                        for(String host :hosts){
                            client .addTransportAddress(new TransportAddress(InetAddress.getByName(host),port));
                        }
                    }
                }
            }
            return client;
        }catch (Exception e){
            logger.error("连接异常");
        }
        return null;
    }

}
