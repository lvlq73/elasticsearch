package test;

import com.google.gson.JsonObject;
import elasticsearch.HttpUtil;
import org.elasticsearch.action.ActionFuture;
import org.elasticsearch.action.search.SearchRequest;
import org.elasticsearch.action.search.SearchResponse;
import org.elasticsearch.client.transport.TransportClient;
import org.elasticsearch.common.settings.Settings;
import org.elasticsearch.common.transport.TransportAddress;
import org.elasticsearch.index.query.QueryBuilders;
import org.elasticsearch.search.SearchHit;
import org.elasticsearch.search.SearchHits;
import org.elasticsearch.search.builder.SearchSourceBuilder;
import org.elasticsearch.transport.client.PreBuiltTransportClient;

import java.net.InetAddress;

public class Test {

    public static void main(String[] args) throws Exception{
        //java api方式
        /*ElasticsearchExecute execute = new ElasticsearchExecute(Arrays.asList("192.168.2.100","192.168.2.105"),9300,"my-application");
        String index = "test-1";
        String type="purchase";
        String[] searchField = new String[]{"id","code","vencode","venname"};
        String[] filterField = new String[]{"code","vencode","venname"};
        Map<String,Object> result = execute.search(index,type,searchField,filterField,"安安");
        List<Map<String,Object>> list = (List<Map<String, Object>>) result.get("resultData");
        for(Map<String,Object> obj : list){
            String msg = "";
            for(String key : searchField){
                msg += (key+"="+obj.get(key)+ "；");
            }
            System.out.println(msg);
        }
        execute.close();*/

        //java resful方式 可参考 https://www.elastic.co/guide/en/elasticsearch/client/java-rest/current/java-rest-high-search.html
        Settings.Builder settings=Settings.builder().put("cluster.name","my-application");
        TransportClient client = new PreBuiltTransportClient(settings.build())
                .addTransportAddress(new TransportAddress(InetAddress.getByName("192.168.2.100"),9300))
                .addTransportAddress(new TransportAddress(InetAddress.getByName("192.168.2.105"),9300));
        SearchSourceBuilder searchSourceBuilder = new SearchSourceBuilder();
        //searchSourceBuilder.query(QueryBuilders.matchQuery("venname", "福建新纪元鞋材发展有限公司"));
        searchSourceBuilder.query(QueryBuilders.termQuery("venname", "福建康茂鞋材有限公司"));
        searchSourceBuilder.from(0);
        searchSourceBuilder.size(5);
        SearchRequest searchRequest = new SearchRequest("test-1");
        searchRequest.types("purchase");
        searchRequest.source(searchSourceBuilder);
        ActionFuture<SearchResponse> actionFuture =  client.search(searchRequest);
        SearchHits hits = actionFuture.actionGet().getHits();
        for(SearchHit hit:hits){
            System.out.println(hit.getSourceAsString());
        }
        //http 方式  可参考 https://www.elastic.co/guide/en/elasticsearch/reference/current/search-request-body.html
        /*JsonObject jsonObject2=new JsonObject();
        jsonObject2.addProperty("venname", "福建新纪元鞋材发展有限公司");
        JsonObject jsonObject1=new JsonObject();
        jsonObject1.addProperty("term", jsonObject1.toString());
        JsonObject jsonObject=new JsonObject();
        jsonObject.addProperty("query", jsonObject1.toString());
        String result = HttpUtil.httpResFul("http://192.168.2.100:9200/test-1/purchase/_search/" ,"GET",jsonObject.toString());
        System.out.println(result);*/
    }
}
