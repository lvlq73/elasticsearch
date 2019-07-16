package elasticsearch;

import org.elasticsearch.action.admin.indices.create.CreateIndexRequest;
import org.elasticsearch.action.admin.indices.delete.DeleteIndexRequest;
import org.elasticsearch.action.admin.indices.mapping.put.PutMappingRequest;
import org.elasticsearch.action.delete.DeleteResponse;
import org.elasticsearch.action.get.GetResponse;
import org.elasticsearch.action.index.IndexResponse;
import org.elasticsearch.action.search.SearchRequestBuilder;
import org.elasticsearch.action.search.SearchResponse;
import org.elasticsearch.client.Requests;
import org.elasticsearch.client.transport.TransportClient;
import org.elasticsearch.common.text.Text;
import org.elasticsearch.common.xcontent.XContentBuilder;
import org.elasticsearch.common.xcontent.XContentFactory;
import org.elasticsearch.common.xcontent.XContentType;
import org.elasticsearch.index.query.QueryBuilders;
import org.elasticsearch.search.SearchHit;
import org.elasticsearch.search.SearchHits;
import org.elasticsearch.search.fetch.subphase.highlight.HighlightBuilder;
import org.elasticsearch.search.fetch.subphase.highlight.HighlightField;
import org.elasticsearch.search.sort.SortOrder;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.IOException;
import java.util.*;

/**
 * elasticsearch工具类
 */
public class ElasticsearchExecute {
    private static Logger logger = LoggerFactory.getLogger(ElasticsearchExecute.class);

    private TransportClient client;

    public ElasticsearchExecute(String host,int port){
        client = ElasticsearchConfig.getCient(host,port);
    }

    public ElasticsearchExecute(List<String> hosts,int port,String CLUSTER_NAME){
        client = ElasticsearchConfig.getCient(hosts,port,CLUSTER_NAME);
    }
    /**
     * 创建索引
     * @param index 索引名称
     */
    public void createIndex(String index){
        //构建一个Index（索引）
        CreateIndexRequest request = new CreateIndexRequest(index);
        client.admin().indices().create(request);
    }
    /**
     * 创建mapping
     * @param index 索引名称
     * @param type 类型分类
     * @param mappingBuilder mapping可指定字段类型检索的类型
     */
    public void createMapping(String index,String type,XContentBuilder mappingBuilder){
        PutMappingRequest mapping = Requests.putMappingRequest(index).type(type).source(mappingBuilder);
        client.admin().indices().putMapping(mapping).actionGet();
    }
    /**
     * 创建索引和插入数据
     * @param index 索引名称
     * @param type 类型
     * @param jsonStrings json字符串数据集合（List集合）
     */
    public void createIndexData(String index,String type,List<String> jsonStrings){
        if(!CollectionUtil.isEmpty(jsonStrings)){
            for(String jsonString : jsonStrings){
                createIndexData(index,type,jsonString);
            }
        }
    }
    public void createIndexData(String index,String type,String id,List<String> jsonStrings){
        if(!CollectionUtil.isEmpty(jsonStrings)){
            for(String jsonString : jsonStrings){
                createIndexData(index,type,id,jsonString);
            }
        }
    }
    /**
     * 创建索引和插入数据
     * @param index 索引名称
     * @param type 类型
     * @param jsonString json字符串数据
     */
    public void createIndexData(String index,String type,String jsonString){
        createIndexData(index,type,null,jsonString);
    }
    /**
     * 创建索引和插入数据
     * @param index 索引名称
     * @param type 类型
     * @param id  数据唯一的id(没写会自动生成)
     * @param jsonString json字符串数据
     */
    public void createIndexData(String index,String type,String id,String jsonString){
        IndexResponse response=client.prepareIndex(index, type,id)
                .setSource(jsonString, XContentType.JSON).get();
        logger.debug("索引名称："+response.getIndex());
        logger.debug("类型："+response.getType());
        logger.debug("ID："+response.getId());
        logger.debug("当前实例状态："+response.status());
    }

    /**
     * 获取相应索引中的相应id的数据
     * @param index 索引名称
     * @param type 类型
     * @param id  数据唯一的id
     * @return
     */
    public Map<String,Object> getDataByIndex(String index, String type, String id){
        try{
            GetResponse response=client.prepareGet(index, type, id).get();
            logger.debug(response.getSourceAsString());
            return response.getSource();
        }catch (Exception e){
            logger.error("获取失败");
        }
        return null;
    }
    /**
     * 获取相应索引中的相应ids集合的数据
     * @param index 索引名称
     * @param type 类型
     * @param ids 数据唯一的id 集合
     * @return
     */
    public List<Map<String,Object>> getDataListByIndex(String index, String type, List<String> ids){
        List<Map<String,Object>> result = new ArrayList<Map<String, Object>>();
        try{
            if(!CollectionUtil.isEmpty(ids)){
                for(String id : ids){
                    GetResponse response=client.prepareGet(index, type, id).get();
                    logger.debug(response.getSourceAsString());
                    result.add( response.getSource());
                }
            }
            return result;
        }catch (Exception e){
            logger.error("获取失败");
        }
        return result;
    }
    /**
     * 删除索引
     * @param index 索引名称
     */
    public void deleteIndex(String index){
        DeleteIndexRequest request = Requests.deleteIndexRequest(index);
        client.admin().indices().delete(request);
        logger.debug("删除成功");
    }

    /**
     * 删除索引指定id的数据
     * @param index 索引名称
     * @param type 类型
     * @param id  数据唯一的id
     */
    public void deleteIndexData(String index,String type,String id){
        DeleteResponse response=client.prepareDelete(index, type, id).get();
        logger.debug("删除成功");
    }
    /**
     * 删除索引指定多个id的数据
     * @param index 索引名称
     * @param type 类型
     * @param ids id集合
     */
    public void deleteIndexData(String index,String type,List<String> ids){
        if(!CollectionUtil.isEmpty(ids)){
            for(String id : ids){
                client.prepareDelete(index, type, id).get();
            }
        }
        logger.debug("删除成功");
    }

    /**
     * 查询
     * @param index 索引名称
     * @param type 类型
     * @param searchField 查询返回的字段（string[]）
     * @return
     */
    public Map<String,Object> search(String index,String type,String[] searchField){
        return search(index,type,0,9999,searchField);
    }
    public Map<String,Object> search(String index,String type,String[] searchField,String[] filterField,Object content){
        return search(index,type,0,9999,searchField,filterField,content,"","asc");
    }
    /**
     * 查询
     * @param index 索引名称
     * @param type 类型
     * @param page 第几页
     * @param limit 一页显示几行
     * @param searchField 查询返回显示的字段（string[]）
     * @return
     */
    public Map<String,Object> search(String index,String type,Integer page,Integer limit,String[] searchField){
        return search(index,type,page,limit,searchField,null);
    }

    /**
     * 查询
     * @param index 索引名称
     * @param type 类型
     * @param page 第几页
     * @param limit 一页显示几行
     * @param searchField 查询返回显示的字段（string[]）
     * @param sortField 需要排序的字段（暂时只支持一个，多个没研究出来）
     * @return
     */
    public Map<String,Object> search(String index,String type,Integer page,Integer limit,String[] searchField,String sortField){
        return search(index,type,page,limit,searchField,null,null,sortField, "asc");
    }
    /**
     * 查询
     * @param index 索引名称
     * @param type 类型
     * @param page 第几页
     * @param limit 一页显示几行
     * @param searchField 查询返回显示的字段（string[]）
     * @param filterField 需要查询过滤的字段（string[]）
     * @param content 查询的内容
     * @param sortField 需要排序的字段（暂时只支持一个，多个没研究出来）
     * @return
     */
    public Map<String,Object> search(String index,String type,Integer page,Integer limit,String[] searchField,String[] filterField,Object content,String sortField){
        return search(index,type,page,limit,searchField,filterField,content,sortField, "asc");
    }
    /**
     * 查询
     * @param index 索引名称
     * @param type 类型
     * @param page 第几页
     * @param limit 一页显示几行
     * @param searchField 查询返回显示的字段（string[]）
     * @param filterField 需要查询过滤的字段（string[]）
     * @param content 查询的内容
     * @param sortField 需要排序的字段（暂时只支持一个，多个没研究出来）
     * @param sortOrder 排序正序或逆序 ，value=asc或desc
     * @return
     */
    public Map<String,Object> search(String index,String type,Integer page,Integer limit,
                                     String[] searchField,String[] filterField,Object content,String sortField,String sortOrder){
        Integer start = page*limit;
        //设置高亮字段
        HighlightBuilder highlightBuilder=new HighlightBuilder();
        //自定义标签
        highlightBuilder.preTags("<font style='color:red'>");
        highlightBuilder.postTags("</font>");
        highlightBuilder.field("*");//*标识所有
        //查询请求builder
        SearchRequestBuilder srb=client.prepareSearch(index).setTypes(type);
        if(content!=null){
            if(filterField!=null&&filterField.length>0){
                srb.setQuery(QueryBuilders.multiMatchQuery(content,filterField));
            }
        }else{
            srb.setQuery(QueryBuilders.matchAllQuery());
        }
        //查询排序顺序
        if(!StringUtil.isEmpty(sortField)){
            if("asc".equals(sortOrder)){
                srb.addSort(sortField,SortOrder.ASC);
            }else{
                srb.addSort(sortField,SortOrder.DESC);
            }
        }
        SearchResponse sr= srb
                .highlighter(highlightBuilder)
                .setFrom(start)
                .setSize(limit)
                .setFetchSource(searchField, null)
                .execute()
                .actionGet(); // 查询所有

        Map<String,Object> result = new HashMap<String,Object>();
        List<Map<String,Object>> dataList = new ArrayList<Map<String, Object>>();
        //获取数据集合
        SearchHits hits=sr.getHits();
        for(SearchHit hit:hits){
           logger.debug(hit.getSourceAsString());
            Map<String,Object> obj = hit.getSourceAsMap();
            //高亮字段
            Map<String, HighlightField> hf = hit.getHighlightFields();
            if(hf!=null){
                Set<String> keys = hf.keySet();
                for(String key:keys){
                    obj.put(key,hf.get(key).getFragments()[0]);
                }
            }
            dataList.add(obj);
        }
        result.put("totalCount",hits.getTotalHits());
        result.put("resultData",dataList);
        return result;
    }
    /**
     * 构建mapping数据 (测试例子)
     * @return
     */
    @Deprecated
    public XContentBuilder getMapping(){
        XContentBuilder mapping = null;
        try {
            mapping = XContentFactory. jsonBuilder()
                    .startObject()
                    .startObject("properties")
                    .startObject("title")
                    .field("type","text")
                    .field("analyzer","smartcn")
                    .endObject()
                    .startObject("publishDate")
                    .field("type","date")
                    .endObject()
                    .startObject("content")
                    .field("type","text")
                    .field("analyzer","smartcn")
                    .endObject()
                    .startObject("director")
                    .field("type","keyword")
                    .endObject()
                    .startObject("price")
                    .field("type","float")
                    .endObject()
                    .endObject()
                    .endObject();
        } catch (IOException e) {
            e.printStackTrace();
        }
        return mapping;
    }

    /**
     * 关闭客户端传输对象
     */
    public void close(){
        if(client!=null){
            client.close();
        }
    }
}
