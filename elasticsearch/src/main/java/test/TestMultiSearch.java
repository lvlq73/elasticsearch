package test;

import org.elasticsearch.action.search.SearchRequestBuilder;
import org.elasticsearch.action.search.SearchResponse;
import org.elasticsearch.client.transport.TransportClient;
import org.elasticsearch.common.settings.Settings;
import org.elasticsearch.common.transport.TransportAddress;
import org.elasticsearch.index.query.QueryBuilder;
import org.elasticsearch.index.query.QueryBuilders;
import org.elasticsearch.search.SearchHit;
import org.elasticsearch.search.SearchHits;
import org.elasticsearch.transport.client.PreBuiltTransportClient;
import org.junit.After;
import org.junit.Before;
import org.junit.Test;

import java.net.InetAddress;

public class TestMultiSearch {

	private static String host="192.168.2.100"; // 服务器地址
	private static int port=9300; // 端口

	public static final String CLUSTER_NAME="my-application"; // 集群名称

	private static Settings.Builder settings=Settings.builder().put("cluster.name",CLUSTER_NAME);

	private TransportClient client=null;

	@SuppressWarnings({ "resource", "unchecked" })
	@Before
	public void getCient()throws Exception{
		client = new PreBuiltTransportClient(settings.build())
				.addTransportAddress(new TransportAddress(InetAddress.getByName(host),port));
	}

	@After
	public void close(){
		if(client!=null){
			client.close();
		}
	}



	/**
	 * 多条件查询
	 * @throws Exception
	 */
	@Test
	public void searchMulti()throws Exception{
		SearchRequestBuilder srb=client.prepareSearch("film").setTypes("dongzuo");
		QueryBuilder queryBuilder=QueryBuilders.matchPhraseQuery("title", "战");
		QueryBuilder queryBuilder2=QueryBuilders.matchPhraseQuery("content", "星球");
		SearchResponse sr=srb.setQuery(QueryBuilders.boolQuery()
				.must(queryBuilder)
				.must(queryBuilder2))
				.execute()
				.actionGet();
		SearchHits hits=sr.getHits();
		for(SearchHit hit:hits){
			System.out.println(hit.getSourceAsString());
		}
	}

	/**
	 * 多条件查询
	 * @throws Exception
	 */
	@Test
	public void searchMulti2()throws Exception{
		SearchRequestBuilder srb=client.prepareSearch("film").setTypes("dongzuo");
		QueryBuilder queryBuilder=QueryBuilders.matchPhraseQuery("title", "战");
		QueryBuilder queryBuilder2=QueryBuilders.matchPhraseQuery("content", "武士");
		SearchResponse sr=srb.setQuery(QueryBuilders.boolQuery()
				.must(queryBuilder)
				.mustNot(queryBuilder2))
				.execute()
				.actionGet();
		SearchHits hits=sr.getHits();
		for(SearchHit hit:hits){
			System.out.println(hit.getSourceAsString());
		}
	}

	/**
	 * 多条件查询
	 * @throws Exception
	 */
	@Test
	public void searchMulti3()throws Exception{
		SearchRequestBuilder srb=client.prepareSearch("film").setTypes("dongzuo");
		QueryBuilder queryBuilder=QueryBuilders.matchPhraseQuery("title", "战");
		QueryBuilder queryBuilder2=QueryBuilders.matchPhraseQuery("content", "星球");
		QueryBuilder queryBuilder3=QueryBuilders.rangeQuery("publishDate").gte("2018-01-01");
		SearchResponse sr=srb.setQuery(QueryBuilders.boolQuery()
				.must(queryBuilder)
				.should(queryBuilder2)
				.should(queryBuilder3))
				.execute()
				.actionGet();
		SearchHits hits=sr.getHits();
		for(SearchHit hit:hits){
			System.out.println(hit.getScore()+":"+hit.getSourceAsString());
		}
	}

	/**
	 * 多条件查询
	 * @throws Exception
	 */
	@Test
	public void searchMulti4()throws Exception{
		SearchRequestBuilder srb=client.prepareSearch("film").setTypes("dongzuo");
		QueryBuilder queryBuilder=QueryBuilders.matchPhraseQuery("title", "战");
		QueryBuilder queryBuilder2=QueryBuilders.rangeQuery("price").lte(40);
		SearchResponse sr=srb.setQuery(QueryBuilders.boolQuery()
				.must(queryBuilder)
				.filter(queryBuilder2))
				.execute()
				.actionGet();
		SearchHits hits=sr.getHits();
		for(SearchHit hit:hits){
			System.out.println(hit.getSourceAsString());
		}
	}
}
