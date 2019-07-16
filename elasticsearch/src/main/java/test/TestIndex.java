package test;

import com.google.gson.JsonObject;
import elasticsearch.ElasticsearchExecute;
import org.elasticsearch.ElasticsearchException;
import org.elasticsearch.action.admin.indices.create.CreateIndexResponse;
import org.elasticsearch.action.admin.indices.delete.DeleteIndexRequest;
import org.elasticsearch.action.delete.DeleteResponse;
import org.elasticsearch.action.get.GetResponse;
import org.elasticsearch.action.index.IndexResponse;
import org.elasticsearch.action.update.UpdateResponse;
import org.elasticsearch.client.Requests;
import org.elasticsearch.client.transport.TransportClient;
import org.elasticsearch.common.settings.Settings;
import org.elasticsearch.common.transport.TransportAddress;
import org.elasticsearch.common.xcontent.XContentType;
import org.elasticsearch.transport.client.PreBuiltTransportClient;
import org.junit.After;
import org.junit.Before;
import org.junit.Test;

import java.net.InetAddress;

public class TestIndex {

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
	 * 创建索引 添加文档
	 * @throws Exception
	 */
	@Test
	public void testIndex()throws Exception{
		JsonObject jsonObject=new JsonObject();
		jsonObject.addProperty("name", "java编程思想");
		jsonObject.addProperty("publishDate", "2012-11-11");
		jsonObject.addProperty("price", 100);
		IndexResponse response=client.prepareIndex("book", "java", "1")
				.setSource(jsonObject.toString(), XContentType.JSON).get();
		System.out.println("索引名称："+response.getIndex());
		System.out.println("类型："+response.getType());
		System.out.println("文档ID："+response.getId());
		System.out.println("当前实例状态："+response.status());
	}

	/**
	 * 根据id获取文档
	 * @throws Exception
	 */
	@Test
	public void testGet()throws Exception{
		GetResponse response=client.prepareGet("book", "java", "1").get();
		System.out.println(response.getSourceAsString());
	}

	/**
	 * 根据id修改文档
	 * @throws Exception
	 */
	@Test
	public void testUpdate()throws Exception{
		JsonObject jsonObject=new JsonObject();
		jsonObject.addProperty("name", "java编程思想2");
		jsonObject.addProperty("publishDate", "2012-11-12");
		jsonObject.addProperty("price", 102);

		UpdateResponse response=client.prepareUpdate("book", "java", "1").setDoc(jsonObject.toString(), XContentType.JSON).get();
		System.out.println("索引名称："+response.getIndex());
		System.out.println("类型："+response.getType());
		System.out.println("文档ID："+response.getId());
		System.out.println("当前实例状态："+response.status());
	}

	/**
	 * 根据id删除文档
	 * @throws Exception
	 */
	@Test
	public void testDelete()throws Exception{
		DeleteResponse response=client.prepareDelete("book", "java", "1").get();
		System.out.println("索引名称："+response.getIndex());
		System.out.println("类型："+response.getType());
		System.out.println("文档ID："+response.getId());
		System.out.println("当前实例状态："+response.status());
	}

	/**
	 * 删除索引
	 * @throws Exception
	 */
	@Test
	public void testIndexDelete()throws Exception{
		DeleteIndexRequest request = Requests.deleteIndexRequest("film");
		client.admin().indices().delete(request);
	}
}
