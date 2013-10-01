package com.plec.search.indexer;
import java.util.Date;
import java.util.HashMap;
import java.util.Map;

import org.elasticsearch.action.admin.indices.create.CreateIndexRequestBuilder;
import org.elasticsearch.action.admin.indices.mapping.put.PutMappingRequestBuilder;
import org.elasticsearch.action.index.IndexResponse;
import org.elasticsearch.client.Client;
import org.elasticsearch.client.transport.TransportClient;
import org.elasticsearch.common.transport.InetSocketTransportAddress;

import com.plec.search.wikipedia.WikipediaArticle;

public class ESIndexer implements Indexer {
	private static final String ES_HOST = "localhost";
	private static final int ES_PORT = 9300;
	Client clientES = null;

	public ESIndexer() {
	}
	public void commit() {
		// TODO Auto-generated method stub
	}
	private void connect() {
		System.out.println("connect !");
		clientES = new TransportClient()
		.addTransportAddress(new InetSocketTransportAddress(
				ES_HOST, ES_PORT));
	}
	public void disconnect() {
		System.out.println("disconnect");
		clientES.close();
		clientES = null;
	}
	//"wikipedia"
	private void createIndex(String indexName) {
		if (clientES == null) {
			connect();
		}
		CreateIndexRequestBuilder irb = clientES.admin().indices().prepareCreate(indexName);
		irb.execute().actionGet();
		Map<String, Object> globalMapping = new HashMap<String, Object>();
		Map<String, Object> articleMapping = new HashMap<String, Object>();
		Map<String, Object> articlePropertiesMapping = new HashMap<String, Object>();
		Map<String, Object> articlePropertiesContentMapping = new HashMap<String, Object>();
//		Map<String, Object> sourceMapping = new HashMap<String, Object>();
		articlePropertiesContentMapping.put("type", "string");
		articlePropertiesContentMapping.put("analyser", "francais");
		
//		articlePropertiesContentMapping.put("store", "no");
		articlePropertiesMapping.put("content", articlePropertiesContentMapping);
		articleMapping.put("properties", articlePropertiesMapping);
//		sourceMapping.put("enable", "false");
//		articleMapping.put("_source", sourceMapping);
		globalMapping.put("article", articleMapping);
		PutMappingRequestBuilder pmrb = clientES.admin().indices()
				.preparePutMapping(indexName).setType("string").setSource(globalMapping);
		pmrb.execute().actionGet();
		
	}

	public void index(WikipediaArticle article) {
		if (clientES == null) {
			connect();
		}
		Map<String, Object> json = new HashMap<String, Object>();
//		if (!withSource) {
//			Map<String, Object> disableSource = new HashMap<String, Object>();
//			disableSource.put("enabled", false);
//			json.put("_source",disableSource);
//		}
		json.put("user","plec");
		json.put("postDate",new Date());
		json.put("content",article.getContent());
		json.put("title",article.getTitle());
		json.put("links",article.getUrl());
		IndexResponse response = clientES.prepareIndex("wikipedia", "article").setSource(json).execute().actionGet();
//		// Index name
//		String _index = response.getIndex();
//		// Type name
//		String _type = response.getType();
//		// Document ID (generated or not)
//		String _id = response.getId();
//		// Version (if it's the first time you index this document, you will get: 1)
//		long _version = response.getVersion();
//		System.out.println( "Index response : " + _index +" "+ _type + " " +_id + " " + _version);
		
	}
//	public static void main(String[] args) {
//		ESIndexer esIndexer = new ESIndexer();
//		esIndexer.index("trying out Elastic Search");
//		esIndexer.index("article 2 qui parle d'un super truc de fou");
//		esIndexer.index("lorem ipsum");
//		esIndexer.disconnect();
//	}

}
