package com.plec.search.indexer;

import java.io.File;
import java.util.HashMap;
import java.util.Map;

import org.apache.commons.io.FileUtils;
import org.elasticsearch.action.admin.indices.create.CreateIndexRequestBuilder;
import org.elasticsearch.action.admin.indices.mapping.put.PutMappingRequestBuilder;
import org.elasticsearch.client.Client;
import org.elasticsearch.client.transport.TransportClient;
import org.elasticsearch.common.settings.ImmutableSettings;
import org.elasticsearch.common.transport.InetSocketTransportAddress;
import static org.elasticsearch.common.xcontent.XContentFactory.*;

public class CreateESIndex {
	private static final String ES_HOST = "localhost";
	private static final int ES_PORT = 9300;
	Client clientES = null;

	private void connect() {
		System.out.println("connect !");
		clientES = new TransportClient()
				.addTransportAddress(new InetSocketTransportAddress(ES_HOST,
						ES_PORT));
	}
public static void main(String[] args) {
	CreateESIndex createESIndex = new CreateESIndex();
	try  {
		createESIndex.createSimpleIndex("wikipedia");
	} catch (Exception e) {
		e.printStackTrace();
	}
}

public void createSimpleIndex(String indexName) {
	try {
		if (clientES == null) {
			connect();
		}
	//	CreateIndexRequestBuilder irb = clientES.admin().indices().prepareCreate(indexName);
	//	irb.execute().actionGet();
	
		//Settings analyser 
		clientES.admin().indices().prepareCreate(indexName)
	    .setSettings(ImmutableSettings.settingsBuilder().loadFromSource(jsonBuilder()
	        .startObject()
	            .startObject("analysis")
	                .startObject("filter")
	                	.startObject("elision")
	                		.field("type", "elision")
	                		.field("articles", new String[]{"l", "m", "t", "qu", "n", "s", "j", "d"})
	                	.endObject()
                	.endObject()
	                .startObject("analyzer")
	                    .startObject("custom_french_analyzer")
	                        .field("type", "custom")
	                        .field("tokenizer", "letter")
	                        .field("filter", new String[]{"snowball", "elision", "lowercase", "asciifolding", "french_stem", "stop"})
	                    .endObject()
	                .endObject()
	            .endObject()
	        .endObject().string()))
	    .execute().actionGet();
		
		Map<String, Object> globalMapping = new HashMap<String, Object>();
		Map<String, Object> articleMapping = new HashMap<String, Object>();
		Map<String, Object> articlePropertiesMapping = new HashMap<String, Object>();
		Map<String, Object> articlePropertiesContentMapping = new HashMap<String, Object>();
		Map<String, Object> titlePropertiesContentMapping = new HashMap<String, Object>();
	//	Map<String, Object> sourceMapping = new HashMap<String, Object>();
		titlePropertiesContentMapping.put("type", "string");
		articlePropertiesContentMapping.put("type", "string");
		articlePropertiesContentMapping.put("analyser", "custom_french_analyzer");
		
	//	articlePropertiesContentMapping.put("store", "no");
		articlePropertiesMapping.put("content", articlePropertiesContentMapping);
		articlePropertiesMapping.put("title", titlePropertiesContentMapping);
		articlePropertiesMapping.put("links", titlePropertiesContentMapping);
		articleMapping.put("properties", articlePropertiesMapping);
	//	sourceMapping.put("enable", "false");
	//	articleMapping.put("_source", sourceMapping);
		globalMapping.put("article", articleMapping);
		PutMappingRequestBuilder pmrb = clientES.admin().indices()
				.preparePutMapping(indexName).setType("string").setSource(globalMapping);
		pmrb.execute().actionGet();
	} catch (Exception e) {
		e.printStackTrace();
	}
}
	public void createIndex(String indexName) throws Exception {
		if (clientES == null) {
			connect();
		}
		CreateIndexRequestBuilder irb = clientES.admin().indices()
				.prepareCreate(indexName);
		irb.execute().actionGet();
		
		String indexInfo = FileUtils.readFileToString(new File("d:/dev/work/createESIndex.json"));
		
//		
//		Map<String, Object> globalMapping = new HashMap<String, Object>();
//		Map<String, Object> articleMapping = new HashMap<String, Object>();
//		Map<String, Object> articlePropertiesMapping = new HashMap<String, Object>();
//		Map<String, Object> articlePropertiesContentMapping = new HashMap<String, Object>();
//		// Map<String, Object> sourceMapping = new HashMap<String, Object>();
//		articlePropertiesContentMapping.put("type", "string");
//		articlePropertiesContentMapping.put("analyser", "francais");
//
//		// articlePropertiesContentMapping.put("store", "no");
//		articlePropertiesMapping
//				.put("content", articlePropertiesContentMapping);
//		articleMapping.put("properties", articlePropertiesMapping);
//		// sourceMapping.put("enable", "false");
//		// articleMapping.put("_source", sourceMapping);
//		globalMapping.put("article", articleMapping);
		PutMappingRequestBuilder pmrb = clientES.admin().indices()
				.preparePutMapping(indexName).setType("article")
				.setSource(indexInfo);
		pmrb.execute().actionGet();
	}
}
