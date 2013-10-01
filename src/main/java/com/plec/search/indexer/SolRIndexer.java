package com.plec.search.indexer;
import info.bliki.wiki.filter.PlainTextConverter;
import info.bliki.wiki.model.WikiModel;

import java.io.IOException;
import java.util.UUID;

import org.apache.solr.client.solrj.SolrServer;
import org.apache.solr.client.solrj.SolrServerException;
import org.apache.solr.client.solrj.impl.HttpSolrServer;
import org.apache.solr.common.SolrInputDocument;

import com.plec.search.wikipedia.WikipediaArticle;

public class SolRIndexer implements Indexer {

	private static final String SOLR_SERVEUR_URL = "http://localhost:8983/solr/wikipedia";
	private SolrServer solr = null;
	

	public SolRIndexer() {
		solr = new HttpSolrServer(SOLR_SERVEUR_URL);
		// TODO Auto-generated constructor stub
	}

	public void commit() {
		try {
			solr.commit();
		} catch (IOException e) {
			// TODO Auto-generated catch block
			e.printStackTrace();
		} catch (SolrServerException e) {
			// TODO Auto-generated catch block
			e.printStackTrace();
		}

	}

	public void index(WikipediaArticle article) {
		try {

			SolrInputDocument document = new SolrInputDocument();
			document.addField("id",UUID.randomUUID().toString());
			document.addField("title", article.getTitle());
			document.addField("content", article.getContent());
			document.addField("links", article.getUrl());
			solr.add(document);
		} catch (IOException e) {
			// TODO Auto-generated catch block
			e.printStackTrace();
		} catch (SolrServerException e) {
			// TODO Auto-generated catch block
			e.printStackTrace();
		}
	}

	public static void main(String[] args) {
		SolRIndexer indexer = new SolRIndexer();
		WikipediaArticle article = new WikipediaArticle();
		article.setTitle("Test SolrJ wikiepdia title");
		article.setContent("Test SolrJ wikiepdia content");
		indexer.index(article);

	}
}
