package com.plec.search.wikipedia;

import info.bliki.wiki.filter.PlainTextConverter;
import info.bliki.wiki.model.WikiModel;

import java.util.ArrayList;
import java.util.List;

import org.xml.sax.Attributes;
import org.xml.sax.SAXException;
import org.xml.sax.helpers.DefaultHandler;

import com.plec.search.indexer.ESIndexer;
import com.plec.search.indexer.SolRIndexer;

public class WikipediaHandler extends DefaultHandler {
	//variable permettant de definir si on index dans solr ou dans ES - a modifier en fonction des besoins
	private static final boolean INDEX_SOLR = false;
	private static final boolean INDEX_ES = true;

	
	private boolean page = false;
	private boolean title = false;
	private boolean text = false;
	private boolean id = false;
	private boolean idDone = false;
	private StringBuilder currentString = new StringBuilder();
	private List<WikipediaArticle> articles = new ArrayList<WikipediaArticle>();
	private static final String PREFIX_URL_ARTICLE_WIKIPEDIA= "http://fr.wikipedia.org/wiki?curid=";
	private WikipediaArticle currentArticle = null;
	private static final int ARTICLE_LIST = 20000;
	private ESIndexer esIndexer = new ESIndexer();
	private SolRIndexer solrIndexer = new SolRIndexer();
	private int nbArticlesTotal = 0;
	private long startTime;
	private long endTime;
	private WikiModel wikiModel = new WikiModel("http://commons.wikimedia.org/wiki/${image}", "http://fr.wikipedia.org/${title}");
	@Override
	public void startDocument() throws SAXException {
		startTime = System.currentTimeMillis();
		//if (INDEX_ES) esIndexer.createIndex("wikipedia");
		super.startDocument();
	}
@Override
public void endDocument() throws SAXException {
	// TODO Auto-generated method stub
	super.endDocument();
	int articleSize = articles.size();
	if (articleSize > 0) {
		long time1 = System.currentTimeMillis();
		for (WikipediaArticle article : articles) {
			index(article, false);
		}
		if(INDEX_SOLR) solrIndexer.commit();
		long time2 = System.currentTimeMillis();
		System.out.println("Indexation de "+articleSize+" articles en " + (time2 - time1));
		articles.clear();
	}
	if (INDEX_ES) esIndexer.disconnect();
	endTime= System.currentTimeMillis();
	System.out.println("Indexation totale terminée pour " + nbArticlesTotal + " articles en " + (endTime - startTime) + " ms");
	
}
private void index(WikipediaArticle article, boolean withsource) {
	if (INDEX_ES) {
		esIndexer.index(article);
	}
	if (INDEX_SOLR) {
		solrIndexer.index(article);
	}

}
	@Override
	public void startElement(String uri, String localName, String qName,
			Attributes attributes) throws SAXException {
		super.startElement(uri, localName, qName, attributes);
		if (localName.equals("page")) {
			idDone = false;
			page = true;
			if (currentArticle != null) {
				articles.add(currentArticle);
				nbArticlesTotal++;
			}
			if (articles.size() > ARTICLE_LIST) {
				long time1 = System.currentTimeMillis();
				for (WikipediaArticle article : articles) {
					index(article, false);
				}
				if(INDEX_SOLR) solrIndexer.commit();
				long time2 = System.currentTimeMillis();
				System.out.println("Indexation de "+ARTICLE_LIST+" articles en " + (time2 - time1));
				articles.clear();
			}
			currentArticle = new WikipediaArticle();
			
		} else if (page && localName.equals("title")) {
			title = true;
			 currentString = new StringBuilder();
		} else if (page && localName.equals("text")) {
			text = true;
			 currentString = new StringBuilder();
		} else if (page && !idDone && localName.equals("id")) {
			id = true;
			currentString = new StringBuilder();
		}
	}

	@Override
	public void endElement(String uri, String localName, String qName)
			throws SAXException {
		super.endElement(uri, localName, qName);
		if (localName.equals("page")) {
			page = false;
			title = false;
			text = false;
		} else if (page && localName.equals("title")) {
			title = false;
			currentArticle.setTitle(currentString.toString());
		} else if (page && localName.equals("text")) {
			String content = currentString.toString();
			String plainTextContent = wikiModel.render(new PlainTextConverter(true), content);
			currentArticle.setContent(plainTextContent);
			text = false;
		} else if (page && localName.equals("id")) {
			id = false;
			idDone = true;
			currentArticle.setUrl(PREFIX_URL_ARTICLE_WIKIPEDIA+currentString.toString());
		}
	}

	@Override
	public void characters(char[] ch, int start, int length)
			throws SAXException {
		super.characters(ch, start, length);
		if (title || text || id) {
			currentString.append(getCharactersContent(ch, start, length));
		}
	}
	

	private String getCharactersContent(char[] ch, int start, int length) {
		StringBuilder builder = new StringBuilder();
		for (int i = start; i < start + length; i++) {
			builder.append(ch[i]);
		}
		return builder.toString();
	}
}