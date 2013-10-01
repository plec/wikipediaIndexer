package com.plec.search.indexer;

import com.plec.search.wikipedia.WikipediaArticle;

public interface Indexer {
	public void commit();
	public void index(WikipediaArticle article);
}
