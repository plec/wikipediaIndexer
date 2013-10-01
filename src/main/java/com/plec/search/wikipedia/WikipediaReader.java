package com.plec.search.wikipedia;
import java.io.File;
import java.io.FileInputStream;
import java.io.FileReader;
import java.io.IOException;
import java.util.Collection;

import org.apache.commons.io.FileUtils;
import org.xml.sax.InputSource;
import org.xml.sax.SAXException;
import org.xml.sax.XMLReader;
import org.xml.sax.helpers.XMLReaderFactory;

public class WikipediaReader {
	private static final String FILE_PATH = "d:/dev/work/frwiki-20130903-pages-meta-current1.xml";
	private static final String DIR_PATH = "d:/dev/work/";
	private static final String FILE_EXTENTION = "xml";

	public WikipediaReader() {
		// TODO Auto-generated constructor stub
	}

	public void read() throws IOException, SAXException {
		XMLReader xr = XMLReaderFactory.createXMLReader();
		WikipediaHandler handler = new WikipediaHandler();
		xr.setContentHandler(handler);
		xr.setErrorHandler(handler);
		Collection<File> filesToProcess = FileUtils.listFiles(new File(DIR_PATH), new String[]{FILE_EXTENTION}, false);
		int nbFiles = filesToProcess.size();
		int currentFile=1;
		for (File fileToProcess : filesToProcess) {
			System.out.println("Processing file "+currentFile + "/" + nbFiles + " file Name " + fileToProcess.getName() + " file size " + (fileToProcess.length()/1024/1024) +"Mo");
			long startTime = System.currentTimeMillis();
			FileReader r = new FileReader(fileToProcess);
			xr.parse(new InputSource(r));
			long endTime = System.currentTimeMillis();
			long fileProcessTime = endTime - startTime;
			System.out.println("Processing file "+currentFile + "/" + nbFiles + " in " + fileProcessTime + "ms");
			currentFile++;
		}
	}
	public static void main(String[] args) {
		WikipediaReader reader = new WikipediaReader();
		try {
			long start = System.currentTimeMillis();
			reader.read();
			long end = System.currentTimeMillis();
			System.out.println("TOTAL TIME : " + (end - start) +"ms");
		} catch (Exception e) {
			// TODO Auto-generated catch block
			e.printStackTrace();
		}
	}
	public void readTest() {
		try {
			FileInputStream fis = new FileInputStream(new File(FILE_PATH));
			byte[] buffer = new byte[1024];
			for (int i = 0; i < 1000; i++) {
				fis.read(buffer);
				System.out.println(new String(buffer));
			}
		} catch (Exception e) {
			e.printStackTrace();
		}
	}
	
}
