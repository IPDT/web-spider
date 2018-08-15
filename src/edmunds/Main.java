package edmunds;

import java.net.InetSocketAddress;
import java.net.Proxy;
import java.util.ArrayList;
import java.util.List;


import org.jsoup.Jsoup;
import org.jsoup.nodes.Document;
import org.jsoup.nodes.Element;
import org.jsoup.select.Elements;

import com.util.FileUtil;
import com.util.HttpUtil;
import com.util.MongoDB;

public class Main implements Runnable {
	static String host = "10.58.0.189";
	static int port = 27017;
	
	static String dbName = "edmunds";
	
	static String baseUrl = "https://www.edmunds.com";
	static String metaDatUrl = "https://www.edmunds.com/car-reviews/car-reviews-road-tests.html?sort=date_desc&itemsPerPage=0";
	
	private MongoDB mongoDB = new MongoDB(host, port, dbName);
	private int start;
	private int end;
	private String[] urls;
	private int threadNo;
	private Proxy proxy;
	
	private String failUrls;
	public Main(int threadNo, int start, int end, String[] urls, String ip, int port) {
		this.threadNo = threadNo;
		this.start = start;
		this.end = end < urls.length ? end : urls.length;
		this.urls = urls;
		
		this.failUrls = threadNo + "_fails";
		this.proxy = new Proxy(Proxy.Type.HTTP,  new InetSocketAddress(ip, port));
	}
	
	public void readMetaDataFromUrl() {
		String html = HttpUtil.sendGet(metaDatUrl, proxy);
		Document doc = Jsoup.parse(html);
		Elements aList = doc.select(".info1 a");
		
		List<org.bson.Document> documents = new ArrayList<>();
		for(Element element: aList) {
			String rowUrl = baseUrl + element.attr("href");
			FileUtil.writeFile("metaData", rowUrl + "\n", true);
			org.bson.Document document = new org.bson.Document();
			document.append("url",baseUrl + element.attr("href"));
			documents.add(document);
		}
		mongoDB.insertMany(documents, "metaData");
		
		
	}
	
	public static String[] readMetaDtaFromLocalFile(String fileName) {
		String[] urls = FileUtil.readFile(fileName).split("\n");
		return urls;
	}
	
	public org.bson.Document getPageInfo(String url) {
		org.bson.Document pageData = new org.bson.Document();
		pageData.append("url", url);
		String html = HttpUtil.sendGet(url, proxy);
		if (html == null) {
			return null;
		}
		Document doc = Jsoup.parse(html);
		Elements h1 = doc.select("h1");
		if (h1.size() != 1) {
			return null;
		}
		pageData.append("title", h1.text());
		
		Elements info = doc.select(".editorial-review-full-container>.container .offset-md-0>.mb-1 p");
		pageData.append("info", info.text());
		
		
		org.bson.Document document = new org.bson.Document();
		int count = 0;
		Elements pageContainers = doc.select(".editorial-review-full-container>.container>.row>div");
		for(Element container: pageContainers) {
			Element h2 = container.selectFirst("div>h2");
			Element h2Content = container.selectFirst("div.mb-1");
			if (h2 != null && h2Content != null) {
				count++;
				String key = h2.text();
				String value = h2Content.text();
							
				Element h2ContentMore = container.selectFirst("div.content-collapse");
				
				org.bson.Document documentMore = new org.bson.Document();
				if (h2ContentMore!= null) {
					Elements h2ContentMoreElements = h2ContentMore.select("div.collapse>div.row>div.container");
					for(Element h2ContentMoreElement : h2ContentMoreElements ) {
						Elements h2ContentMoreElementH2 = h2ContentMoreElement.select("h2");
						Elements h2ContentMoreElementContent = h2ContentMoreElement.select("div.mb-1");
						if (h2ContentMoreElementH2.text().equals("")) {
							value += " " + h2ContentMoreElementContent.text();
						} else {
							documentMore.append(h2ContentMoreElementH2.text(), h2ContentMoreElementContent.text());
						}
					}
					
					
				}
				documentMore.append("description", value);
				document.append(key, documentMore);
			}

		}
		if (count == 0) {
			return null;
		}
		if (document.size() == 0) {
			return null;
		}
		pageData.append("review", document);
		return pageData;
		
	}
	
	public static void main(String[] args) {
//		readMetaData(); //exec onece

		String[] urls = readMetaDtaFromLocalFile("metaData");
		int batch = 9000;
		int threadNums = urls.length/batch + 1;
		String[] ips = {"proxy.sha.sap.corp"};
		int[] port = {8080};
		
		for(int i = 0 ;i < threadNums ;i++) {
			Main main = new Main(i, i * batch, (i+1)*batch, urls,ips[i],port[i] );
			new Thread(main).start();
		}
		
	}
	@Override
	public void run() {
		System.out.println("Thread:" + threadNo + ",start:" + start + ",end:" + end);
		
		mongoDB.con();


		for(int i = start; i < end; i++) {
			String url = urls[i];
			try {
				org.bson.Document document = getPageInfo(url);
				if (document == null) {
					FileUtil.writeFile(failUrls, url + "\n", true);
				} else {
					mongoDB.insertOne(document, "data");
				}
			} catch (Exception e) {
				FileUtil.writeFile(failUrls, url + "\n", true);
			}
			if (i%20 == 0) {
				System.out.println("Thread:" + threadNo + ",Current:" + i);
			}

		}
		mongoDB.close();
		
	}

}
