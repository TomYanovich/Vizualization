package main;

import java.io.BufferedWriter;
import java.io.File;
import java.io.FileWriter;
import java.io.IOException;
import java.net.URISyntaxException;
import java.net.UnknownHostException;
import java.text.ParseException;
import java.text.SimpleDateFormat;
import java.util.ArrayList;
import java.util.Calendar;
import java.util.Collection;
import java.util.Collections;
import java.util.Comparator;
import java.util.HashMap;
import java.util.HashSet;
import java.util.Iterator;
import java.util.LinkedList;
import java.util.List;
import java.util.Map;
import java.util.Map.Entry;
import java.util.SortedMap;
import java.util.SortedSet;
import java.util.TreeMap;
import java.util.TreeSet;
import java.util.concurrent.TimeUnit;

import org.apache.commons.lang3.StringUtils;
import org.bson.types.ObjectId;

import com.mongodb.BasicDBList;
import com.mongodb.BasicDBObject;
import com.mongodb.DBCollection;
import com.mongodb.DBCursor;
import com.mongodb.DBObject;

public class WordMatrix {

	private static final int NUM_OF_KEYWORDS = 40;
	/*
	 * rows = dates, columns = words, values = frequencies
	 */
	private int[][] wordMatrix;
	/*
	 * index = word, value = frequency
	 */
	private int[] wordFrequencies;

	private Calendar start_date;

	private Calendar end_date;

	private ArrayList<String> keywords;

	private String name;

	private static final SimpleDateFormat dateFormatter = new SimpleDateFormat("yyyyMMdd");

	private static HashMap<String, HashSet<String>> event_keywords = new HashMap<>();

	private static HashMap<String, String> domains = new HashMap<>();

	private static String country;

	private static boolean foreignOrDomestic;

	/**
	 * words total number of words document = total number of documents
	 * 
	 * @param words
	 * @param documents
	 */
	public WordMatrix(Calendar start, Calendar end, Collection<String> words) {
		super();
		start_date = start;
		end_date = end;
		this.keywords = new ArrayList<String>(words);
		Collections.sort(keywords);
		this.wordMatrix = new int[Utils.getDateDiff(start, end, TimeUnit.DAYS) + 1][keywords.size()];
		this.wordFrequencies = new int[keywords.size()];

		this.name = dateFormatter.format(start_date.getTime()) + "-" + dateFormatter.format(end_date.getTime());
	}

	public int[][] getWordMatrix() {
		return wordMatrix;
	}

	public synchronized boolean insert(Calendar date, String keyword, int f) {
		if (date.before(start_date) || date.after(end_date) || f <= 0) {
			return false;
		}

		int wordIndex = keywords.indexOf(keyword);
		wordMatrix[getIndexOfDate(date)][wordIndex] += f;
		wordFrequencies[wordIndex] += f;
		return true;
	}

	public int getf(Calendar date, String keyword) {
		return wordMatrix[getIndexOfDate(date)][keywords.indexOf(keyword)];
	}

	public int getf(int row, String keyword) {
		return wordMatrix[row][keywords.indexOf(keyword)];
	}

	public int getWordFrequency(String keyword) {
		return wordFrequencies[keywords.indexOf(keyword)];
	}

	private int getIndexOfDate(Calendar date) {
		if (date.before(start_date)) {
			return -1;
		}
		return Utils.getDateDiff(start_date, date, TimeUnit.DAYS);
	}

	private TreeSet<String> sortByFrequency() {
		TreeSet<String> sortedKeywords = new TreeSet<>(new Comparator<String>() {

			@Override
			public int compare(String k1, String k2) {
				int freq_1 = wordFrequencies[keywords.indexOf(k1)];
				int freq_2 = wordFrequencies[keywords.indexOf(k2)];

				if (freq_1 == freq_2) {
					return k1.compareTo(k2);
				}
				return freq_2 - freq_1;
			}
		});
		sortedKeywords.addAll(keywords);
		return sortedKeywords;
	}

	public List<String> getKeywords() {
		return keywords;
	}

	private String getDateOfRow(int row, SimpleDateFormat formatter) {
		Calendar dateOfRow = Calendar.getInstance();
		dateOfRow.setTimeInMillis(start_date.getTimeInMillis());
		dateOfRow.add(Calendar.DATE, row);
		return formatter.format(dateOfRow.getTime());
	}

	public static List<String> getKlongestEpisodes(int k) {

		SortedMap<String, Integer> episodesMap = new TreeMap<String, Integer>();
		SortedSet<Map.Entry<String, Integer>> sortedEpisodes = new TreeSet<Map.Entry<String, Integer>>(
				new Comparator<Map.Entry<String, Integer>>() {
					@Override
					public int compare(Map.Entry<String, Integer> e1, Map.Entry<String, Integer> e2) {
						return e2.getValue().compareTo(e1.getValue());
					}
				});

		DBCollection coll = MyDB.getDB().getCollection("newsItems_Episodes");
		DBCursor cursor = coll.find();

		while (cursor.hasNext()) {
			cursor.next();
			String _id = "";
			try {
				_id = cursor.curr().get("_id").toString();
				Calendar start_date = Utils.parseDateISO8601((cursor.curr().get("rssUpdFirst")).toString());
				Calendar end_date = Utils.parseDateISO8601((cursor.curr().get("rssUpdLast")).toString());
				int duration = Utils.getDateDiff(start_date, end_date, TimeUnit.DAYS);
				episodesMap.put(_id, duration);
			} catch (Exception e) {
				e.printStackTrace();
				continue;
			}

		}
		sortedEpisodes.addAll(episodesMap.entrySet());
		List<String> longestEpisodes = new LinkedList<>();
		Iterator<Entry<String, Integer>> it = sortedEpisodes.iterator();
		for (int i = 0; i < k; i++) {
			longestEpisodes.add(it.next().getKey());
		}
		return longestEpisodes;
	}

	public static WordMatrix init(String _country, boolean _foreignOrDomestic) {
		WordMatrix.country = _country;
		WordMatrix.foreignOrDomestic = _foreignOrDomestic;

		DBCollection coll = MyDB.getDB().getCollection("newsItems_Events");
		BasicDBObject query = new BasicDBObject();
		query.append("nlp:entity.LOCATION", country);
		DBCursor cursor = coll.find(query);

		long results = coll.count(query);
		int old_perc = 0;

		HashSet<String> keywordSet = new HashSet<>();
		int i = 0;
		long time = 0;
		Calendar minDate = Calendar.getInstance();
		Calendar maxDate = Calendar.getInstance();
		boolean firstIteration = true;
		System.out.println("init");

		try {
			while (cursor.hasNext()) {
				cursor.next();
				long t1 = System.currentTimeMillis();
				BasicDBObject doc = (BasicDBObject) cursor.curr();

				int new_perc = (int) (++i * 100 / (double) results);
				if (new_perc > old_perc) {
					old_perc = new_perc;
					System.out.println(old_perc + "%. elapsed time: " + time / 1000.0 + " sec.");
				}
				String _id = doc.get("_id").toString();
				String domain_name = "";
				
				try {
					String url = ((BasicDBObject) doc.get("link")).get("href").toString();
					domain_name = Utils.getDomainName(url);

					if (!domains.containsKey(domain_name)) {

						String ip = java.net.InetAddress.getByName("www." + domain_name).getHostAddress();
						String domain_country = Utils.getLookupService().getLocation(ip).countryName;
						domains.put(domain_name, domain_country);
					}
					
					if (domains.get(domain_name).equals(country)) {
						// the web-site is domestic
						if (foreignOrDomestic == Utils.FOREIGN) {
							continue;
						}

					} else {
						// the web-site is foreign
						if (foreignOrDomestic == Utils.DOMESTIC) {
							continue;
						}

						boolean mentionedCountry = ((BasicDBList) ((BasicDBObject) doc.get("nlp:entity"))
								.get("LOCATION")).contains(country);
						if (!mentionedCountry) {
							continue;
						}
					}

					Calendar publishDate = Utils.parseDateISO8601((doc.get("published")).toString());

					if (firstIteration) {
						minDate.setTimeInMillis(publishDate.getTimeInMillis());
						maxDate.setTimeInMillis(publishDate.getTimeInMillis());
						firstIteration = false;
					} else {
						if (publishDate.before(minDate)) {
							minDate.setTimeInMillis(publishDate.getTimeInMillis());
						}
						if (publishDate.after(maxDate)) {
							maxDate.setTimeInMillis(publishDate.getTimeInMillis());
						}
					}
					String str = ((BasicDBObject) doc.get("keywords")).get("terms").toString();

					String[] keywordsArr = str.substring(2, str.length() - 1) // remove
																				// brackets
							.replace("\"", "") // remove apostrophes
							.split(" , ");

					for (int k = 0; k < keywordsArr.length; k++) {
						String keyword = keywordsArr[k];

						// removes strings that are not alphanumeric
						if (!StringUtils.isAlpha(keyword)) {
							continue;
						}

						// removes stop-words
						if (Utils.STOPWORDS.contains(keyword)) {
							continue;
						}

						// remove the country from the keywords
						if (keyword.contains(country.toLowerCase())) {
							continue;
						}

						String lemma = Utils.lemmatize(keyword);
						keywordSet.add(lemma);
					}
					event_keywords.put(_id, keywordSet);
				} catch (NullPointerException | URISyntaxException | UnknownHostException | ParseException e) {
					domains.get(domain_name);
					e.printStackTrace();
					continue;
				} finally {
					long t2 = System.currentTimeMillis();
					time += t2 - t1;
				}
			}
		} finally {
			cursor.close();
		}

		try {
			// exportCollection(domains, "domains");
			Utils.exportCollection(keywordSet, "keywords");
		} catch (IOException e) {
			// TODO Auto-generated catch block
			e.printStackTrace();
		}
		return new WordMatrix(minDate, maxDate, keywordSet);
	}

	public WordMatrix fill() {

		DBCollection coll = MyDB.getDB().getCollection("newsItems_Events");
		int i = 0;
		int old_perc = 0;
		long time = 0;
		long results = event_keywords.keySet().size();

		System.out.println("fill");

		for (String event_id : event_keywords.keySet()) {
			try {
				int new_perc = (int) (++i * 100 / (double) results);
				if (new_perc > old_perc) {
					old_perc = new_perc;
					System.out.println(old_perc + "%. elapsed time: " + time / 1000.0 + " sec.");
				}

				long t1 = System.currentTimeMillis();
				DBObject doc = findDocumentById(coll, event_id);

				String url = ((BasicDBObject) doc.get("link")).get("href").toString();
				String domain_name = Utils.getDomainName(url);

				if (domains.get(domain_name).equals(country)) {
					// the web-site is domestic
					if (foreignOrDomestic == Utils.FOREIGN) {
						continue;
					}

				} else {
					// the web-site is foreign
					if (foreignOrDomestic == Utils.DOMESTIC) {
						continue;
					}

					boolean mentionedCountry = ((BasicDBList) ((BasicDBObject) doc.get("nlp:entity")).get("LOCATION"))
							.contains(country);
					if (!mentionedCountry) {
						continue;
					}
				}

				Calendar date = Utils.parseDateISO8601((doc.get("published")).toString());

				String _id = doc.get("_id").toString();
				HashSet<String> keywordSet = event_keywords.get(_id);

				for (String keyword : keywordSet) {
					int f = Utils.countMatches(doc.get("fullText").toString(), keyword);
					if (f > 0) {
						insert(date, keyword, f);
					}
				}

				long t2 = System.currentTimeMillis();
				time += t2 - t1;
			} catch (ParseException | URISyntaxException e) {
				e.printStackTrace();
				continue;
			}
		}
		return this;
	}

	public void exportTSV() throws IOException {
		System.out.println("starting export");
		File file = new File(name);
		BufferedWriter bw = new BufferedWriter(new FileWriter(file + ".tsv"));

		ArrayList<String> keywordsToK = new ArrayList<>();
		TreeSet<String> sortedKeywords = sortByFrequency();
		int k = Math.min(NUM_OF_KEYWORDS, sortedKeywords.size());

		bw.write("date\t");
		for (int i = 0; i < k; i++) {
			String key = sortedKeywords.pollFirst();
			keywordsToK.add(key);
			key = key.substring(0, 1).toUpperCase() + key.substring(1);

			if (keywordsToK.size() != k)
				bw.write(key + "\t");
			else
				bw.write(key);
		}
		bw.newLine();

		for (int row = 0; row < wordMatrix.length; row++) {
			bw.write(getDateOfRow(row, dateFormatter) + "\t");

			for (String keyword : keywordsToK) {
				bw.write(getf(row, keyword) + "\t");
			}
			bw.newLine();
		}
		bw.close();
	}

	public DBObject findDocumentById(DBCollection collection, String id) {
		BasicDBObject query = new BasicDBObject();
		query.put("_id", new ObjectId(id));
		DBObject dbObj = collection.findOne(query);
		return dbObj;
	}
}
