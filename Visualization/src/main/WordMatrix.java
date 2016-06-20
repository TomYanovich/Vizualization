package main;

import java.io.BufferedWriter;
import java.io.File;
import java.io.FileWriter;
import java.io.IOException;
import java.text.DateFormat;
import java.text.ParseException;
import java.text.SimpleDateFormat;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collection;
import java.util.Collections;
import java.util.Comparator;
import java.util.Date;
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

import com.mongodb.BasicDBObject;
import com.mongodb.DBCollection;
import com.mongodb.DBCursor;

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

	private final Date start_date;

	private final Date end_date;

	private ArrayList<String> keywords;

	private String episode_id;

	/**
	 * words total number of words document = total number of documents
	 * 
	 * @param words
	 * @param documents
	 */
	public WordMatrix(String id, Date start, Date end, Collection<String> words) {
		super();
		episode_id = id;
		start_date = start;
		end_date = end;
		keywords = new ArrayList<String>(words);
		Collections.sort(keywords);
		wordMatrix = new int[getDateDiff(start, end, TimeUnit.DAYS) + 1][keywords.size()];
		wordFrequencies = new int[keywords.size()];
	}

	public int[][] getWordMatrix() {
		return wordMatrix;
	}

	public boolean insert(Date date, String keyword, int f) {
		if (date.before(start_date) || date.after(end_date) || !keywords.contains(keyword) || f < 0) {
			return false;
		}
		int wordIndex = keywords.indexOf(keyword);
		wordMatrix[getIndexOfDate(date)][wordIndex] += f;
		wordFrequencies[wordIndex] += f;
		return true;
	}

	public int getf(Date date, String keyword) {
		return wordMatrix[getIndexOfDate(date)][keywords.indexOf(keyword)];
	}

	public int getWordFrequency(String keyword) {
		return wordFrequencies[keywords.indexOf(keyword)];
	}

	private int getIndexOfDate(Date date) {
		if (date.before(start_date)) {
			return -1;
		}
		return getDateDiff(start_date, date, TimeUnit.DAYS);
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

	public String getEpisodeId() {
		return episode_id;
	}

	public List<String> getKeywords() {
		return keywords;
	}

	private static int getDateDiff(Date date1, Date date2, TimeUnit timeUnit) {
		long diffInMillies = date2.getTime() - date1.getTime();
		return (int) timeUnit.convert(diffInMillies, TimeUnit.MILLISECONDS);
	}

	public static Date convertStrToDate(String date) {
		try {
			DateFormat df = new SimpleDateFormat("yyyy-MM-dd");
			return df.parse(date.substring(0, 10));
		} catch (ParseException e) {
			e.printStackTrace();
			return null;
		}
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
				Date start_date = convertStrToDate((cursor.curr().get("rssUpdFirst")).toString());
				Date end_date = convertStrToDate((cursor.curr().get("rssUpdLast")).toString());
				int duration = getDateDiff(start_date, end_date, TimeUnit.DAYS);
				episodesMap.put(_id, duration);
			} catch (NullPointerException e) {
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

	public static WordMatrix init(String episode_id) {
		HashSet<String> keyWordSet = null;
		BasicDBObject query = new BasicDBObject("_id", new ObjectId(episode_id));
		DBCollection coll = MyDB.getDB().getCollection("newsItems_Episodes");
		DBCursor cursor = coll.find(query);

		if (cursor.hasNext()) {
			cursor.next();
			Date start_date = convertStrToDate((cursor.curr().get("rssUpdFirst")).toString());
			Date end_date = convertStrToDate((cursor.curr().get("rssUpdLast")).toString());

			String str = ((BasicDBObject) cursor.curr().get("keywords")).get("terms").toString();
			String[] keywordsArr = str.substring(2, str.length() - 1).replace("\"", "").split(" , ");

			keyWordSet = new HashSet<>(Arrays.asList(keywordsArr));
			keyWordSet.removeIf(p -> StringUtils.isNumericSpace(p));

			return new WordMatrix(episode_id, start_date, end_date, keyWordSet);
		}
		return null;
	}

	public WordMatrix fill() {
		BasicDBObject query = new BasicDBObject("episodeID", getEpisodeId());
		DBCollection coll = MyDB.getDB().getCollection("newsItems_Events");
		DBCursor cursor = coll.find(query);
		List<String> keywords = getKeywords();
		while (cursor.hasNext()) {
			cursor.next();
			for (String keyword : keywords) {
				// String title = cursor.curr().get("title").toString();
				Date date = convertStrToDate((cursor.curr().get("published")).toString());
				int f = StringUtils.countMatches(cursor.curr().get("fullText").toString().toLowerCase(), keyword);
				if (f > 0) {
					insert(date, keyword, f);
					// System.out.println("date= " + date + ", keyword= " +
					// keyword + ", f= " + f);
				}
			}
		}
		return this;
	}

	@SuppressWarnings("deprecation")
	public void exportTSV() throws IOException {

		File file = new File(getEpisodeId());
		BufferedWriter bw = new BufferedWriter(new FileWriter(file + ".tsv"));
		ArrayList<String> keywordsToK = new ArrayList<>();
		TreeSet<String> sortedKeywords = sortByFrequency();
		int k = Math.min(NUM_OF_KEYWORDS, sortedKeywords.size());
		bw.write("date\t");
		for (int i = 0; i < k; i++) {
			keywordsToK.add(sortedKeywords.first());
			bw.write(sortedKeywords.pollFirst() + "\t");
		}
		bw.newLine();

		Date dayAfterEnd = new Date(end_date.getYear(), end_date.getMonth(), end_date.getDate() + 1);
		for (Date date = new Date(start_date.getTime()); date.before(dayAfterEnd); date.setDate(date.getDate() + 1)) {
			bw.write(date.getYear() + 1900 + "" + date.getMonth() + 1 + "" + date.getDate() + "\t");
			for (String keyword : keywordsToK) {
				bw.write(getf(date, keyword) + "\t");
			}
			bw.newLine();
		}
		bw.close();
	}
}
