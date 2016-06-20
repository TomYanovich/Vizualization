import java.io.BufferedWriter;
import java.io.File;
import java.io.FileWriter;
import java.io.IOException;
import java.util.ArrayList;
import java.util.Collection;
import java.util.Collections;
import java.util.Comparator;
import java.util.Date;
import java.util.TreeSet;
import java.util.concurrent.TimeUnit;

public class WordMatrix {
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
	/**
	 * words total number of words document = total number of documents
	 * 
	 * @param words
	 * @param documents
	 */
	public WordMatrix(Date start, Date end, Collection<String> words) {
		super();
		start_date = start;
		end_date = end;
		keywords = new ArrayList<String>(words);
		Collections.sort(keywords);
		wordMatrix = new int[getDateDiff(start, end, TimeUnit.DAYS)+1][keywords.size()];
		wordFrequencies = new int[keywords.size()];
	}

	public int[][] getWordMatrix() {
		return wordMatrix;
	}

	public boolean insert(Date date, String keyword, int f) {
		if (date.before(start_date) || date.after(end_date) || !keywords.contains(keyword) || f <0){
			return false;
		}
		int wordIndex = keywords.indexOf(keyword);
		wordMatrix[getIndexOfDate(date)][wordIndex] += f;
		wordFrequencies[wordIndex]+=f;
		return true;
	}


	public int getf(Date date, String keyword) {
		return wordMatrix[getIndexOfDate(date)][keywords.indexOf(keyword)];
	}


	public int getWordFrequency(String keyword) {
		return wordFrequencies[keywords.indexOf(keyword)];
	}
	
	private static int getDateDiff(Date date1, Date date2, TimeUnit timeUnit) {
	    long diffInMillies = date2.getTime() - date1.getTime();
	    return (int) timeUnit.convert(diffInMillies,TimeUnit.MILLISECONDS);
	}
	
	private int getIndexOfDate(Date date){
		if (date.before(start_date)){
			return -1;
		}
		return getDateDiff(start_date, date, TimeUnit.DAYS);
	}
	
	private TreeSet<String> sortByFrequency(){
		TreeSet<String> sortedKeywords = new TreeSet<>(new Comparator<String>() {

			@Override
			public int compare(String k1, String k2) {
				return wordFrequencies[keywords.indexOf(k2)] - wordFrequencies[keywords.indexOf(k1)];
			}
		});
	sortedKeywords.addAll(keywords);
	return sortedKeywords;
	}
	
	@SuppressWarnings("deprecation")
	public void exportTSV(String filename, int k) throws IOException{
		
		if (k < 0){
			return;
		}
		
		k = Math.min(k, keywords.size());
		
		File file = new File(filename);
		BufferedWriter bw = new BufferedWriter(new FileWriter(file+".tsv"));
		ArrayList<String> keywordsToK= new ArrayList<>();
		TreeSet<String> sortedKeywords = sortByFrequency();
		bw.write("date\t");
		for (int i = 0 ; i < k ; i++){
			String key = sortedKeywords.pollFirst();
			keywordsToK.add(key);
			
			if(keywordsToK.size()!=k)
				bw.write(key + "\t");
			else
				bw.write(key);
		}
		bw.newLine();
		
		Date dayAfterEnd = new Date(end_date.getYear(), end_date.getMonth(), end_date.getDate()+1);		
		for (Date date  = new Date(start_date.getTime()) ; date.before(dayAfterEnd); date.setDate(date.getDate()+1)){
			bw.write(date.getYear()+1900 + "" + date.getMonth()+1 + "" +date.getDate()+"\t");
			for (String keyword : keywordsToK){
				bw.write(getf(date, keyword)+"\t");
			}
			bw.newLine();
		}
		bw.close();
	}
}
