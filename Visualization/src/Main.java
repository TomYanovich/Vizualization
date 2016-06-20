import java.io.IOException;
import java.text.DateFormat;
import java.text.ParseException;
import java.text.SimpleDateFormat;
import java.util.Arrays;
import java.util.Date;
import java.util.HashSet;
import java.util.function.Predicate;

import org.apache.commons.lang3.StringUtils;
import org.bson.types.ObjectId;

import com.mongodb.BasicDBObject;
import com.mongodb.DBCollection;
import com.mongodb.DBCursor;

public class Main {
	public static void main(String[] args) throws IOException {

		String epidode1 = "56c468bb5adbab1a826c04ee";
		HashSet<String> keyWordSet = null;
		WordMatrix wm1 = null;

		BasicDBObject query1 = new BasicDBObject("_id", new ObjectId(epidode1));
		DBCollection coll1 = MyDB.getDB().getCollection("newsItems_Episodes");
		DBCursor cursor1 = coll1.find(query1);

		try {
			if (cursor1.hasNext()) {
				cursor1.next();
				Date start_date = convertStrToDate((cursor1.curr().get("rssUpdFirst")).toString());
				Date end_date = convertStrToDate((cursor1.curr().get("rssUpdLast")).toString());

				String str = ((BasicDBObject) cursor1.curr().get("keywords")).get("terms").toString();

				String[] keywordsArr = str.substring(2, str.length() - 1).replace("\"", "").split(" , ");

				keyWordSet = new HashSet<>(Arrays.asList(keywordsArr));
				Predicate<String> IS_NUMERIC = new Predicate<String>() {
					  @Override
					  public boolean test(String s) {
					    return StringUtils.isNumericSpace(s);
					  }
					};

					keyWordSet.removeIf(IS_NUMERIC);
				

				wm1 = new WordMatrix(start_date, end_date, keyWordSet);
			}
		} finally {
			cursor1.close();
		}

		BasicDBObject query = new BasicDBObject("episodeID", epidode1);
		DBCollection coll = MyDB.getDB().getCollection("newsItems_Events");
		DBCursor cursor = coll.find(query);

		try {
			while (cursor.hasNext()) {
				cursor.next();
				for (String keyword : keyWordSet) {
//					String title = cursor.curr().get("title").toString();
					Date date = convertStrToDate((cursor.curr().get("published")).toString());
					int f = StringUtils.countMatches(cursor.curr().get("fullText").toString().toLowerCase(), keyword);
					if (f > 0){
						wm1.insert(date, keyword, f);
						System.out.println("date= " + date + ", keyword= " + keyword + ", f= " + f);
					}
				}
			}
			wm1.exportTSV("output", 40);
		} finally {
			cursor.close();
		}
	}

	private static Date convertStrToDate(String date) {
		try {
			DateFormat df = new SimpleDateFormat("yyyy-MM-dd");
			return df.parse(date.substring(0, 10));
		} catch (ParseException e) {
			e.printStackTrace();
			return null;
		}
	}
}
