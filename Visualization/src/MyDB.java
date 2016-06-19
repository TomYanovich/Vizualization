import java.net.UnknownHostException;
import java.util.Arrays;

import com.mongodb.DB;
import com.mongodb.MongoClient;
import com.mongodb.MongoCredential;
import com.mongodb.ServerAddress;

public class MyDB {
	private static DB db;

	private MyDB() {
		try {
		MongoCredential credential = MongoCredential.createCredential("stud", "infoMedia_PRD",
				"stud".toCharArray());
		
			MongoClient mongoClient = new MongoClient(new ServerAddress("132.74.122.194", 27017),
					Arrays.asList(credential));
			db = mongoClient.getDB("infoMedia_PRD");
		} catch (UnknownHostException e) {
			// TODO Auto-generated catch block
			e.printStackTrace();
		}
	}

	public static DB getDB() {
		if (db == null) {
			new MyDB();
		}
		return db;
	}
}
