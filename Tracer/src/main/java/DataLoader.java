import java.util.ArrayList;

import org.bson.Document;

import com.mongodb.MongoClient;
import com.mongodb.client.MongoCollection;
import com.mongodb.client.MongoDatabase;

public class DataLoader {
	MongoClient mongoClient;
	MongoDatabase db;
	MongoCollection<Document> docs;
	ArrayList<Document> batch;
	long counter;
	
	public DataLoader(String collection_name) {
		mongoClient = new MongoClient();
		db = mongoClient.getDatabase("tracer");
		docs = db.getCollection(collection_name);
		this.batch = new ArrayList<>();
		counter = 0;
	}
	
	public boolean isSafe() {
		long count = docs.count();
		
		if(count > 0) {
			return false;
		}
		
		return true;
	}
	
	public void Batch(Request request) {
		Document doc = new Document();
		doc.append("time", ++counter);
		doc.append("address", new Long(request.address));
		doc.append("type", new Short(request.type));
		this.batch.add(doc);
		//docs.insertOne(doc);
	}
	
	public void Store() {
		docs.insertMany(batch);
		batch.clear();
	}
	
	public void Done() {
		docs.createIndex(new Document("address", 1));
		docs.createIndex(new Document("time", 1));
	}
}
