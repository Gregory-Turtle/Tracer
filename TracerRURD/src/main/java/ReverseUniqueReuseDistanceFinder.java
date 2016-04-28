import java.util.ArrayList;
import java.util.List;

import org.bson.Document;

import com.mongodb.MongoClient;
import com.mongodb.client.MongoCollection;
import com.mongodb.client.MongoDatabase;

public class ReverseUniqueReuseDistanceFinder {
	MongoClient mongoClient;
	MongoDatabase db;
	MongoCollection<Document> docs;
	
	public ReverseUniqueReuseDistanceFinder(String collection) {
		mongoClient = new MongoClient();
		db = mongoClient.getDatabase("tracer");
		docs = db.getCollection(collection);
	}
	
	public boolean hasData() {
		long count = docs.count();
		
		if(count > 0) {
			return true;
		}
		
		return false;
	}
	
	public void Calculate(int chunk_size) {
		boolean loop = true;
		int round = 0;
		int counter = 0;
		
		while(loop) {
			List<Document> requests = (List<Document>) docs.find().sort(new Document("time", 1))
					.skip(round * chunk_size)
					.limit(chunk_size)
					.into(new ArrayList<Document>());

			if(!requests.isEmpty()) {
				for (Document request : requests) {
				    this.CheckDocument(request);
				    counter++;
				    
				    if(counter % 1 == 0) {
						System.out.println(counter + " records scanned.");
					}
				}
				
				round++;
			} else {
				loop = false;
			}
		}
	}
	
	private void CheckDocument(Document doc) {
		try {
			if(!doc.containsKey("reverse_unique_reuse_distance")) {
	        	this.FindByAddress(doc);
			}
		} catch(Exception ex) {
			//System.out.println(ex.toString());
			ex.printStackTrace();
		}
	}
	
	private void FindByAddress(Document doc) {
		long address = doc.getLong("address");
		List<Document> results = docs.find(new Document("address", address)).sort(new Document("time", 1))
				.into(new ArrayList<Document>());
				
		System.out.println("Shared addresses: " + results.size() + " for address " + address);
		
		for(int i = 0; i < results.size(); i++) {
			if(i == 0) {
				results.get(i).put("reverse_unique_reuse_distance", new Long(-1));
			} else {
				long unique_reuse_distance = results.get(i-1).getLong("unique_reuse_distance");
				results.get(i).put("reverse_unique_reuse_distance", unique_reuse_distance);
			}
			
			docs.updateOne(new Document("time", results.get(i).getLong("time")),
			        new Document("$set", results.get(i)));
		}
	}
}




























