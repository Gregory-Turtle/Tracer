import java.util.ArrayList;
import java.util.List;

import org.bson.Document;

import com.mongodb.MongoClient;
import com.mongodb.client.MongoCollection;
import com.mongodb.client.MongoDatabase;

public class ReverseFrequentReuseDistanceFinder {
	MongoClient mongoClient;
	MongoDatabase db;
	MongoCollection<Document> docs;
	String collection;
	
	public ReverseFrequentReuseDistanceFinder(String collection) {
		mongoClient = new MongoClient();
		db = mongoClient.getDatabase("tracer");
		docs = db.getCollection(collection);
		this.collection = collection;
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
		long reverse_reuse_distance = doc.getLong("reverse_reuse_distance");
		long time = doc.getLong("time");

		if(reverse_reuse_distance > -1) {
			long used = time - reverse_reuse_distance;
			ArrayList<Document> pipeline = new ArrayList<>();
			Document match = new Document();
			match.append("$match", new Document("time", new Document("$lte", time).append("$gt", used))
					.append("reverse_reuse_distance", new Document("$gte", 1)));
			Document group = new Document();
			group.append("$group", new Document("_id", "$address"));
			pipeline.add(match);
			pipeline.add(group);
			ArrayList<Document> alldocs = docs.aggregate(pipeline).into(new ArrayList<Document>());
			int size = alldocs.size();
			doc.append("reverse_frequent_reuse_distance", size);
			
			docs.updateOne(new Document("time", time),
			        new Document("$set", doc));
		} else {
			doc.append("reverse_frequent_reuse_distance", new Long(-1));
			
			docs.updateOne(new Document("time", time),
			        new Document("$set", doc));
		}
	}
}






























