import java.util.ArrayList;
import java.util.List;

import org.bson.Document;

import com.mongodb.MongoClient;
import com.mongodb.client.MongoCollection;
import com.mongodb.client.MongoDatabase;

public class UniqueReuseDistanceFinder {
	MongoClient mongoClient;
	MongoDatabase db;
	MongoCollection<Document> docs;
	String collection;
	
	public UniqueReuseDistanceFinder(String collection) {
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
		long reuse_distance = doc.getLong("reuse_distance");
		long time = doc.getLong("time");
		
		if(reuse_distance > -1) {
			long reused = time + reuse_distance;
			ArrayList<Document> pipeline = new ArrayList<>();
			Document match = new Document();
			match.append("$match", new Document("time", new Document("$gte", time).append("$lt", reused)));
			Document group = new Document();
			group.append("$group", new Document("_id", "$address"));
			pipeline.add(match);
			pipeline.add(group);
			ArrayList<Document> alldocs = docs.aggregate(pipeline).into(new ArrayList<Document>());
			int size = alldocs.size();
			doc.append("unique_reuse_distance", new Long(size));
			
			docs.updateOne(new Document("time", time),
			        new Document("$set", doc));
			System.out.println("Updated: " + time);
		} else {
			doc.append("unique_reuse_distance", new Long(-1));
			docs.updateOne(new Document("time", time),
			        new Document("$set", doc));
			System.out.println("Negative: " + time);
		}
	}
}






























