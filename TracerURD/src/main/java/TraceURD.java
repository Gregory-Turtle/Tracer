
public class TraceURD {

	public static void main(String[] args) {
		// TODO Auto-generated method stub
		//db.getCollection('webmail').aggregate([{"$match" : {"time" : {"$gte" : 37, "$lt" : 631}}},{"$group" : {"_id" : "$address", "max" : {"$max" : "$time"}, "min" : {"$min" : "$time"}}}])
		if(args.length != 2) {
			System.out.println("Please specify only the collection that contains the trace and the number of records to pull per read.");
			System.exit(0);
		}
		
		String collection_name = args[0];
		int chunk_size;
		
		try {
			chunk_size = Integer.parseInt(args[1]);
			UniqueReuseDistanceFinder reuseDistanceFinder = new UniqueReuseDistanceFinder(collection_name);
			
			if(!reuseDistanceFinder.hasData()) {
				System.out.println("This trace does not exist or is empty.");
				System.exit(0);
			}
			
			reuseDistanceFinder.Calculate(chunk_size);
		} catch(Exception ex) {
			//System.out.println(ex.toString());
			//System.exit(0);
			ex.printStackTrace();
		}
	}

}
