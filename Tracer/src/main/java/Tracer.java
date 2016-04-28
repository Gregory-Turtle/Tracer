
public class Tracer {

	public static void main(String[] args) {
		// TODO Auto-generated method stub
		if(args.length != 3) {
			System.out.println("Please provide only the file path of the tracefile, the name of the collection and "
					+ "the max number of lines to be read from the trace file at a time.");
			System.exit(0);
		}
		
		String filename = args[0];
		String collection = args[1];
		int max_reads;
		try {
			max_reads = Integer.parseInt(args[2]);
			Parser parser = new Parser(filename, max_reads);
			DataLoader loader = new DataLoader(collection);
			
			if(!loader.isSafe()) {
				System.out.println("There is already a collection with this name");
				System.exit(0);
			}
			
			int total = 0;
			
			while(!parser.done) {
				parser.Get_Requests();
				
				for(int i = 0; i < parser.requests.length; i++) {
					Request req = parser.requests[i];
					loader.Batch(req);
					total++;
					
					if(total % 1000 == 0) {
						System.out.println(total + " records stored.");
					}
				}
				
				loader.Store();
			}
			
			loader.Done();
			
			System.out.println("Storage complete.");
		} catch (Exception e) {
			// TODO Auto-generated catch block
			e.printStackTrace();
			System.exit(0);
		}
	}

}
