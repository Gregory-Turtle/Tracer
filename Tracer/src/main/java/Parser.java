
import java.io.BufferedReader;
import java.io.FileInputStream;
import java.io.FileNotFoundException;
import java.io.IOException;
import java.io.InputStreamReader;

public class Parser 
{
	FileInputStream fis;
	BufferedReader br;
	Request[] requests;
	int size;
	boolean done;
	int max_requests;
	
	public Parser(String filename, int max_requests) throws FileNotFoundException
	{
		this.max_requests = max_requests;
		this.fis = new FileInputStream(filename);
		this.br = new BufferedReader(new InputStreamReader(this.fis));
		this.requests = new Request[this.max_requests];
		this.size = 0;
		this.done = false;
	}
	
	public void Get_Requests() throws NumberFormatException, IOException
	{
		int i = 0;
		
		while(i < this.max_requests && !this.done)
		{
			String strLine = "";
			
			if((strLine = this.br.readLine()) != null)
			{
				String[] splitArray = strLine.split("\\s+");
				long source = Long.parseLong(splitArray[3]);
				short is_read = (short) ((splitArray[5].equalsIgnoreCase("W")) ? 1 : 0);
				this.requests[i] = new Request(source, is_read);
				i++;
			}
			else
			{
				this.done = true;
				break;
			}
		}
		
		this.size = i;
	}
}





























