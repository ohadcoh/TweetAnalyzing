package remote;

import java.io.BufferedReader;
import java.io.File;
import java.io.FileNotFoundException;
import java.io.FileOutputStream;
import java.io.IOException;
import java.io.InputStreamReader;
import java.io.OutputStreamWriter;
import java.io.Writer;
import java.util.List;

import com.amazonaws.services.s3.model.S3Object;
import com.amazonaws.services.sqs.model.Message;

import aws.S3;
import aws.SQS;

public class Task {
	private String id;
	private SQS localToManager;
	private SQS managerToLocal;
	//private SQS managerToLocal;
	private boolean terminate;
	private long counter;
	private long remainingCounter;
	private File outputFile;
	private Writer writer;
	private int n;
	
	public Task(String id, SQS localToManager, SQS managerToLocal){
		super();
		this.id 			= id;
		this.localToManager = localToManager;
		this.managerToLocal = managerToLocal;
		this.terminate 		= false;
		//this.managerToLocal = new SQS(credentials, "managerToLocal" + String.valueOf(id));
	}
	
	public int startTask(S3 s3, SQS managerToWorker)
	{
		
		// 1.1 read input message, send to local its queue (keep reading until receive request)
		List<Message> inputMessageList;
		do{
			inputMessageList = localToManager.getMessages(1);
			try {
				Thread.sleep(500);
			} catch (InterruptedException e) {
				e.printStackTrace();
			}
		}while(inputMessageList.size() == 0);
		Message	inputMessage = inputMessageList.get(0);
		localToManager.deleteMessage(inputMessage);
		// 1.2 send to local its id
		//localToManager.sendMessage(id);
		
		// 1.2 find attributes
		n = Integer.parseInt(inputMessage.getMessageAttributes().get("numOfWorkers").getStringValue());

		System.out.println("numOfWorkers: " + inputMessage.getMessageAttributes().get("numOfWorkers").getStringValue());
		System.out.println("terminate: " + inputMessage.getMessageAttributes().get("terminate").getStringValue());
		
		if(inputMessage.getMessageAttributes().get("terminate").getStringValue().equals("true"))
			terminate = true;
		
		// 2. download input file, delete it from s3
		S3Object inputFile = s3.downloadFile(inputMessage.getBody());
		s3.deleteFile(inputMessage.getBody());
		// 3. each line is a message in queue for workers
		BufferedReader reader = new BufferedReader(new InputStreamReader(inputFile.getObjectContent()));
    	while (true) {          
    		String line = "";
			try {
				line = reader.readLine();
			} catch (IOException e) {
				// TODO Auto-generated catch block
				e.printStackTrace();
				return 1;
			}           
    		if (line == null || line.length() == 0)
    			break;
    		// send with id attribute
    		managerToWorker.sendMessageType2(line, id);
    		counter++;
    		remainingCounter++;
    	}
    	try {
			reader.close();
		} catch (IOException e1) {
			// TODO Auto-generated catch block
			e1.printStackTrace();
			return 1;
		}
		// 3. open output file and writer
		this.outputFile = new File("./managerOutputFile" + this.id);
		try {
			this.writer = new OutputStreamWriter(new FileOutputStream(outputFile));
		} catch (FileNotFoundException e) {
			// TODO Auto-generated catch block
			e.printStackTrace();
			return 1;
		}
		return 0;
	}

	public void addLineToFile(String line)
	{
		// write line and decrement the counter of the remaining lines to analyze
		try {
			writer.write(line + "\n");
			this.remainingCounter--;
		} catch (IOException e) {
			// TODO Auto-generated catch block
			e.printStackTrace();
		}
	}
	
	public void finishTask(S3 s3)
	{
		// close writer, upload file to s3 and send message to local applications
		try {
			writer.close();
		} catch (IOException e) {
			// TODO Auto-generated catch block
			e.printStackTrace();
		}
		String outputFileS3key = s3.uploadFile("./managerOutputFile" + this.id);
		System.out.println("Tasks output File Uploaded\n");
		managerToLocal.sendMessageType2(outputFileS3key, id);
		//managerToLocal.deleteQueue();
	}
	
	public String getId() {
		return id;
	}

	public int getN() {
		return n;
	}

	public SQS getLocalToManager() {
		return localToManager;
	}

	public void setLocalToManager(SQS localToManager) {
		this.localToManager = localToManager;
	}

	public boolean isTerminate() {
		return terminate;
	}

	public void setTerminate(boolean terminate) {
		this.terminate = terminate;
	}

	public long getCounter() {
		return counter;
	}

	public long getRemainingCounter() {
		return remainingCounter;
	}
	
	public boolean getTerminate()
	{
		return terminate;
	}

	public void decrementRemainingCounter() {
		this.remainingCounter--;
		
	}
	
}
