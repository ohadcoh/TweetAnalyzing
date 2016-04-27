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
import java.util.UUID;
import java.util.Vector;

import com.amazonaws.auth.AWSCredentials;
import com.amazonaws.services.s3.model.S3Object;
import com.amazonaws.services.sqs.model.Message;

import aws.S3;
import aws.SQS;

public class Manager {
	private SQS localToManager;
	private SQS managerToWorker;
	private SQS workerToManager;
	private S3 s3;
	private int numOfWorkers;
	private Vector<String> jobID;
	AWSCredentials credentials;
	
	// for now it is constructor, but later it will be stand-alone class
	//public static void main(String[] args) throws FileNotFoundException, IOException, InterruptedException {
	public Manager(AWSCredentials _credentials, SQS _localToManager, S3 _s3, int _numOfWorkers) throws FileNotFoundException, IOException {
		// parse all input argument to the needed variables
		
		super();
		credentials =_credentials;
		localToManager = _localToManager;
		// 3. uploading the input file to S3
		s3 = _s3;
		// 4. assign number of worker for this manager
		numOfWorkers = _numOfWorkers;
		// 5. create queues ManagerToWorker and workerToManager
		managerToWorker = new SQS(credentials, "managertoworker" + UUID.randomUUID());
		workerToManager = new SQS(credentials, "workerToManager" + UUID.randomUUID());
		
		
		// 6. init jobID list
		jobID = new Vector<String>();
		
	}
	
	// returns error value: 0- good, other- error
	public int startWork(String currJobID) throws IOException{
		// add jobID
		jobID.add(currJobID);
		// read file key
		Message inputMessage = localToManager.getMessages(1).get(0);
		S3Object inputFile = s3.downloadFile(inputMessage.getBody());
		s3.deleteFile(inputMessage.getBody());
		BufferedReader reader = new BufferedReader(new InputStreamReader(inputFile.getObjectContent()));
		// add all links to queue with the jobID
    	while (true) {          
    		String line = reader.readLine();           
    		if (line == null || line.length() == 0)
    			break;
    		managerToWorker.sendMessage(line + " " + currJobID);
    	}
    	reader.close();
    	// start workers
    	// requests from AWS- for now create one worker
    	Worker worker1 = new Worker(credentials, managerToWorker, workerToManager);
    	worker1.analyzeTweet();
    	System.out.println("Worker Finishes!");
    	
    	String pathToOutputFile = "./ManagerOutputFile";
    	File file = new File(pathToOutputFile);      
        Writer writer = new OutputStreamWriter(new FileOutputStream(file));

    	while (true) {   
    		List<Message>  finalMessagesList = workerToManager.getMessages(1);
    		if(finalMessagesList.size() == 0)
    			break;
           

    	     writer.write(finalMessagesList.get(0).getBody() + "\n");
    	     workerToManager.deleteMessage(finalMessagesList.get(0));
    	}
        writer.close();
        // upload file to S3
        String inputFileS3key = s3.uploadFile(pathToOutputFile);
 		System.out.println("Manager output File Uploaded\n");

 		// send to local
 		localToManager.sendMessage(inputFileS3key);
 		
 		// finish process
    	managerToWorker.deleteQueue();
    	workerToManager.deleteQueue();
		return 0;
	}
	
	
}
