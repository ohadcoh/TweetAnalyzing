package local;
import java.io.BufferedReader;
import java.io.BufferedWriter;
import java.io.File;
import java.io.FileInputStream;
import java.io.FileOutputStream;
import java.io.IOException;
import java.io.InputStreamReader;
import java.io.OutputStreamWriter;
import java.util.List;
import java.util.UUID;
import org.joda.time.LocalDateTime;
import com.amazonaws.auth.AWSCredentials;
import com.amazonaws.auth.PropertiesCredentials;
import com.amazonaws.services.s3.model.S3Object;
import com.amazonaws.services.sqs.model.Message;
import aws.EC2;
import aws.S3;
import aws.SQS;
//import remote.Manager;
 public class TweetAnaylizer{
	// aws 
	private AWSCredentials credentials;
	private EC2 ec2;
	private S3 s3;
	private SQS localToManager;
	private SQS managerToLocal;
	// internal info
	private String id;
	// arguments from user
	private boolean terminate;
	private int n;
	
	// main function
    public static void main(String[] args) throws Exception {
    	// 1. parse input arguments
    	int argsNum = args.length;
    	if(argsNum != 3 && argsNum != 4 )
    	{
    		System.out.println("LocalApp: Usage: inputFileName outputFileName n terminate(optional)");
    		return;
    	}
    	System.out.println(LocalDateTime.now());
    	boolean terminate = false;
		String inputFileName = args[0];
		String outputFileName = args[1];
		int n = Integer.parseInt(args[2]);
		if(argsNum == 4 && args[3].equals("terminate"))
			terminate = true;
		// 2. hard coded names
		String bucketName 				= "dspsass1bucketasafohad";
		String localToManagerQueueName 	= "localToManagerasafohad";
		String managerToLocalQueueName 	= "managerToLocalasafohad";
		String propertiesFilePath 		= "./dspsass1.properties";
		// 4. create instance of TweetAnaylizer
		TweetAnaylizer myTweetAnaylizer = new TweetAnaylizer(propertiesFilePath, 
															 bucketName,
															 localToManagerQueueName,
															 managerToLocalQueueName,
															 terminate,
															 n);
		// 5. run the analyzing
		myTweetAnaylizer.run(inputFileName, outputFileName);
		// 6. terminate analyzing
		myTweetAnaylizer.terminate();
		System.out.println(LocalDateTime.now());
		System.out.println("LocalApp: Bye bye");
	}
    
	private TweetAnaylizer(	String propertiesFilePath, 
							String bucketName,
							String localToManagerQueueName,
							String managerToLocalQueueName,
							boolean terminate,
							int n) {
		
		try {
			// 1. creating credentials and ec2 client
			credentials = new PropertiesCredentials(new FileInputStream(propertiesFilePath));
			System.out.println("LocalApp: Credentials created.");
	 	 	ec2 = new EC2(credentials);
	 		// 2. creating S3 client and bucket
	 		s3 = new S3(credentials, bucketName);
	 		// 3. create queue (from local app to manager) and send message with the key of the file
	 		localToManager = new SQS(credentials, localToManagerQueueName);
	 		managerToLocal = new SQS(credentials, managerToLocalQueueName);
		} catch (IOException e) {
			e.printStackTrace();
		}
		this.terminate 	= terminate;
		this.n			= n;
		this.id			= UUID.randomUUID().toString();
	}
	
	private void run(String inputFileName, String outputFileName) {
		// 1. upload input file to s3
 		String inputFileS3key = s3.uploadFile(inputFileName, this.id);
 		System.out.println("LocalApp: File Uploaded\n");
 		// 2. send key to manager with attributes (numOfWorkers and my id)
 		localToManager.sendMessageWithNumOfWorkersAndId(inputFileS3key, String.valueOf(n), id);
		// 3. find if there is manager instance and if not create manager instance
		if(!ec2.checkIfManagerExist())
		{
			System.out.println("LocalApp: manager does not exist");
			ec2.startManagerInstance();
		}
 		// 4. read message- analyzed data
 		// read until receive answer from manager
 		List<Message> messageFromManagerList;
 		S3Object outputFile = null;
 		boolean msgFound = false;
 		boolean managerAlive = true;
 		int managerAliveCheck = 0;
 		
 		// 5. wait for message with my IP
 		while (!msgFound && managerAlive){
 			messageFromManagerList = managerToLocal.getMessagesMinimalVisibilityTime(1);
 			for (Message message : messageFromManagerList){
 				if (message.getMessageAttributes().get("id").getStringValue().equals(id)){
 					msgFound = true;
 			        System.out.println("LocalApp: Message from Manager with my id: " + message.getBody());
 			        // 6. download the output file, and delete it      
 			        outputFile = s3.downloadFile(message.getBody());
 			        s3.deleteFile(message.getBody());
 			        managerToLocal.deleteMessage(message);
 				}
 			}
			try {
				Thread.sleep(500);
			} catch (InterruptedException e) {
				e.printStackTrace();
			}
			if (++managerAliveCheck == 10){
				if (!ec2.checkIfManagerExist()){
					managerAlive = false;
					if (this.terminate){
			            localToManager.deleteQueue();
			            managerToLocal.deleteQueue();
					}
				}
				managerAliveCheck = 0;
			}
 		}
 		if (!managerAlive){
 			System.out.println("Manager is not alive!  Exiting...");
 			System.exit(1);
 		}
 		
    	// 9. create HTML file
    	try {
			createHTMLFile(outputFile, outputFileName+".html");
		} catch (IOException e) {
			System.out.println("Error: Failed to write HTML file");
			e.printStackTrace();
		}
	}
	
	private void terminate() {
    	// send termination message to manager
    	if(terminate)
    	{
    		localToManager.sendMessageWithIdAndTerminate("I am terminating you!", id);
    		// 4. read ack terminate message
	 		
     		List<Message> messageFromManagerList;
     		boolean msgFound = false;
     		while (!msgFound){
     			messageFromManagerList = managerToLocal.getMessagesMinimalVisibilityTime(1);
     			for (Message message : messageFromManagerList){
     				if (message.getMessageAttributes().get("id").getStringValue().equals(id)){
     					msgFound = true;
     					System.out.println("Manager ack message for termination: " + message.getBody());
     				}
     			}
				try {
					Thread.sleep(500);
				} catch (InterruptedException e) {
					e.printStackTrace();
				}
				
     		}
  		
            localToManager.deleteQueue();
            managerToLocal.deleteQueue();
    	}
    	
	}
	 
    // create HTML file from the analyzed tweets file
	// create HTML file from the analyzed tweets file
		private void createHTMLFile(S3Object outputFile,String outputFilePath) throws IOException {
			File fout = new File(outputFilePath);
			FileOutputStream fos = new FileOutputStream(fout);
			BufferedWriter bw = new BufferedWriter(new OutputStreamWriter(fos));
			BufferedReader reader = new BufferedReader(new InputStreamReader(outputFile.getObjectContent()));
			// create output string with header
	    	bw.write("<HTML>\n<HEAD>\n</HEAD>\n<BODY>\n");
	    	while (true) {          
	    	    String line;
				try {
					line = reader.readLine();
					if (line == null)
					{
						reader.close();
						break;
					}
					if(createOutputLine(line) != ""){
						bw.write(createOutputLine(line));
						bw.newLine();
					}
						
				} catch (IOException e) {
					e.printStackTrace();
				}           
	    	}
	    	bw.write("</BODY>\n</HTML>");
	    	bw.close();
			System.out.println("LocalApp: Write html file to: "+ outputFilePath);
			
		}

		private String createOutputLine(String line) {
			String sentiment  	= line.substring(0, line.indexOf(';'));
			String entitiesAndTweet = line.substring(line.indexOf(';')+1, line.length());
			String entities 	= entitiesAndTweet.substring(0, entitiesAndTweet.indexOf(';'));
			String tweet 		= entitiesAndTweet.substring(entitiesAndTweet.indexOf(';')+1, entitiesAndTweet.length());
			if(tweet.equals(""))
				return "";
			String rawTweet = "";
			try {
				rawTweet		= tweet.substring(tweet.indexOf('"')+1,tweet.lastIndexOf('"'));
			} catch (IndexOutOfBoundsException e) {
				return "";
			}
	       return "<p> <b><font color=\"" + sentiment + "\"> " + rawTweet + "</font></b> " + entities + "</p>\n";
		}
    
 }