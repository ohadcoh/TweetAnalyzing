package local;
import java.io.FileInputStream;
import java.io.FileNotFoundException;
import java.io.IOException;
import java.util.UUID;
import java.util.Map.Entry;
 
import com.amazonaws.auth.AWSCredentials;
import com.amazonaws.auth.PropertiesCredentials;
import com.amazonaws.services.sqs.model.Message;

import aws.S3;
import aws.SQS;


// main class of the first assignment in DSPS course
 public class TweetAnaylizer{

	 public static PropertiesCredentials Credentials;
	 public static String bucketName = "dspsass1bucket";
	 public static String localToManagerQueueName = "localtomanager" + UUID.randomUUID();
	 public static String ManagerToLocalQueueName = "managertolocal" + UUID.randomUUID();
	 public static String propertiesFilePath = "./ohadInfo.properties";
	 public static String fileToUploadPath = "tweetLinks.txt";

public static void main(String[] args) throws FileNotFoundException, IOException, InterruptedException {
	
		// 1. creating credentials
		AWSCredentials credentials = new PropertiesCredentials(new FileInputStream(propertiesFilePath));
 		System.out.println("Credentials created.");
 		
 		// 2. uploading the input file to S3
 		S3 s3 = new S3(credentials, bucketName);
 		String inputFileS3key = s3.uploadFile(fileToUploadPath);
 		System.out.println("File Uploaded\n");
 		
 		// 3. create output queue (from local app to manager) and send message with the key of the file
 		SQS localToManager = new SQS(credentials, localToManagerQueueName);
 		localToManager.sendMessage(inputFileS3key);
 		
 		// 4. create input queue (from manager to local app) and receive message
 		SQS managerToLocal = new SQS(credentials, ManagerToLocalQueueName);
 		Message messageFromManager = managerToLocal.getMessages(1).get(0);
 		
 		System.out.println("  Message");
        System.out.println("    MessageId:     " + messageFromManager.getMessageId());
        System.out.println("    ReceiptHandle: " + messageFromManager.getReceiptHandle());
        System.out.println("    MD5OfBody:     " + messageFromManager.getMD5OfBody());
        System.out.println("    Body:          " + messageFromManager.getBody());
        for (Entry<String, String> entry : messageFromManager.getAttributes().entrySet()) {
            System.out.println("  Attribute");
            System.out.println("    Name:  " + entry.getKey());
            System.out.println("    Value: " + entry.getValue());
        }
 		
        // 5. download the output file
 		s3.downloadFile(messageFromManager.getBody());
 		System.out.println("File Downloaded\n");
		
		System.out.println("Bye bye");
	}
 }