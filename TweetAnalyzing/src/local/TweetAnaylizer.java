package local;
import java.io.BufferedReader;
import java.io.File;
import java.io.FileInputStream;
import java.io.FileNotFoundException;
import java.io.IOException;
import java.io.InputStreamReader;
import java.util.List;
import java.util.UUID;
import java.util.Map.Entry;
 
import com.amazonaws.AmazonClientException;
import com.amazonaws.AmazonServiceException;
import com.amazonaws.auth.AWSCredentials;
import com.amazonaws.auth.PropertiesCredentials;
import com.amazonaws.services.sqs.AmazonSQS;
import com.amazonaws.services.sqs.AmazonSQSClient;
import com.amazonaws.services.sqs.model.CreateQueueRequest;
import com.amazonaws.services.sqs.model.DeleteMessageRequest;
import com.amazonaws.services.sqs.model.DeleteQueueRequest;
import com.amazonaws.services.sqs.model.Message;
import com.amazonaws.services.sqs.model.ReceiveMessageRequest;
import com.amazonaws.services.sqs.model.SendMessageRequest;

import aws.S3;
import aws.SQS;

import org.jsoup.Jsoup;
import org.jsoup.nodes.*;

import com.amazonaws.auth.PropertiesCredentials;
import com.amazonaws.services.s3.AmazonS3;
import com.amazonaws.services.s3.AmazonS3Client;
import com.amazonaws.services.s3.model.PutObjectRequest;


// main class of the first assignment in DSPS course
 public class TweetAnaylizer{

	 public static PropertiesCredentials Credentials;
	 //public static AmazonS3 S3;
	 public static String bucketName = "dspsass1bucket";
	 public static String queueName = "dspsass1queue";
	 public static String propertiesFilePath = "asafsarid.properties";
	 public static String fileToUploadPath = "tweetLinks.txt";

public static void main(String[] args) throws FileNotFoundException, IOException, InterruptedException {
	
		
	
	
		// creating credentials
		AWSCredentials credentials = new PropertiesCredentials(new FileInputStream(propertiesFilePath));
		//Credentials = new PropertiesCredentials(new FileInputStream(propertiesFilePath));
 		System.out.println("Credentials created.");
 		
 		// uploading the input file to S3
 		S3 s3 = new S3(credentials, bucketName);
 		String key = s3.uploadFile(fileToUploadPath);
 		System.out.println("File Uploaded\n");
 		
 		SQS sqs = new SQS(credentials, queueName);
 		sqs.sendMessage("Start");
 		System.out.println("Message Sent To SQS\n");
 		
 		List<Message> queue_msg = sqs.getMessages(1);
 		
        for (Message message : queue_msg) {
            System.out.println("  Message");
            System.out.println("    MessageId:     " + message.getMessageId());
            System.out.println("    ReceiptHandle: " + message.getReceiptHandle());
            System.out.println("    MD5OfBody:     " + message.getMD5OfBody());
            System.out.println("    Body:          " + message.getBody());
            for (Entry<String, String> entry : message.getAttributes().entrySet()) {
                System.out.println("  Attribute");
                System.out.println("    Name:  " + entry.getKey());
                System.out.println("    Value: " + entry.getValue());
            }
        }
        System.out.println();
 		
        
 		s3.downloadFile(key);
 		System.out.println("File Downloaded\n");
 		
//		System.out.println("Starting anaylizing");
//		// create worker and init it
//		Worker worker1 = new Worker();
//		
//		// Open the file
//		FileInputStream fstream = new FileInputStream("tweetLinks.txt");
//		BufferedReader br = new BufferedReader(new InputStreamReader(fstream));
//
//		String tweetLink;
//
//		//Read File Line By Line
//		while ((tweetLink = br.readLine()) != null)   {
//		  Document tweetPage = Jsoup.connect(tweetLink).get();
//		  String tweet = tweetPage.select("title").first().text();
//		  
//		  TweetAnaylizingOutput tempOutput = worker1.analyzeTweet(tweet);
//		  
//		  System.out.println("Tweet: " + tempOutput.tweet);
//		  System.out.println("Color: " + tempOutput.sentimentColor);
//		  System.out.println("Entities: " + tempOutput.namedEntities + "\n");
//		}
//		//Close the input stream
//		br.close();
		
		System.out.println("Bye bye");
	}
 }