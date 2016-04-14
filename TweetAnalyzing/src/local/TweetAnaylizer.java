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
import org.jsoup.Jsoup;
import org.jsoup.nodes.*;

import com.amazonaws.auth.PropertiesCredentials;
import com.amazonaws.services.s3.AmazonS3;
import com.amazonaws.services.s3.AmazonS3Client;
import com.amazonaws.services.s3.model.PutObjectRequest;


// main class of the first assignment in DSPS course
 public class TweetAnaylizer{

	 public static PropertiesCredentials Credentials;
	 public static AmazonS3 S3;
	 public static String bucketName = "dsps_ass1";
	 public static String propertiesFilePath = "./ohadInfo.properties";
	 public static String fileToUploadPath = "../tweetLinks.txt";

 public static void mainOld(String[] args) throws FileNotFoundException,IOException, InterruptedException {
	 Credentials = new PropertiesCredentials(new FileInputStream(propertiesFilePath));
	 System.out.println("Credentials created.");
	 
	 S3 = new AmazonS3Client(Credentials);
	 System.out.println("AmazonS3Client created.");
	 
	 // If the bucket doesnt exist - will create it.
	 // Notice - this will create it in the default region :Region.US_Standard
	 if (!S3.doesBucketExist(bucketName)) {
		 S3.createBucket(bucketName);
	 }
	 System.out.println("Bucket exist.");
	 
	 File f = new File(fileToUploadPath);
	 PutObjectRequest por = new PutObjectRequest(bucketName, f.getName(),f);
	 
	 // Upload the file
	 S3.putObject(por);
	 System.out.println("File uploaded.");
 }
 
public static void main(String[] args) throws FileNotFoundException, IOException, InterruptedException {
	
		// creating credentials
		AWSCredentials credentials = new PropertiesCredentials(
				TweetAnaylizer.class.getResourceAsStream("AwsCredentials.properties"));
		//Credentials = new PropertiesCredentials(new FileInputStream(propertiesFilePath));
 		System.out.println("Credentials created.");
 		
 		// uploading the input file to S3
 		S3 = new AmazonS3Client(credentials);
 		if (!S3.doesBucketExist(bucketName)) {		// If the bucket doesnt exist - will create it.
 			S3.createBucket(bucketName);			// Notice - this will create it in the default region :Region.US_Standard
 		} 
 		File f = new File(fileToUploadPath);
 		PutObjectRequest por = new PutObjectRequest(bucketName, f.getName(),f);
 		S3.putObject(por);
 		System.out.println("Input file uploaded!");
 		
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