package local;
import java.io.BufferedReader;
import java.io.File;
import java.io.FileInputStream;
import java.io.FileNotFoundException;
import java.io.InputStreamReader;
import java.io.PrintWriter;
//import java.util.UUID;

import java.util.ArrayList;
import java.util.Iterator;
import java.util.List;

import com.amazonaws.AmazonServiceException;
import com.amazonaws.auth.AWSCredentials;
import com.amazonaws.auth.PropertiesCredentials;
import com.amazonaws.services.ec2.AmazonEC2;
import com.amazonaws.services.ec2.AmazonEC2Client;
import com.amazonaws.services.ec2.model.CreateTagsRequest;
import com.amazonaws.services.ec2.model.Instance;
import com.amazonaws.services.ec2.model.InstanceType;
import com.amazonaws.services.ec2.model.Reservation;
import com.amazonaws.services.ec2.model.RunInstancesRequest;
import com.amazonaws.services.ec2.model.Tag;
import com.amazonaws.services.ec2.model.TerminateInstancesRequest;
import com.amazonaws.services.s3.model.S3Object;
import com.amazonaws.services.sqs.model.Message;

import aws.S3;
import aws.SQS;
import remote.Manager;

// local application- first assignment in DSPS course
 public class TweetAnaylizer{

	private static String bucketName = "dspsass1bucketasafohad";
	private static String localToManagerQueueName = "localtomanagerasafohad";
	private static String managerToLocalQueueName = "managerToLocalasafohad";

	private static String propertiesFilePath = "./ohadInfo.properties";
	private static AmazonEC2 ec2;
	//private static String id;

    public static boolean checkIfManagerExist() {
    	
        Iterator<Reservation> vReservations = ec2.describeInstances()
                .getReservations().iterator();
        Instance managerInstance = null;
        //Step through all the reservations...
        Reservation vResItem = null;
        while (vReservations.hasNext()) {
            //For each reservation, get the instances
            vResItem = vReservations.next();
            Iterator<Instance> vInstances = vResItem.getInstances().iterator();
            //For each instance, get the tags associated with it.
            while (vInstances.hasNext()) {
                Instance vInstanceItem = vInstances.next();
                List<Tag> pTags = vInstanceItem.getTags();
                Iterator<Tag> vIt = pTags.iterator();
                while (vIt.hasNext()) {
                    Tag item = vIt.next();
                    //if the tag key macthes and the value we're looking for, we return
                    if (item.getKey().equals("Name") && item.getValue().equals("Manager")) {
                    	managerInstance= vInstanceItem;
                    }
                }
            }
        }
        if(managerInstance != null)
        {
        	System.out.println("Manager extist: " + managerInstance.getInstanceId());
        	return true;
        }
        else
        {
        	System.out.println("Manager not extist");
        	return false;
        }
    }

    public static void launchManager(AWSCredentials credentials) throws Exception {    
        try {
            // Basic 64-bit Amazon Linux AMI (AMI Id: ami-08111162)
        	// create request for manager
            RunInstancesRequest request = new RunInstancesRequest("ami-08111162", 1, 1);
            request.setInstanceType(InstanceType.T2Micro.toString());
            // run instance
            List<Instance> instances = ec2.runInstances(request).getReservation().getInstances();
            // add 'manager' tag
            CreateTagsRequest createTagsRequest = new CreateTagsRequest().
            		withResources(instances.get(0).getInstanceId()).withTags(new Tag("Name", "Manager"));
            ec2.createTags(createTagsRequest);
            System.out.println("LocalApp: Launch instances: " + instances.get(0).getInstanceId());
            // temporary terminate
            TerminateInstancesRequest termintateManagerRequest = 
            		new TerminateInstancesRequest().withInstanceIds(instances.get(0).getInstanceId());
            ec2.terminateInstances(termintateManagerRequest);
            System.out.println("LocalApp: Terminate instances: " + instances.get(0).getInstanceId());
 
        } catch (AmazonServiceException ase) {
            System.out.println("Caught Exception: " + ase.getMessage());
            System.out.println("Reponse Status Code: " + ase.getStatusCode());
            System.out.println("Error Code: " + ase.getErrorCode());
            System.out.println("Request ID: " + ase.getRequestId());
        }

    }
	 
    public static void main(String[] args) throws Exception {
    	// 0. parse input arguments
    	int argsNum = args.length;
    	if(argsNum != 3 && argsNum != 4 )
    	{
    		System.out.println("LocalApp: Usage: inputFileName outputFileName n terminate(optional)");
    		return;
    	}

    	boolean terminate = false;
		String inputFileName = args[0];
		String outputFileName = args[1];
		int numOfWorkers = Integer.parseInt(args[2]);
		if(argsNum == 4)
			terminate = true;
		
		// 1. creating credentials and ec2 client
		AWSCredentials credentials = new PropertiesCredentials(new FileInputStream(propertiesFilePath));
 		System.out.println("LocalApp: Credentials created.");
 	 	ec2 = new AmazonEC2Client(credentials);
 		
 		// 2. uploading the input file to S3
 		S3 s3 = new S3(credentials, bucketName);
 		String inputFileS3key = s3.uploadFile(inputFileName);
 		System.out.println("LocalApp: File Uploaded\n");
 		
 		// 3. create queue (from local app to manager) and send message with the key of the file
 		SQS localToManager = new SQS(credentials, localToManagerQueueName);
 		SQS managerToLocal = new SQS(credentials, managerToLocalQueueName);
 		
 		// 4. send with attributes (terminate and numOfWorkers)
 		localToManager.sendMessageWithNumOfWorkers(inputFileS3key, String.valueOf(numOfWorkers));
 		
		// 5. find if there is manager instance
		if (!checkIfManagerExist())
		{
			//will be added when we know how to bootstrap
			launchManager(credentials);
		}
		
 		// for now - create manager
 		Runnable manager1 = new Manager(credentials, localToManager, managerToLocal, s3);
 		new Thread(manager1).start();
 		//manager1.run();
 		
 		// 5. read your id
 		try {
			Thread.sleep(1000);
		} catch (InterruptedException e) {
			e.printStackTrace();
		}
 		System.out.println("LocalApp: after first sleep");
 		List<Message> inputMessageList;
		do{
			inputMessageList = managerToLocal.getMessages(1);
			try {
				Thread.sleep(500);
			} catch (InterruptedException e) {
				e.printStackTrace();
			}
		}while(inputMessageList.size() == 0);
		Message	idMessages = inputMessageList.get(0);
 		String body = idMessages.getBody();
 		String id = idMessages.getMessageAttributes().get("id").getStringValue();
 		System.out.println("LocalApp: Body: " + body + "id: " + id);
 		managerToLocal.deleteMessage(idMessages);


 		
 		// 6. read message
 		// read until receive answer from manager
 		List<Message> messageFromManagerList;
 		do{
 			messageFromManagerList = managerToLocal.getMessagesMinimalVisibilityTime(1);
 			try {
				Thread.sleep(500);
			} catch (InterruptedException e) {
				e.printStackTrace();
			}
 			//if(messageFromManagerList.get(0).getMessageAttributes().get("id").getStringValue().equals(id))
 		}while(messageFromManagerList.size() == 0 );
 		Message messageFromManager = messageFromManagerList.get(0);
 		managerToLocal.deleteMessage(messageFromManagerList.get(0));
        System.out.println("LocalApp: Message from Manager with my id: " + messageFromManager.getBody());
 		
        // 7. download the output file, write it to HTML and delete it        
        S3Object outputFile = s3.downloadFile(messageFromManager.getBody());

        BufferedReader reader = new BufferedReader(new InputStreamReader(outputFile.getObjectContent()));

        ArrayList<String> allOutputLines = new ArrayList<String>(); 

    	while (true) {          
    	     String line = reader.readLine();           
    	     if (line == null)
    	          break;            
    	     allOutputLines.add(line);

    	}
    	reader.close();
    	createHTMLFile(allOutputLines, outputFileName+".html");
    	
    	// send termination message to manager
    	if(terminate)
    	{
    		localToManager.sendMessageWithIdAndTerminate("I am terminating you!", id);
    	}
 		try {
			Thread.sleep(2000);
		} catch (InterruptedException e) {
			e.printStackTrace();
		}
        s3.deleteFile(messageFromManager.getBody());
        s3.deletebucket();
        // need to delete s3!!!
        localToManager.deleteQueue();
        managerToLocal.deleteQueue();
        // need to delete clients!!
 		
		System.out.println("LocalApp: Bye bye");
	}
    
 

	private static void createHTMLFile(ArrayList<String> allLines,
			String outputFilePath) {
		
		String output = "<HTML>\n<HEAD>\n</HEAD>\n<BODY>\n";
		for (String line : allLines) {
			//System.out.println("Line: " + line);
			String sentiment  	= line.substring(0, line.indexOf(';'));
			String entitiesAndTweet = line.substring(line.indexOf(';')+1, line.length());
			String entities 	= entitiesAndTweet.substring(0, entitiesAndTweet.indexOf(';'));
			String tweet 		= entitiesAndTweet.substring(entitiesAndTweet.indexOf(';')+1, entitiesAndTweet.length());
			String rawTweet		= tweet.substring(tweet.indexOf('"')+1,tweet.lastIndexOf('"'));
            output += "<p> <b><font color=\"" + sentiment + "\"> " + rawTweet + "</font></b> " + entities + "</p>\n";
		}
		output += "</BODY>\n</HTML>";
		
		if ( null == outputFilePath ) {
			System.out.println(output);
		} else {
			try {
				File file = new File (outputFilePath);
				PrintWriter out = new PrintWriter(file);
				out.println(output);
				out.close();
				System.out.println("LocalApp: Write html file to: "+ outputFilePath);
			} catch (FileNotFoundException e) {
				System.out.println("LocalApp: Failed to write to file: "+ outputFilePath);
				System.out.println(output);
			}
		}
		
	}
 }