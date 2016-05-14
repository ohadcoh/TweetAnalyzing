package remote;

import java.io.BufferedReader;
import java.io.File;
import java.io.FileInputStream;
import java.io.FileNotFoundException;
import java.io.FileReader;
import java.io.FileWriter;
import java.io.IOException;
import java.io.LineNumberReader;
import java.io.PrintWriter;
import java.io.RandomAccessFile;
import java.io.Writer;
import java.util.List;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.concurrent.atomic.AtomicInteger;

import org.joda.time.LocalDateTime;

import com.amazonaws.auth.AWSCredentials;
import com.amazonaws.auth.PropertiesCredentials;
import com.amazonaws.services.sqs.model.Message;

import aws.EC2;
import aws.S3;
import aws.SQS;

public class Manager implements Runnable{
	// AWS
	private AWSCredentials credentials;
	private SQS localToManager;
	private SQS managerToLocal;
	private SQS managerToWorker;
	private SQS workerToManager;
	private S3 s3;
	private EC2 ec2;
	// internal info
	private AtomicInteger gNumOfWorkers;
	private AtomicInteger numberOfOpenTasks;
	// control booleans
	private AtomicBoolean gTerminate;
	private String idOfTerminateRequester;
	// variable for threads
	ExecutorService newTasksExecutor;
	private Runnable readFromWorkers;
	
	public static void main(String[] args)
	{
		// hard coded names
		String propertiesFilePath 				= "./dspsass1.properties";
		String localToManagerSQSQueueName		= "localToManagerasafohad";
		String managerToLocalSQSQueueName		= "managerToLocalasafohad";
		String s3BucketName						= "dspsass1bucketasafohad";
		String managerToWorkerSQSQueueName		= "managerToWorkerasafohad";
		String workerToManagerSQSQueueName		= "workerToManagerasafohad";
		// create manager instance
		Manager myManager = new Manager(propertiesFilePath,
										localToManagerSQSQueueName,
										managerToLocalSQSQueueName,
										s3BucketName,
										managerToWorkerSQSQueueName,
										workerToManagerSQSQueueName);
		// run manager
		myManager.run();
		// terminate manager
	}
	
	// constructor
	public Manager( String propertiesFilePath,
					String localToManagerSQSQueueName,
					String managerToLocalSQSQueueName,
					String s3BucketName,
					String managerToWorkerSQSQueueName,
					String workerToManagerSQSQueueName){
		// 1. creating credentials- exception: only return, no queue for informing local app
		try {
			credentials = new PropertiesCredentials(new FileInputStream(propertiesFilePath));
		} catch (FileNotFoundException e) {
			e.printStackTrace();
			return;
		} catch (IOException e) {
			e.printStackTrace();
			return;
		}
		// 2. creating all AWS
		localToManager 	= new SQS(credentials, localToManagerSQSQueueName);
		managerToLocal 	= new SQS(credentials, managerToLocalSQSQueueName);
		managerToWorker = new SQS(credentials, managerToWorkerSQSQueueName);
		workerToManager = new SQS(credentials, workerToManagerSQSQueueName);
		s3 				= new S3(credentials, s3BucketName);
		ec2				= new EC2(credentials);
		// 3. init internal info
		numberOfOpenTasks.set(0);
		gNumOfWorkers.set(0);
		gTerminate.set(false); // on init no terminate
		readFromWorkers = new Runnable() { // init thread for reading input from worker
            public void run() {
                Manager.this.readFromWorkers();
            }
            
        };
        
        newTasksExecutor = Executors.newFixedThreadPool(5);//creating a pool of 5 threads
	}

	protected void addNewTask(Message inputMessage) {
		// 3. if not terminate- request for new task
		int tempN = Integer.parseInt(inputMessage.getMessageAttributes().get("numOfWorkers").getStringValue());
		String tempId = inputMessage.getMessageAttributes().get("id").getStringValue();
		long tempTaskCounter = parseNewTask(inputMessage, tempId);
		
		int neededWorkers = (int) (tempTaskCounter/tempN - gNumOfWorkers.get());
		// if need to launch more workers 
		System.out.println("Manager: Current Msg Counter = " + tempTaskCounter);
		System.out.println("Manager: N = " + tempN);
		System.out.println("Manager: Creating " + neededWorkers + " workers.");
		if(neededWorkers > 0)
		{
			ec2.startWorkerInstances(neededWorkers);
		}
		
		addLinesToWorkersQueue(tempId);		
	}

	// run the manager
	public void run() {
		// wrap all actions
		try {
			// create and run thread for reading messages from workers
			Thread workersThread = new Thread(readFromWorkers);
			workersThread.start();
			
			// this thread will read from local applications queue
			readFromLocalApps();
			
			// wait for other thread to finish
			try {
				workersThread.join();
			} catch (InterruptedException e) {
				e.printStackTrace();
			}
		} catch (Exception e) {
			e.printStackTrace();
		}
		
		try {
			terminate();
		} catch (Exception e) {
			e.printStackTrace();
			managerToLocal.sendMessageWithId("I crushed!", idOfTerminateRequester);
		}
		
		return;
	}

	// handle queue from local applications
	private int readFromLocalApps(){
		while(!gTerminate.get())
		{
			// 1. read input message (keep reading until receive request)
			List<Message> inputMessageList;
			do{
				inputMessageList = localToManager.getMessages(1);
				try {
					Thread.sleep(500);
				} catch (InterruptedException e) {
					e.printStackTrace();
				}
			}while(inputMessageList.size() == 0);
			
			final Message	inputMessage = inputMessageList.get(0);
			System.out.println("Manager: received: " + inputMessageList.get(0).getBody());
			localToManager.deleteMessage(inputMessage);
			
			// 2. check if it is termination message
			if(inputMessage.getMessageAttributes().get("terminate") != null)
			{
				System.out.println("Manager received termination request");
				gTerminate.set(true);
				idOfTerminateRequester = inputMessage.getMessageAttributes().get("id").getStringValue();
				continue;
			}
			
			gNumOfWorkers.set(ec2.countNumOfWorkers());
			Runnable addNewTask		= new Runnable(){ // init thread for adding new task
	            public void run() {
	            	Manager.this.addNewTask(inputMessage);
	            }
	        };
	        System.out.println("New task thread");
	        newTasksExecutor.execute(addNewTask);
			
			
		}
		newTasksExecutor.shutdown();  
        while (!newTasksExecutor.isTerminated()) {   }
        System.out.println("Finished all new task threads");  

		return 0;

	}
	
	private void addLinesToWorkersQueue(String taskId){
		File localFile = new File("./" + taskId + "InputFile.txt");
	    try {
	    	BufferedReader reader = new BufferedReader(new FileReader(localFile));
			for(String line; (line = reader.readLine()) != null; ) {
				managerToWorker.sendMessageWithId(line, taskId);
			    // process the line.
			}
			reader.close();
		} catch (IOException e) {
			e.printStackTrace();
		}
	}
	
	// add task to the manager
	private long parseNewTask(Message inputMessage, String taskId)
	{
		// download file
		File localFile = new File("./" + taskId + "InputFile.txt");
		s3.downloadAndSaveFile(localFile, inputMessage.getBody());
		s3.deleteFile(inputMessage.getBody());
		
		//count lines
		long numOfLines = 0;
		try {
			LineNumberReader lnr = new LineNumberReader(new FileReader(localFile));
			lnr.skip(Long.MAX_VALUE);
			numOfLines = lnr.getLineNumber();
			lnr.close();
		} catch (IOException e2) {
			e2.printStackTrace();
		}
		
		System.out.println("Num of lines in file: " + numOfLines);
		
		// create file with the id name, 
		File tempFile = new File("./" + taskId + "OutputFile.txt"); 
		
		try {
			// create new output file
			tempFile.createNewFile();
			// read how many lines in the original file
			Writer writer = new FileWriter(tempFile);
			// write counter
			writer.write(String.valueOf(numOfLines) + "\n");
			// close writer
			writer.close();
		} catch (IOException e4) {
			e4.printStackTrace();
			return -1;
		}

    	// update the numOfLines
    	numberOfOpenTasks.incrementAndGet();
		return numOfLines;
	}
	
	// terminate manager
	private void terminate(){
		// close all workers and wait for them to finish properly
		//int waitsCounter;
		gNumOfWorkers.set(ec2.countNumOfWorkers());
		System.out.println("manager termination, Num of workers: " + gNumOfWorkers);
		Writer writer= null;
		String statisticsFileName = "./" + LocalDateTime.now() + "_allWorkersStatistics.txt";
		File statisticsFile = new File(statisticsFileName);
		try {
			writer = new FileWriter(statisticsFile);
		} catch (IOException e1) {
			e1.printStackTrace();
		}
		gNumOfWorkers.set(ec2.countNumOfWorkers());
		do
		{
			// send termination message to workers
			managerToWorker.sendMessageWithIdAndTerminate("Worker- stop!", "0");
	 		System.out.println("Manager: " + gNumOfWorkers + " more to go!");
			try {
				Thread.sleep(1000);
			} catch (InterruptedException e1) {
				e1.printStackTrace();
			}
	 		gNumOfWorkers.set(ec2.countNumOfWorkers());
		}while((gNumOfWorkers.get()) != 0);
		// read statistics
		List<Message> statisticsMessageList = workerToManager.getMessages(1);
		while(statisticsMessageList.size() > 0)
		{
			try {
				writer.append(statisticsMessageList.get(0).getBody() + "\n");
			} catch (IOException e) {
				// TODO Auto-generated catch block
				e.printStackTrace();
			}
			System.out.println("message from worker: " + statisticsMessageList.get(0).getBody());
	 		workerToManager.deleteMessage(statisticsMessageList.get(0));
	 		statisticsMessageList = workerToManager.getMessages(1);
		}
		
		
		try {
			writer.close();
		} catch (IOException e) {
			e.printStackTrace();
		}
		S3 statisticsS3 = new S3(credentials, "workerststistics");
		statisticsS3.uploadFile(statisticsFileName);
    	statisticsFile.delete();
    	
 		// finish process
 		s3.deletebucket();

    	managerToWorker.deleteQueue();
    	workerToManager.deleteQueue();
    	// send message to local
    	managerToLocal.sendMessageWithId("I am finished", idOfTerminateRequester);
    	System.out.println("Manager: finished");
	}
    
	// read workers queue and parse the messages
	private void readFromWorkers() {
		int count = 0;
		// run until terminate request arrived ***AND*** all tasks were handled and finished
    	while ((numberOfOpenTasks.get() != 0) || (gTerminate.get() == false)) {
    		// read one message
    		List<Message>  messagesList = workerToManager.getMessages(1);
    		
    		// if no input messages- sleep a while and try again
    		if(messagesList.size() == 0)
    		{
    			try {
					Thread.sleep(300);
				} catch (InterruptedException e) {
					e.printStackTrace();
				}
    			if (++count == 100){
    				if (ec2.countNumOfWorkers() == 0){
    					ec2.startWorkerInstances(1);
    					count = 0;
    				}
    			}
    			continue;
    		}
    		
    		count = 0;
    		workerToManager.deleteMessage(messagesList.get(0));
    		
    		if (messagesList.get(0).getMessageAttributes().get("terminate") != null){
    			System.out.println(messagesList.get(0).getBody());
    			continue;
    		}
    		
    		String msgTaskId;
    		try {
    			msgTaskId = messagesList.get(0).getMessageAttributes().get("id").getStringValue();
    		} catch (NullPointerException e) {
    			e.printStackTrace();
    			continue;
    		}
    		int taskFileStatus = -1;
			taskFileStatus = addLineToFile(messagesList.get(0).getBody(), "./" + msgTaskId + "OutputFile.txt");
    		
    		// if -1- error
    		if(taskFileStatus == -1)
    		{
    			break;
    		}
    		// if 0 - task finished
    		else if(taskFileStatus == 0)
    		{
    			String outputFileS3key = s3.uploadFile("./" + msgTaskId + "OutputFile.txt");
    			File tempFile = new File("./" + msgTaskId + "OutputFile.txt");
    			tempFile.delete();
    			System.out.println("Task " + msgTaskId + ": output File Uploaded\n");
    			managerToLocal.sendMessageWithId(outputFileS3key, msgTaskId);
    			numberOfOpenTasks.decrementAndGet();
    		}
    		// if 1 - line added, do nothing

    	}
	}
	
	private String getAndRemoveLastLine( File file ) {
        RandomAccessFile fileHandler = null;
        try {
            fileHandler = new RandomAccessFile( file, "rw" );
            long fileLength = fileHandler.length() - 1;
            long filePointer;
            StringBuilder sb = new StringBuilder();

            for(filePointer = fileLength; filePointer != -1; filePointer--){
                fileHandler.seek( filePointer );
                int readByte = fileHandler.readByte();

                if( readByte == 0xA ) {
                    if( filePointer == fileLength ) {
                        continue;
                    }
                    break;

                } else if( readByte == 0xD ) {
                    if( filePointer == fileLength - 1 ) {
                        continue;
                    }
                    break;
                }

                sb.append( ( char ) readByte );
            }

            String lastLine = sb.reverse().toString();
            fileHandler.setLength(filePointer+1);
            return lastLine;
        } catch( java.io.FileNotFoundException e ) {
            e.printStackTrace();
            return null;
        } catch( java.io.IOException e ) {
            e.printStackTrace();
            return null;
        } finally {
            if (fileHandler != null )
                try {
                    fileHandler.close();
                } catch (IOException e) {
                    /* ignore */
                }
        }
    }
	
    private int addLineToFile(String line, String path){
    	File file = new File(path);
    	//Get Counter From File
    	String lastLine = getAndRemoveLastLine(file);
    	int counter = Integer.valueOf(lastLine);
    	counter--;
    	try {
			FileWriter writer = new FileWriter(file, true);
			PrintWriter out = new PrintWriter(writer);
			//Write Last Line
			out.append(line + "\n");
			if (counter == 0){
				out.close();
				return 0;
			} else {
				out.append(Integer.toString(counter));
				out.close();
				return 1;
			}
			
		} catch (IOException e) {
			e.printStackTrace();
			return -1;
		} 
    }
}
