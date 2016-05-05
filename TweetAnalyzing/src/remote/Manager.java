package remote;

import java.io.BufferedInputStream;
import java.io.BufferedReader;
import java.io.File;
import java.io.FileInputStream;
import java.io.FileNotFoundException;
import java.io.FileWriter;
import java.io.IOException;
import java.io.InputStream;
import java.io.InputStreamReader;
import java.io.PrintWriter;
import java.io.RandomAccessFile;
import java.io.Writer;
import java.util.List;

import com.amazonaws.auth.AWSCredentials;
import com.amazonaws.auth.PropertiesCredentials;
import com.amazonaws.services.s3.model.S3Object;
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
	private int gNumOfWorkers;
	private int numberOfOpenTasks;
	// control booleans
	private boolean gTerminate;
	private String idOfTerminateRequester;
	// variable for threads
	private Runnable readInputFromWorkers;
	
	public static void main(String[] args)
	{
		// hard coded names
		String propertiesFilePath 				= "./ohadInfo.properties";
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
		// 1. creating credentials
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
		// 4. init internal info
		numberOfOpenTasks = 0;
		gNumOfWorkers 	= 0;
		gTerminate		= false; // on init no terminate
		readInputFromWorkers = new Runnable() { // init thread for reading input from worker
            public void run() {
                Manager.this.readFromWorkers();
            }
        };
	}

	// run the manager
	public void run() {
		// create and run thread for reading messages from workers
		Thread workersThread = new Thread(readInputFromWorkers);
		workersThread.start();
		
		// this thread will read from local applications queue
		readFromLocalApps();
		
		// wait for other thread to finish
		try {
			workersThread.join();
		} catch (InterruptedException e) {
			e.printStackTrace();
		}
		
		terminate();
		return;
	}

	// handle queue from local applications
	private int readFromLocalApps(){
		while(!gTerminate)
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
			Message	inputMessage = inputMessageList.get(0);
			System.out.println("Manager: received: " + inputMessageList.get(0).getBody());
			localToManager.deleteMessage(inputMessage);
			// 2. check if it is termination message
			if(inputMessage.getMessageAttributes().get("terminate") != null)
			{
				System.out.println("Manager received termination request");
				gTerminate = true;
				idOfTerminateRequester = inputMessage.getMessageAttributes().get("id").getStringValue();
				continue;
			}
			// 3. if not terminate- request for new task
			int tempN = Integer.parseInt(inputMessage.getMessageAttributes().get("numOfWorkers").getStringValue());
			String tempId = inputMessage.getMessageAttributes().get("id").getStringValue();
			Long tempTaskCounter = parseNewTask(inputMessage, tempId);
			int neededWorkers = (int) (tempTaskCounter/tempN - gNumOfWorkers);
			// if need to launch more workers 
			if(neededWorkers > 0)
			{
				System.out.println("Manager: Creating " + neededWorkers + " workers.");
				//ec2.startWorkerInstances(neededWorkers);
				gNumOfWorkers += neededWorkers;
			}

//	    	// start workers
	    	// requests from AWS- for now create one worker
			String propertiesFilePath 		= "./ohadInfo.properties";
			String inputSQSQueueName		= "managerToWorkerasafohad";
			String outputSQSQueueName		= "workerToManagerasafohad";
			String statisticsBucketName 	= "workersstatisticsasafohad";
	    	Worker worker = new Worker(propertiesFilePath, inputSQSQueueName, outputSQSQueueName, statisticsBucketName);
	    	new Thread(worker).start();
			
		}
		// close all workers...
		return 0;

	}
	
	// add task to the manager
	private long parseNewTask(Message inputMessage, String taskId)
	{
		// download file
		S3Object inputFile = s3.downloadFile(inputMessage.getBody());
		s3.deleteFile(inputMessage.getBody());
		// create file with the id name, 
		File tempFile = new File("./" + taskId + "OutputFile.txt"); 
		long numOfLines = 0;
		try {
			// create new output file
			tempFile.createNewFile();
			// read how many lines in the original file
			numOfLines = 5;//= countLines(inputFile);
			Writer writer = new FileWriter(tempFile);
			// write counter
			writer.write(String.valueOf(numOfLines) + "\n");
			// close writer
			writer.close();
		} catch (IOException e4) {
			e4.printStackTrace();
		}

		// add all lines to workers queue
		BufferedReader reader = new BufferedReader(new InputStreamReader(inputFile.getObjectContent()));
    	while (true) {          
    		String line = "";
			try {
				line = reader.readLine();
			} catch (IOException e) {
				e.printStackTrace();
				return (long) -1;
			}           
    		if (line == null || line.length() == 0)
    			break;
    		// send with id attribute
    		managerToWorker.sendMessageWithId(line, taskId);
    	}
    	try {
			reader.close();
		} catch (IOException e1) {
			e1.printStackTrace();
			return (long) -1;
		}
    	numberOfOpenTasks++;
		return numOfLines;
	}
	
	// terminate manager
	private void terminate(){
		// close all workers and wait for them to finish properly 
		
 		// finish process
    	managerToWorker.deleteQueue();
    	workerToManager.deleteQueue();
    	// send message to local
    	managerToLocal.sendMessageWithId("I am finished", idOfTerminateRequester);
    	System.out.println("Manager: finished");
	}
    
    public long countLines(S3Object inputFile) throws IOException {
    	// was taken from stack overflow
        InputStream is = new BufferedInputStream(inputFile.getObjectContent());
        try {
            byte[] c = new byte[1024];
            long count = 0;
            int readChars = 0;
            boolean empty = true;
            while ((readChars = is.read(c)) != -1) {
                empty = false;
                for (int i = 0; i < readChars; ++i) {
                    if (c[i] == '\n') {
                        ++count;
                    }
                }
            }
            return (count == 0 && !empty) ? 1 : count;
        } finally {
            is.close();
        }
    }
    
	// read workers queue and parse the messages
	private void readFromWorkers() {
		// run until terminate request arrived ***AND*** all tasks were handled and finished
    	while ((numberOfOpenTasks != 0) || (gTerminate == false)) {
    		// read one message
    		List<Message>  messagesList = workerToManager.getMessages(1);
    		// if no input messages- sleep a while and try again
    		if(messagesList.size() == 0)
    		{
    			try {
					Thread.sleep(300);
				} catch (InterruptedException e) {
					// TODO Auto-generated catch block
					e.printStackTrace();
				}
    			continue;
    		}
    		// check if this 'worker finished' message
    		if(messagesList.get(0).getMessageAttributes().get("workerId") != null)
    		{
    			gNumOfWorkers--;
    			System.out.println("Manager: received end message from worker " + messagesList.get(0).getBody());
    			workerToManager.deleteMessage(messagesList.get(0));
    			continue;
    		}

    		String msgTaskId = messagesList.get(0).getMessageAttributes().get("id").getStringValue();
    		int taskFileStatus = addLineToFile(messagesList.get(0).getBody(), "./" + msgTaskId + "OutputFile.txt");
    		workerToManager.deleteMessage(messagesList.get(0));
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
    			numberOfOpenTasks--;
    		}
    		// if 1 - line added, do nothing

    	}
    	managerToWorker.sendMessageWithIdAndTerminate("Worker- stop!", "0");
    	List<Message> messageFromManagerList;
 		do{
 			messageFromManagerList = workerToManager.getMessagesMinimalVisibilityTime(1);
 			if( messageFromManagerList.size() == 1)
 					break;
 			try {
				Thread.sleep(500);
			} catch (InterruptedException e) {
				e.printStackTrace();
			}
 		}while(messageFromManagerList.size() == 0 );
 		System.out.println("Manager: Worker finished!: " + messageFromManagerList.get(0).getBody());
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
    	//System.out.println(counter);
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
