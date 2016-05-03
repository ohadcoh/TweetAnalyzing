package remote;

import java.io.FileNotFoundException;
import java.io.IOException;
import java.util.List;
import java.util.UUID;
import java.util.Vector;

import com.amazonaws.auth.AWSCredentials;
import com.amazonaws.services.sqs.model.Message;

import aws.S3;
import aws.SQS;

public class Manager implements Runnable{
	private AWSCredentials credentials;
	private SQS localToManager;
	private SQS managerToLocal;
	private SQS managerToWorker;
	private SQS workerToManager;
	
	private S3 s3;
	
	private long gCounter;
	private int gNumOfWorkers;
	private Vector<Task> tasks;
	// control booleans
	private boolean gTerminate;
	// variable for 
	private Runnable readInputFromWorkers;
	private boolean threadIsAlive;
	
	// constructor
	public Manager(AWSCredentials _credentials, SQS _localToManager, SQS _managerToLocal, S3 _s3) throws FileNotFoundException, IOException {
		// parse all input argument to the needed variables
		super();
		credentials =_credentials;
		localToManager = _localToManager;
		managerToLocal = _managerToLocal;
		// 3. uploading the input file to S3
		s3 = _s3;
		// 4. assign number of worker for this manager
		gNumOfWorkers 	= 0;
		gCounter		= 0;
		// 5. create queues ManagerToWorker and workerToManager
		managerToWorker = new SQS(credentials, "managertoworker" + UUID.randomUUID());
		workerToManager = new SQS(credentials, "workerToManager" + UUID.randomUUID());
		// on init no terminate
		gTerminate		= false; // this variable is for terminating manager
		// init thread for reading input from worker
		readInputFromWorkers = new Runnable() {
            public void run() {
                Manager.this.readInputFromWorkers();
            }
        };
        threadIsAlive = false;
		
		// 6. init tasks list
		tasks = new Vector<Task>();	
	}

	// run the manager
	public void run() {
		// this methods also creates thread for reading from workers
		readFromLocalApps();
		terminateManager();
		
		return;
	}
	
	
	protected void readInputFromWorkers() {
		// 1. update that this thread is alive
		threadIsAlive = true;
		// 2. check if last task finished- dont enter
    	while ((tasks.size() != 0) && (gTerminate == false)) {
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

    		// find the right task id, and let it handle the input message
    		String msgTaskId = messagesList.get(0).getMessageAttributes().get("id").getStringValue();
    		// iterate over all tasks
    		for(Task tempLoopTask: tasks){
    			// if ids are equal
    			if(tempLoopTask.getId().equals(msgTaskId))
    			{
    	    		// empty body
    	    		if(messagesList.get(0).getBody() == "")
    	    		{
    	    			tempLoopTask.decrementRemainingCounter();
    	    			break;
    	    		}
    				// add this line to file in this task
    				tempLoopTask.addLineToFile(messagesList.get(0).getBody());
    				// if finished, delete this message, finish the task
    				if(tempLoopTask.getRemainingCounter() == 0){
    					//workerToManager.deleteMessage(messagesList.get(0));
    					tempLoopTask.finishTask(s3);
    					// remove task from tasks list
    					tasks.remove(tempLoopTask);
    					System.out.println("Manager: tasks size: " + tasks.size());
    				}
    				break;
    			}
    		}

    	    workerToManager.deleteMessage(messagesList.get(0));
    	}
    	threadIsAlive = false;
		
	}
	
	// create task for this local application request, init it and add to list
	private int readFromLocalApps(){
//    	// start workers
//    	// requests from AWS- for now create one worker
//    	Worker worker1 = new Worker(credentials, managerToWorker, workerToManager, 1);
//    	worker1.analyzeTweet();
//    	System.out.println("Manager: After Worker Finishes!");
//
//    	// check if thread of reading input from worker is running
//    	if(!threadIsAlive)
//    		readInputFromWorkers.run();
//		// if there wasn't terminate request yet-
		while(true)
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
			System.out.println("Manager: received: " + inputMessageList.get(0).getBody());
			localToManager.deleteMessage(inputMessage);
			// check if it is termination message
			if(inputMessage.getMessageAttributes().get("terminate") != null)
			{
				// terminate
				gTerminate = true;
				return 0;
			}
			
			Task tempTask = addNewTask(inputMessage);
			// no running workers 
			if(gNumOfWorkers == 0)
			{
				System.out.println("Manager: Creating " + gCounter/tempTask.getN() + " workers.");
				gNumOfWorkers = (int) (gCounter/tempTask.getN());
				// run workers: gCounter/tempTask.getN() = numOfWorkersToRun
			}
			else if(tempTask.getCounter()/tempTask.getN() > gNumOfWorkers)
			{
				System.out.println("Manager: Creating " + (int)(gCounter/tempTask.getN() - gNumOfWorkers) + " workers.");
				gNumOfWorkers = (int) (tempTask.getCounter()/tempTask.getN() - gNumOfWorkers);
				// run workers: gCounter/tempTask.getN() - gNumOfWorkers
			}
	    	// start workers
	    	// requests from AWS- for now create one worker
	    	Worker worker11 = new Worker(credentials, managerToWorker, workerToManager, 1);
	    	worker11.analyzeTweet();
	    	System.out.println("Worker Finishes!");

	    	// check if thread of reading input from worker is running
	    	if(!threadIsAlive)
	    		readInputFromWorkers.run();
			
		}

	}
	
	private Task addNewTask(Message inputMessage)
	{
		// init task
		Task tempTask = new Task(String.valueOf(tasks.size()), localToManager, managerToLocal);
		// start this task
		tempTask.startTask(s3, managerToWorker, inputMessage);
		gCounter += tempTask.getCounter();

		// add to tasks list
		tasks.add(tempTask);
		return tempTask;
	}
	
	public void terminateManager(){
		// close all workers and wait for them to finish properly 
		
 		// finish process
    	managerToWorker.deleteQueue();
    	workerToManager.deleteQueue();
    	System.out.println("Manager: finished");
	}


	public void init() {
		// 1.1 read input message, send to local its queue (keep reading until receive request)
		// for local use- read first app and then send it its id
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

		
		Task tempTask = addNewTask(inputMessage);
		// no running workers 
		if(gNumOfWorkers == 0)
		{
			System.out.println("Manager: Creating " + gCounter/tempTask.getN() + " workers.");
			gNumOfWorkers = (int) (gCounter/tempTask.getN());
			// run workers: gCounter/tempTask.getN() = numOfWorkersToRun
		}
		else if(tempTask.getCounter()/tempTask.getN() > gNumOfWorkers)
		{
			System.out.println("Manager: Creating " + (int)(gCounter/tempTask.getN() - gNumOfWorkers) + " workers.");
			gNumOfWorkers = (int) (tempTask.getCounter()/tempTask.getN() - gNumOfWorkers);
			// run workers: gCounter/tempTask.getN() - gNumOfWorkers
		}
	}
	
}
