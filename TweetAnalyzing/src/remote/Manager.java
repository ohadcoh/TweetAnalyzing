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

public class Manager {
	private SQS localToManager;
	private SQS managerToLocal;
	private SQS managerToWorker;
	private SQS workerToManager;
	private S3 s3;
	private int numOfWorkers;
	private Vector<Task> tasks;
	private AWSCredentials credentials;
	private boolean gTerminateInit;
	private boolean gTerminate;
	private final Runnable typeA;
	private boolean threadIsAlive;
	
	// for now it is constructor, but later it will be stand-alone class
	//public static void main(String[] args) throws FileNotFoundException, IOException, InterruptedException {
	public Manager(AWSCredentials _credentials, SQS _localToManager, SQS _managerToLocal, S3 _s3, int _numOfWorkers) throws FileNotFoundException, IOException {
		// parse all input argument to the needed variables
		
		super();
		credentials =_credentials;
		localToManager = _localToManager;
		managerToLocal = _managerToLocal;
		// 3. uploading the input file to S3
		s3 = _s3;
		// 4. assign number of worker for this manager
		numOfWorkers = _numOfWorkers;
		// 5. create queues ManagerToWorker and workerToManager
		managerToWorker = new SQS(credentials, "managertoworker" + UUID.randomUUID());
		workerToManager = new SQS(credentials, "workerToManager" + UUID.randomUUID());
		// on init no terminate
		gTerminateInit 	= false; // this variable is for no more new tasks
		gTerminate		= false; // this variable is for terminating manager
		// init thread for reading input from worker
		typeA = new Runnable() {
            public void run() {
                Manager.this.readInputFromWorkers();
            }
        };
        threadIsAlive = false;
		
		// 6. init tasks list
		tasks = new Vector<Task>();
		
	}

	protected void readInputFromWorkers() {
		// 1. update that this thread is alive
		threadIsAlive = true;
		// 2. check if last task finished- dont enter
    	while (!gTerminate) {
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
    		
    		// find the right task id, and let it handle the input message
    		String msgTaskId = messagesList.get(0).getMessageAttributes().get("id").getStringValue();
    		// iterate over all tasks
    		for(Task tempLoopTask: tasks){
    			// if ids are equal
    			if(tempLoopTask.getId().equals(msgTaskId))
    			{
    				// add this line to file in this task
    				tempLoopTask.addLineToFile(messagesList.get(0).getBody());
    				// if finished, delete this message, finish the task
    				if(tempLoopTask.getCounter() == 0){
    					workerToManager.deleteMessage(messagesList.get(0));
    					tempLoopTask.finishTask(s3);
    					// if needs to terminate, finish manager
    					if(tempLoopTask.getTerminate())
    						gTerminate = true;
    					// remove task from tasks list
    					tasks.remove(tempLoopTask);
    				}
    				break;
    			}
    		}

    	     workerToManager.deleteMessage(messagesList.get(0));
    	}
		
	}
	
	public int mainMethod() {
		
		addTasks();
		terminateManager();
		
		return 0;
	}
	
	// create task for this local application request, init it and add to list
	public int addTasks(){
		// if there wasn't terminate request yet-
		while(!gTerminateInit)
		{
			// init task
			Task tempTask = new Task(String.valueOf(tasks.size()), localToManager, managerToLocal);
			// start this task
			tempTask.startTask(s3, managerToWorker);
			// check if this task request termination- update first global
			if(tempTask.getTerminate())
				gTerminateInit = true;
			// add to tasks list
			tasks.add(tempTask);
			
			
	    	// start workers
	    	// requests from AWS- for now create one worker
	    	Worker worker1 = new Worker(credentials, managerToWorker, workerToManager);
	    	worker1.analyzeTweet();
	    	System.out.println("Worker Finishes!");

	    	// check if thread of reading input from worker is running
	    	if(!threadIsAlive)
	    		typeA.run();
		}
    	
		return 0;
	}
	
	public void terminateManager(){
 		// finish process
    	managerToWorker.deleteQueue();
    	workerToManager.deleteQueue();
	}
	
}
