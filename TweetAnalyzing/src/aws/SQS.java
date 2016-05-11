package aws;

import java.util.List;

import com.amazonaws.AmazonClientException;
import com.amazonaws.AmazonServiceException;
import com.amazonaws.auth.AWSCredentials;
import com.amazonaws.services.sqs.AmazonSQS;
import com.amazonaws.services.sqs.AmazonSQSClient;
import com.amazonaws.services.sqs.model.CreateQueueRequest;
import com.amazonaws.services.sqs.model.DeleteMessageRequest;
import com.amazonaws.services.sqs.model.DeleteQueueRequest;
import com.amazonaws.services.sqs.model.Message;
import com.amazonaws.services.sqs.model.MessageAttributeValue;
import com.amazonaws.services.sqs.model.QueueDeletedRecentlyException;
import com.amazonaws.services.sqs.model.QueueNameExistsException;
import com.amazonaws.services.sqs.model.ReceiveMessageRequest;
import com.amazonaws.services.sqs.model.SendMessageRequest;

public class SQS {
	
	private AmazonSQS sqs;
	private String url;
	private String queueName;
	
	
	public SQS(AWSCredentials credentials, String _queueName) {
		super();
		this.sqs = new AmazonSQSClient(credentials);
		this.queueName = _queueName;
		
		try{
	        System.out.println("Creating a new SQS queue called " + queueName + "\n");
	        CreateQueueRequest createQueueRequest = new CreateQueueRequest(queueName);
	        this.url = sqs.createQueue(createQueueRequest).getQueueUrl();
	        
		} catch (QueueDeletedRecentlyException e) {
			System.out.println("Queue" + _queueName + "was recently deleted. Waiting 60 Seconds...");
			try {
				Thread.sleep(60000);
			} catch (InterruptedException e1) {
				e1.printStackTrace();
			}
	        CreateQueueRequest createQueueRequest = new CreateQueueRequest(queueName);
	        this.url = sqs.createQueue(createQueueRequest).getQueueUrl();
	        
    	} catch (QueueNameExistsException e){
    		this.url = sqs.getQueueUrl(_queueName).getQueueUrl();
    	}
	}

	public int sendMessageWithNumOfWorkersAndId(String message, String numOfWorkers, String id){
		try{
	        //System.out.println("Sending message with num of workers.\n");
	        MessageAttributeValue numOfWorkersValue = new MessageAttributeValue().withDataType("String").withStringValue(numOfWorkers);
	        MessageAttributeValue idValue = new MessageAttributeValue().withDataType("String").withStringValue(id);

	        SendMessageRequest messageRequest = new SendMessageRequest(url, message).
	        		addMessageAttributesEntry("numOfWorkers", numOfWorkersValue).
	        		addMessageAttributesEntry("id", idValue);
	        sqs.sendMessage(messageRequest);
	        return 0;
		} catch (AmazonServiceException ase) {
	        System.out.println("Caught an AmazonServiceException, which means your request made it " +
	                "to Amazon SQS, but was rejected with an error response for some reason.");
	        System.out.println("Error Message:    " + ase.getMessage());
	        System.out.println("HTTP Status Code: " + ase.getStatusCode());
	        System.out.println("AWS Error Code:   " + ase.getErrorCode());
	        System.out.println("Error Type:       " + ase.getErrorType());
	        System.out.println("Request ID:       " + ase.getRequestId());
	        return 1;
	    	} catch (AmazonClientException ace) {
	        System.out.println("Caught an AmazonClientException, which means the client encountered " +
	                "a serious internal problem while trying to communicate with SQS, such as not " +
	                "being able to access the network.");
	        System.out.println("Error Message: " + ace.getMessage());
	        return 2;
    	}
	}
	
	public int sendMessageWithId(String message, String id){
		try{
	        //System.out.println("Sending message with id.\n");
	        MessageAttributeValue idValue 	= new MessageAttributeValue().withDataType("String").withStringValue(id);
	        SendMessageRequest messageRequest = new SendMessageRequest(url, message).
	        		addMessageAttributesEntry("id", idValue);
	        sqs.sendMessage(messageRequest);
	        return 0;
		} catch (AmazonServiceException ase) {
	        System.out.println("Caught an AmazonServiceException, which means your request made it " +
	                "to Amazon SQS, but was rejected with an error response for some reason.");
	        System.out.println("Error Message:    " + ase.getMessage());
	        System.out.println("HTTP Status Code: " + ase.getStatusCode());
	        System.out.println("AWS Error Code:   " + ase.getErrorCode());
	        System.out.println("Error Type:       " + ase.getErrorType());
	        System.out.println("Request ID:       " + ase.getRequestId());
	        return 1;
	    	} catch (AmazonClientException ace) {
	        System.out.println("Caught an AmazonClientException, which means the client encountered " +
	                "a serious internal problem while trying to communicate with SQS, such as not " +
	                "being able to access the network.");
	        System.out.println("Error Message: " + ace.getMessage());
	        return 2;
    	}
	}
	
	public int sendMessage(String message){
		try{
	        //System.out.println("Sending a message.\n");
	        sqs.sendMessage(new SendMessageRequest(url, message));
	        return 0;
		} catch (AmazonServiceException ase) {
	        System.out.println("Caught an AmazonServiceException, which means your request made it " +
	                "to Amazon SQS, but was rejected with an error response for some reason.");
	        System.out.println("Error Message:    " + ase.getMessage());
	        System.out.println("HTTP Status Code: " + ase.getStatusCode());
	        System.out.println("AWS Error Code:   " + ase.getErrorCode());
	        System.out.println("Error Type:       " + ase.getErrorType());
	        System.out.println("Request ID:       " + ase.getRequestId());
	        return 1;
	    	} catch (AmazonClientException ace) {
	        System.out.println("Caught an AmazonClientException, which means the client encountered " +
	                "a serious internal problem while trying to communicate with SQS, such as not " +
	                "being able to access the network.");
	        System.out.println("Error Message: " + ace.getMessage());
	        return 2;
    	}
	}
	
	public int sendMessageWorkerFinished(String message, String id) {
		try{
	        //System.out.println("Sending message worker finished.\n");
	        MessageAttributeValue idValue 	= new MessageAttributeValue().withDataType("String").withStringValue(id);
	        MessageAttributeValue terminateValue 	= new MessageAttributeValue().withDataType("String").withStringValue("true");
	        SendMessageRequest messageRequest = new SendMessageRequest(url, message).
	        											addMessageAttributesEntry("workerId", idValue).
	        											addMessageAttributesEntry("terminate", terminateValue);
	        sqs.sendMessage(messageRequest);
	        return 0;
		} catch (AmazonServiceException ase) {
	        System.out.println("Caught an AmazonServiceException, which means your request made it " +
	                "to Amazon SQS, but was rejected with an error response for some reason.");
	        System.out.println("Error Message:    " + ase.getMessage());
	        System.out.println("HTTP Status Code: " + ase.getStatusCode());
	        System.out.println("AWS Error Code:   " + ase.getErrorCode());
	        System.out.println("Error Type:       " + ase.getErrorType());
	        System.out.println("Request ID:       " + ase.getRequestId());
	        return 1;
	    	} catch (AmazonClientException ace) {
	        System.out.println("Caught an AmazonClientException, which means the client encountered " +
	                "a serious internal problem while trying to communicate with SQS, such as not " +
	                "being able to access the network.");
	        System.out.println("Error Message: " + ace.getMessage());
	        return 2;
    	}
		
	}
	
	public int sendMessageWithIdAndTerminate(String message, String id) {
		try{
	        //System.out.println("Sending message worker finished.\n");
	        MessageAttributeValue idValue 	= new MessageAttributeValue().withDataType("String").withStringValue(String.valueOf(id));
	        MessageAttributeValue terminateValue 	= new MessageAttributeValue().withDataType("String").withStringValue("true");

	        SendMessageRequest messageRequest = new SendMessageRequest(url, message).
	        		addMessageAttributesEntry("id", idValue).addMessageAttributesEntry("terminate", terminateValue);
	        sqs.sendMessage(messageRequest);
	        return 0;
		} catch (AmazonServiceException ase) {
	        System.out.println("Caught an AmazonServiceException, which means your request made it " +
	                "to Amazon SQS, but was rejected with an error response for some reason.");
	        System.out.println("Error Message:    " + ase.getMessage());
	        System.out.println("HTTP Status Code: " + ase.getStatusCode());
	        System.out.println("AWS Error Code:   " + ase.getErrorCode());
	        System.out.println("Error Type:       " + ase.getErrorType());
	        System.out.println("Request ID:       " + ase.getRequestId());
	        return 1;
	    	} catch (AmazonClientException ace) {
	        System.out.println("Caught an AmazonClientException, which means the client encountered " +
	                "a serious internal problem while trying to communicate with SQS, such as not " +
	                "being able to access the network.");
	        System.out.println("Error Message: " + ace.getMessage());
	        return 2;
    	}
		
	}
	
	public int sendMessageError(String message, String id) {
		try{
	        //System.out.println("Sending message worker finished.\n");
	        MessageAttributeValue idValue 	= new MessageAttributeValue().withDataType("String").withStringValue(String.valueOf(id));
	        MessageAttributeValue errorValue 	= new MessageAttributeValue().withDataType("String").withStringValue("true");

	        SendMessageRequest messageRequest = new SendMessageRequest(url, message).
	        		addMessageAttributesEntry("id", idValue).addMessageAttributesEntry("error", errorValue);
	        sqs.sendMessage(messageRequest);
	        return 0;
		} catch (AmazonServiceException ase) {
	        System.out.println("Caught an AmazonServiceException, which means your request made it " +
	                "to Amazon SQS, but was rejected with an error response for some reason.");
	        System.out.println("Error Message:    " + ase.getMessage());
	        System.out.println("HTTP Status Code: " + ase.getStatusCode());
	        System.out.println("AWS Error Code:   " + ase.getErrorCode());
	        System.out.println("Error Type:       " + ase.getErrorType());
	        System.out.println("Request ID:       " + ase.getRequestId());
	        return 1;
	    	} catch (AmazonClientException ace) {
	        System.out.println("Caught an AmazonClientException, which means the client encountered " +
	                "a serious internal problem while trying to communicate with SQS, such as not " +
	                "being able to access the network.");
	        System.out.println("Error Message: " + ace.getMessage());
	        return 2;
    	}
		
	}
	
	public List<Message> getMessages(int numOfMsgs){
		try{
	    	ReceiveMessageRequest receiveMessageRequest = new ReceiveMessageRequest(url);
	    	if(numOfMsgs > 10)
	    		numOfMsgs = 10;
	    	receiveMessageRequest.withMaxNumberOfMessages(numOfMsgs).
	    				withMessageAttributeNames("id").
	    				withMessageAttributeNames("terminate").
	    				withMessageAttributeNames("numOfWorkers").
	    				withMessageAttributeNames("workerId").
	    				withAttributeNames("All");
	    	List<Message> messages = sqs.receiveMessage(receiveMessageRequest).getMessages();
	    	//if(messages.size() != 0)
	    	//	System.out.println("Receiving messages: " + messages.size() + ".\n");

	    	return messages;
		} catch (AmazonServiceException ase) {
	        System.out.println("Caught an AmazonServiceException, which means your request made it " +
	                "to Amazon SQS, but was rejected with an error response for some reason.");
	        System.out.println("Error Message:    " + ase.getMessage());
	        System.out.println("HTTP Status Code: " + ase.getStatusCode());
	        System.out.println("AWS Error Code:   " + ase.getErrorCode());
	        System.out.println("Error Type:       " + ase.getErrorType());
	        System.out.println("Request ID:       " + ase.getRequestId());
	    	} catch (AmazonClientException ace) {
	        System.out.println("Caught an AmazonClientException, which means the client encountered " +
	                "a serious internal problem while trying to communicate with SQS, such as not " +
	                "being able to access the network.");
	        System.out.println("Error Message: " + ace.getMessage());
    	}
		return null;
	}
	
	public List<Message> getMessagesMinimalVisibilityTime(int numOfMsgs) {
		try{
	    	ReceiveMessageRequest receiveMessageRequest = new ReceiveMessageRequest(url);
	    	if(numOfMsgs > 10)
	    		numOfMsgs = 10;
	    	receiveMessageRequest.withMaxNumberOfMessages(numOfMsgs).withVisibilityTimeout(1).
	    				withMessageAttributeNames("id").
	    				withMessageAttributeNames("terminate").
	    				withMessageAttributeNames("numOfWorkers");
	    	List<Message> messages = sqs.receiveMessage(receiveMessageRequest).getMessages();
	    	//if(messages.size() != 0)
	    	//	System.out.println("Receiving message.\n");

	    	return messages;
		} catch (AmazonServiceException ase) {
	        System.out.println("Caught an AmazonServiceException, which means your request made it " +
	                "to Amazon SQS, but was rejected with an error response for some reason.");
	        System.out.println("Error Message:    " + ase.getMessage());
	        System.out.println("HTTP Status Code: " + ase.getStatusCode());
	        System.out.println("AWS Error Code:   " + ase.getErrorCode());
	        System.out.println("Error Type:       " + ase.getErrorType());
	        System.out.println("Request ID:       " + ase.getRequestId());
	    	} catch (AmazonClientException ace) {
	        System.out.println("Caught an AmazonClientException, which means the client encountered " +
	                "a serious internal problem while trying to communicate with SQS, such as not " +
	                "being able to access the network.");
	        System.out.println("Error Message: " + ace.getMessage());
    	}
		return null;
	}
	
	public void deleteMessage(Message message){
		try{
			// Delete a message
	        String messageRecieptHandle = message.getReceiptHandle();
	        sqs.deleteMessage(new DeleteMessageRequest(url, messageRecieptHandle));
	        
		} catch (AmazonServiceException ase) {
	        System.out.println("Caught an AmazonServiceException, which means your request made it " +
	                "to Amazon SQS, but was rejected with an error response for some reason.");
	        System.out.println("Error Message:    " + ase.getMessage());
	        System.out.println("HTTP Status Code: " + ase.getStatusCode());
	        System.out.println("AWS Error Code:   " + ase.getErrorCode());
	        System.out.println("Error Type:       " + ase.getErrorType());
	        System.out.println("Request ID:       " + ase.getRequestId());
	    	} catch (AmazonClientException ace) {
	        System.out.println("Caught an AmazonClientException, which means the client encountered " +
	                "a serious internal problem while trying to communicate with SQS, such as not " +
	                "being able to access the network.");
	        System.out.println("Error Message: " + ace.getMessage());
    	}
	}
	
	public void deleteQueue(){
		try{
			// Delete a queue
			System.out.println("Deleting " + queueName + ".\n");
			sqs.deleteQueue(new DeleteQueueRequest(url));
			
    	} catch (AmazonServiceException ase) {
	        System.out.println("Caught an AmazonServiceException, which means your request made it " +
	                "to Amazon SQS, but was rejected with an error response for some reason.");
	        System.out.println("Error Message:    " + ase.getMessage());
	        System.out.println("HTTP Status Code: " + ase.getStatusCode());
	        System.out.println("AWS Error Code:   " + ase.getErrorCode());
	        System.out.println("Error Type:       " + ase.getErrorType());
	        System.out.println("Request ID:       " + ase.getRequestId());
	    	} catch (AmazonClientException ace) {
	        System.out.println("Caught an AmazonClientException, which means the client encountered " +
	                "a serious internal problem while trying to communicate with SQS, such as not " +
	                "being able to access the network.");
	        System.out.println("Error Message: " + ace.getMessage());
    	}
	}

}
