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
import com.amazonaws.services.sqs.model.ReceiveMessageRequest;
import com.amazonaws.services.sqs.model.SendMessageRequest;

public class SQS {
	
	private AmazonSQS sqs;
	private String url;
	private String queueName;
	
	
	public SQS(AWSCredentials credentials, String _queueName) {
		super();
		this.sqs = new AmazonSQSClient(credentials);
		queueName = _queueName;
				
		try{
	        System.out.println("Creating a new SQS queue called " + queueName + "\n");
	        CreateQueueRequest createQueueRequest = new CreateQueueRequest(queueName);
	        this.url = sqs.createQueue(createQueueRequest).getQueueUrl();
	        System.out.println("Queue URL:" + this.url);
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

	
	public void sendMessageType1(String message, String terminate, String numOfWorkers){
		try{
	        System.out.println("Sending message type 1.\n");
	        MessageAttributeValue terminateValue 	= new MessageAttributeValue().withDataType("String").withStringValue(terminate);
	        MessageAttributeValue numOfWorkersValue = new MessageAttributeValue().withDataType("String").withStringValue(numOfWorkers);
	        SendMessageRequest messageRequest = new SendMessageRequest(url, message).
	        		addMessageAttributesEntry("terminate", terminateValue).
	        		addMessageAttributesEntry("numOfWorkers", numOfWorkersValue);
	        sqs.sendMessage(messageRequest);
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
	
	public void sendMessageType2(String message, String id){
		try{
	        System.out.println("Sending message type 2.\n");
	        MessageAttributeValue idValue 	= new MessageAttributeValue().withDataType("String").withStringValue(id);
	        SendMessageRequest messageRequest = new SendMessageRequest(url, message).
	        		addMessageAttributesEntry("id", idValue);
	        sqs.sendMessage(messageRequest);
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
	
	public void sendMessage(String message){
		try{
	        System.out.println("Sending a message.\n");
	        sqs.sendMessage(new SendMessageRequest(url, message));
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
	
	public List<Message> getMessages(int numOfMsgs){
		try{
	    	ReceiveMessageRequest receiveMessageRequest = new ReceiveMessageRequest(url);
	    	if(numOfMsgs > 10)
	    		numOfMsgs = 10;
	    	receiveMessageRequest.withMaxNumberOfMessages(numOfMsgs).
	    				withMessageAttributeNames("id").
	    				withMessageAttributeNames("terminate").
	    				withMessageAttributeNames("numOfWorkers").withAttributeNames("All");
	    	List<Message> messages = sqs.receiveMessage(receiveMessageRequest).getMessages();
	    	if(messages.size() != 0)
	    		System.out.println("Receiving messages: " + messages.size() + ".\n");

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
	    	if(messages.size() != 0)
	    		System.out.println("Receiving message.\n");

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
	        System.out.println("Deleting a message.\n");
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
