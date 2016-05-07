package remote;
import java.io.File;
import java.io.FileInputStream;
import java.io.FileNotFoundException;
import java.io.FileOutputStream;
import java.io.IOException;
import java.io.OutputStreamWriter;
import java.io.Writer;
import java.util.List;
import java.util.Properties;
import java.util.UUID;
import java.util.Vector;

import org.joda.time.LocalDateTime;
import org.jsoup.Jsoup;
import org.jsoup.nodes.Document;

import com.amazonaws.auth.AWSCredentials;
import com.amazonaws.auth.PropertiesCredentials;
import com.amazonaws.services.sqs.model.Message;
import aws.S3;
import aws.SQS;

import edu.stanford.nlp.ling.CoreAnnotations;
import edu.stanford.nlp.ling.CoreAnnotations.NamedEntityTagAnnotation;
import edu.stanford.nlp.ling.CoreAnnotations.SentencesAnnotation;
import edu.stanford.nlp.ling.CoreAnnotations.TextAnnotation;
import edu.stanford.nlp.ling.CoreAnnotations.TokensAnnotation;
import edu.stanford.nlp.ling.CoreLabel;
import edu.stanford.nlp.pipeline.Annotation;
import edu.stanford.nlp.pipeline.StanfordCoreNLP;
import edu.stanford.nlp.rnn.RNNCoreAnnotations;
import edu.stanford.nlp.sentiment.SentimentCoreAnnotations;
import edu.stanford.nlp.trees.Tree;
import edu.stanford.nlp.util.CoreMap;

public class Worker implements Runnable {
	// fields related to tweet analyzing
	private StanfordCoreNLP sentimentPipeline;
	private StanfordCoreNLP NERPipeline;
	// AWS
	private AWSCredentials credentials;
	private SQS inputSQS;
	private SQS outputSQS;
	private S3 s3;
	// Local info
	private String id;
	private long numOfLinksHandled;
	private long numOfLinksBroken;
	private long numOfLinksOk;
	
	public static void main(String[] args)
	{
		// hard coded names
		String propertiesFilePath 		= "./dspsass1.properties";
		String inputSQSQueueName		= "managerToWorkerasafohad";
		String outputSQSQueueName		= "workerToManagerasafohad";
		String statisticsBucketName 	= "workersstatisticsasafohad";
		// create new worker
		Worker myWorker = new Worker(propertiesFilePath, inputSQSQueueName, outputSQSQueueName, statisticsBucketName);
		myWorker.analyzeTweet();
	}
	
	protected Worker(	String propertiesFilePath, 
					String inputSQSQueueName,
					String outputSQSQueueName,
					String statisticsBucketName) {
		// 0. create id
		id = UUID.randomUUID().toString();
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
		// 2. create s3 client for statistics
		s3 = new S3(credentials, statisticsBucketName);
		// 3. create sqs client and two queues
		inputSQS = new SQS(credentials, inputSQSQueueName);
		outputSQS = new SQS(credentials, outputSQSQueueName);
		// 4. init properties for processing
		Properties propsSentiment = new Properties();
		propsSentiment.put("annotators", "tokenize, ssplit, parse, sentiment");
		sentimentPipeline = new StanfordCoreNLP(propsSentiment);
		Properties propsEntities = new Properties();
		propsEntities.put("annotators", "tokenize , ssplit, pos, lemma, ner");
		NERPipeline = new StanfordCoreNLP(propsEntities);
		// init statistics
		numOfLinksHandled 	= 0;
		numOfLinksBroken	= 0;
		numOfLinksOk		= 0;
	}
	
	public void run() {
		analyzeTweet();		
	}
	
	public void analyzeTweet() {
		System.out.println("Worker: Start working!");
		while(true){
			// 1. read message from SQS
			List<Message> inputMessageList = inputSQS.getMessages(1);
			// 2. if queue empty finish
			if(inputMessageList.size() == 0)
			{
				try {
					Thread.sleep(500);
				} catch (InterruptedException e) {
					e.printStackTrace();
				}
				continue;
			}
			// 2. check if terminated request
			if(inputMessageList.get(0).getMessageAttributes().get("terminate") != null)
			{
				System.out.println("Worker: received termination request");
				inputSQS.deleteMessage(inputMessageList.get(0));
				break;
			}
				
			// 3. isolate id and link
			numOfLinksHandled++;
			Message inputMessage = inputMessageList.get(0);
			String taskId = inputMessage.getMessageAttributes().get("id").getStringValue();
			String tweetLink = inputMessage.getBody();
			Document tweetPage;
			// 4. isolate tweet from link
			String tweet = new String("");
			try {
				tweetPage = Jsoup.connect(tweetLink).get();
				tweet = tweetPage.select("title").first().text().replace("\n", "").replace("\r", "");
				numOfLinksOk++;
			} catch (IOException e) {
				numOfLinksBroken++;
				e.printStackTrace();
			} catch (IllegalArgumentException e) {
				numOfLinksBroken++;
				e.printStackTrace();
			}
			// 5. analyze the tweet
			System.out.println("Worker: tweet is- " + tweet);
			Vector<String> entities = findEntities(tweet);
			String sentimentColor = findSentiment(tweet);
			// 6. write message to output SQS
			outputSQS.sendMessageWithId(sentimentColor + ";" + entities + ";" + tweet, taskId);
			// 7. delete message
			inputSQS.deleteMessage(inputMessage);
		}

		try {
			File statistics = new File("statisticsOfWorker" + id + ".txt");
			Writer writer = new OutputStreamWriter(new FileOutputStream(statistics));
			writer.write(LocalDateTime.now() + "### Worker " + id + " handled: " + numOfLinksHandled + 
					". " + numOfLinksOk + " of them are ok, " + numOfLinksBroken + " of them are broken.");
			writer.close();
			s3.uploadFile("statisticsOfWorker" + id + ".txt");
			statistics.delete();
		} catch (IOException e) {
			e.printStackTrace();
			outputSQS.sendMessageError("Statistics IO exception", id);
			return;
		}
		
		outputSQS.sendMessageWorkerFinished("Worker " + id + " handled: " + numOfLinksHandled + 
				". " + numOfLinksOk + " of them are ok, " + numOfLinksBroken + " of them are broken.", id);
	}

	// find named entities in a tweet
	private Vector<String> findEntities(String tweet) {
		// create an empty Annotation just with the given text
		Annotation document = new Annotation(tweet);

		// run all Annotators on this text
		NERPipeline.annotate(document);

		// these are all the sentences in this document
		// a CoreMap is essentially a Map that uses class objects as keys and
		// has values with custom types
		List<CoreMap> sentences = document.get(SentencesAnnotation.class);

		Vector<String> entities = new Vector<String>();
		for (CoreMap sentence : sentences) {
			// traversing the words in the current sentence
			// a CoreLabel is a CoreMap with additional token-specific methods
			for (CoreLabel token : sentence.get(TokensAnnotation.class)) {
				// this is the text of the token
				String word = token.get(TextAnnotation.class);
				// this is the NER label of the token
				String ne = token.get(NamedEntityTagAnnotation.class);
				// if the enetitie is what we looking for, add to output
				if(ne.equals("PERSON") || ne.equals("LOCATION") || ne.equals("ORGANIZATION"))
					entities.add(word + ":" + ne);
			}
		}
		return entities;
	}

	// find sentiment of a tweet
	private String findSentiment(String tweet) {
		int mainSentiment = 0;
		if (tweet != null && tweet.length() > 0) {
			int longest = 0;
			Annotation annotation = sentimentPipeline.process(tweet);
			for (CoreMap sentence : annotation.get(CoreAnnotations.SentencesAnnotation.class)) {
				Tree tree = sentence.get(SentimentCoreAnnotations.AnnotatedTree.class);
				int sentiment = RNNCoreAnnotations.getPredictedClass(tree);
				String partText = sentence.toString();
				if (partText.length() > longest) {
					mainSentiment = sentiment;
					longest = partText.length();
				}
			}
		}
		switch(mainSentiment){
		case 0:
			return "DarkRed";
		case 1:
			return "Red";
		case 2:
			return "Black";
		case 3:
			return "LightGreen";
		case 4:
			return "DarkGreen";
		default:
			return "Black";
		}
	}


}