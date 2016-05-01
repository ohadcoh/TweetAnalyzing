package remote;
import java.io.IOException;
import java.util.*;

import org.jsoup.Jsoup;
import org.jsoup.nodes.Document;

import com.amazonaws.auth.AWSCredentials;
import com.amazonaws.services.sqs.model.Message;

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

public class Worker {
	// fields related to tweet analyzing
	private StanfordCoreNLP sentimentPipeline;
	private StanfordCoreNLP NERPipeline;
	private SQS inputSQS;
	private SQS outputSQS;
	private int id;
	private long numOfLinksHandled;
	private long numOfLinksBroken;
	private long numOfLinksOk;
	
	// initialize analyze tweets fields with the needed info haha
	public Worker(AWSCredentials credentials, SQS _inputSQS, SQS _outputSQS, int _id) {
		id = _id;
		//init SQSs
		inputSQS  = _inputSQS;
		outputSQS = _outputSQS;
		// init properties for processing
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
	
	public void analyzeTweet() {
		
		while(true){
			// 1. read message from SQS
			List<Message> inputMessageList = inputSQS.getMessages(1);
			// 2. if queue empty finish
			// TODO: handle termination properly 
			if(inputMessageList.size() == 0)
				break;
			// 3. isolate id and link
			numOfLinksHandled++;
			Message inputMessage = inputMessageList.get(0);
			String taskId = inputMessage.getMessageAttributes().get("id").getStringValue();
			String tweetLink = inputMessage.getBody();
			//System.out.println("tweet link: " + tweetLink + " , jobID: " + taskId);
			Document tweetPage;
			// 4. isolate tweet from link
			String tweet = new String("");
			try {
				tweetPage = Jsoup.connect(tweetLink).get();
				tweet = tweetPage.select("title").first().text();
				numOfLinksOk++;
			} catch (IOException e) {
				numOfLinksBroken++;
				e.printStackTrace();
			} catch (IllegalArgumentException e) {
				numOfLinksBroken++;
				e.printStackTrace();
			}
			System.out.println("Tweet before analyzing: " + tweet);
			// 5. analyze the tweet
			Vector<String> entities = findEntities(tweet);
			int sentimentColot = findSentiment(tweet);
			// 6. write message to output SQS
			outputSQS.sendMessageType2(tweet+ ", " + sentimentColot + ", " + entities, taskId);
			// 7. delete message
			inputSQS.deleteMessage(inputMessage);
		}
//		System.out.println("Worker " + id + " handled: " + numOfLinksHandled + 
//							". " + numOfLinksOk + " of them are ok, " + numOfLinksBroken + " of them are broken.");
		outputSQS.sendMessageWorkerFinished("Worker " + id + " handled: " + numOfLinksHandled + 
							". " + numOfLinksOk + " of them are ok, " + numOfLinksBroken + " of them are broken.", id);
	}

	// find named entities in a tweet
	Vector<String> findEntities(String tweet) {
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
	int findSentiment(String tweet) {
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
		return mainSentiment;
	}

}