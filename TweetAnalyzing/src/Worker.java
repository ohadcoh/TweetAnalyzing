import java.util.*;

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



class Worker {
	// fields related to tweet analyzing
	private StanfordCoreNLP sentimentPipeline;
	private StanfordCoreNLP NERPipeline;

	// initialize analyze tweets fields with the needed info haha
	Worker() {
		Properties propsSentiment = new Properties();
		propsSentiment.put("annotators", "tokenize, ssplit, parse, sentiment");
		sentimentPipeline = new StanfordCoreNLP(propsSentiment);

		Properties propsEntities = new Properties();
		propsEntities.put("annotators", "tokenize , ssplit, pos, lemma, ner");
		NERPipeline = new StanfordCoreNLP(propsEntities);

	}
	
	public TweetAnaylizingOutput analyzeTweet(String tweet) {
		Vector<String> entities = findEntities(tweet);
		int sentimentColot = findSentiment(tweet);
		TweetAnaylizingOutput output = new TweetAnaylizingOutput(tweet, sentimentColot, entities);
		
		return output;
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