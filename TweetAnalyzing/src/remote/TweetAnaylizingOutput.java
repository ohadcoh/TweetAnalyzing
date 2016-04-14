package remote;
import java.util.*;


/**
 * 
 *
 */
public class TweetAnaylizingOutput {
	
	public String tweet;
	public int sentimentColor;
	public Vector<String> namedEntities;
	
	public TweetAnaylizingOutput(String tweet, int sentimentColor, Vector<String> namedEntities) {
		super();
		this.tweet = tweet;
		this.sentimentColor = sentimentColor;
		this.namedEntities = namedEntities;
	}
	
}
