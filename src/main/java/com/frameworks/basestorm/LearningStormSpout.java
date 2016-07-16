package com.frameworks.basestorm;

import backtype.storm.spout.SpoutOutputCollector;
import backtype.storm.task.TopologyContext;
import backtype.storm.topology.OutputFieldsDeclarer;
import backtype.storm.topology.base.BaseRichSpout;
import backtype.storm.tuple.Fields;
import backtype.storm.tuple.Values;
import com.twitter.hbc.ClientBuilder;
import com.twitter.hbc.core.Constants;
import com.twitter.hbc.core.endpoint.StatusesSampleEndpoint;
import com.twitter.hbc.core.processor.StringDelimitedProcessor;
import com.twitter.hbc.httpclient.BasicClient;
import com.twitter.hbc.httpclient.auth.Authentication;
import com.twitter.hbc.httpclient.auth.OAuth1;

import java.util.Map;
import java.util.concurrent.BlockingQueue;
import java.util.concurrent.LinkedBlockingQueue;
import java.util.concurrent.TimeUnit;

public class LearningStormSpout extends BaseRichSpout {
	private static final long serialVersionUID = 1L;
	private SpoutOutputCollector spoutOutputCollector;

	public void open(Map conf, TopologyContext context, SpoutOutputCollector spoutOutputCollector){
		//Open the spout
		this.spoutOutputCollector = spoutOutputCollector;
	}
	
	public void nextTuple(){
		
		BlockingQueue<String> queue = new LinkedBlockingQueue<String>(10000);

	    // Define our endpoint: By default, delimited=length is set (we need this for our processor)
	    // and stall warnings are on.
	    StatusesSampleEndpoint endpoint = new StatusesSampleEndpoint();
	    endpoint.stallWarnings(false);
	    String consumerKey = "FBoTvAeS4NHVd2ZNO5kPibnwc";
	    String consumerSecret = "1uWbWagYvCyGJ4yvG1LTllPO5fSiRghiNYsEllmj3xJD6mCzSr";
	    String token = "478097758-lzlQvuqnXYBZzqHhJJdmkEunkKzm4C7kd0u88vWN";
	    String secret = "OsPJ6f2gVG7VDVyDdIOSBo7zbfWBcXvh8mOJsr98vt3gp";

	    Authentication auth = new OAuth1(consumerKey, consumerSecret, token, secret);
	    //Authentication auth = new com.twitter.hbc.httpclient.auth.BasicAuth(username, password);

	    // Create a new BasicClient. By default gzip is enabled.
	    BasicClient client = new ClientBuilder()
	            .name("sampleExampleClient")
	            .hosts(Constants.STREAM_HOST)
	            .endpoint(endpoint)
	            .authentication(auth)
	            .processor(new StringDelimitedProcessor(queue))
	            .build();
		
	      client.connect();

	      String msg = "default msg";
	      // Do whatever needs to be done with messages
	      for (int msgRead = 0; msgRead < 1000; msgRead++) {
	        if (client.isDone()) {
	          System.out.println("Client connection closed unexpectedly: " + client.getExitEvent().getMessage());
	          break;
	        }
	        try {
				msg = queue.poll(5, TimeUnit.SECONDS);
			} catch (InterruptedException e) {
				e.printStackTrace();
			}
	        
	        if (msg == null) {
	          System.out.println("Did not receive a message in 5 seconds");
	        }else if(msg.contains("text")){
		          String[] cleaned_msgs = spoutCleaner(msg);
				for(String cleaned_msg: cleaned_msgs){
		          System.out.println("printing:" +cleaned_msg);
		          spoutOutputCollector.emit(new Values(cleaned_msg));
				}
	        }
	        else {
	          System.out.println("No Text");
	        }
	      }
	      client.stop();

	}
	
	public void declareOutputFields(OutputFieldsDeclarer declarer){
		//emit tuple with the field "site"
		declarer.declare(new Fields("word"));
	}
	
	public static String[] spoutCleaner(String msg){
		String cleaned_msg_1 = msg;
		String cleaned_msg_2 = "Default";
		
		cleaned_msg_1= cleaned_msg_1.toLowerCase();
	    cleaned_msg_1 = cleaned_msg_1.substring(cleaned_msg_1.indexOf("text")+7,cleaned_msg_1.indexOf("source")-3);
   		if (cleaned_msg_1.contains("http")) cleaned_msg_1 = cleaned_msg_1.substring(0,cleaned_msg_1.indexOf("http"));
		cleaned_msg_2 = removeStrings(cleaned_msg_1);
		String[] split_words = cleaned_msg_2.toLowerCase().split(" ");

		return(split_words);
	}
	
// remove strings	
	public static String removeStrings(String unclean){
		String list_of_words[] ={" to"," a"," at","the ","to ","a ","at "," the","for ", " for","i "," i","rt "," rt", "de "," de",
				"of "," of","you ","-","on ", " on", " en","en ","un "," un","your "," your","me ","with "," with",
				"que "," que","el "," el","how "," how","my "," my","like "," like","just "," just","not "," not"};
		String unclean_copy = unclean;
		String cleaned = "default";
		
		for(String remove_word:list_of_words){
			unclean_copy = unclean_copy.replaceAll(remove_word,"");
		}
		 
		cleaned = unclean_copy;

		return(cleaned);	
	}
	
}
