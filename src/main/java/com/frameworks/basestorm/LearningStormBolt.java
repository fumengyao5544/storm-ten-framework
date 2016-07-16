package com.frameworks.basestorm;

import backtype.storm.topology.BasicOutputCollector;
import backtype.storm.topology.OutputFieldsDeclarer;
import backtype.storm.topology.base.BaseBasicBolt;
import backtype.storm.tuple.Tuple;

import java.util.Collections;
import java.util.HashMap;
import java.util.Map;

public class LearningStormBolt extends BaseBasicBolt {
	private static final long serialVersionUID = 1L;
    Map<String, Integer> wordCount = new HashMap<String, Integer>(); // for word count

    private Map<String, Integer> maxWords(Map<String, Integer> word_list, int num_words){
    	
    	/*Variables*/
	    Map<String, Integer> maxCounts = new HashMap<String, Integer>(); // for word count
	    Map<String, Integer> word_count= new HashMap<String, Integer>();// for word count
	    word_count.putAll(word_list);
	    
	    int word_count_size  = word_count.size();
	    if(num_words>word_count_size){
	    	num_words = word_count_size-1;
	    }
	    /*DEBUG*/ System.out.println("num words:" + num_words);
	    	    
	    for(int i=0;i<num_words;i++){

		    int max_count = Collections.max(word_count.values());
		    
			for(String word : word_count.keySet()){
				
			    ///*DEBUG*/ System.out.println("total: " + word_count);

			      if((word_count.get(word)) == max_count){
			    	  maxCounts.put(word,word_count.get(word));
			    	  word_count.remove(word);
			    	  /*DEBUG*/
					  System.out.println("word: "+ word+"total: " + wordCount.get(word));

			    	  break;
			      }
			}
	    }
	    
	    /*DEBUG*/
		//System.out.println(maxCounts);
		
		return(maxCounts);
    }

	public void execute(Tuple input, BasicOutputCollector collector){
		/*Variables*/
		String word= input.getStringByField("word"); // fetched the field site from input tuple
	    Map<String, Integer> maxCounts = new HashMap<String, Integer>(); // list of max word count
		int currentCount = wordCount.containsKey(word) ? wordCount.get(word) + 1: 1; //This is just a simple if else statement
		wordCount.put(word,currentCount);
		
		maxCounts = maxWords(wordCount,5);

			// print out the value of field site on console
			//System.out.println("Current Count: " + count);
			//System.out.println("Total: " + wordCount); //prints everything
			System.out.println("Top 5:" + maxCounts);
			
			//try {
			    //Thread.sleep(1000);                 //1000 milliseconds is one second.
			//} catch(InterruptedException ex) {
			    //Thread.currentThread().interrupt();
			//}
		
	      //collector.emit(new Values(word, count)); 

	}
	
	public void declareOutputFields(OutputFieldsDeclarer declarer){
		
	}
	
}

