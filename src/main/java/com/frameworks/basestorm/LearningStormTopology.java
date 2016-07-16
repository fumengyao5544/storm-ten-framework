package com.frameworks.basestorm;

import backtype.storm.Config;
import backtype.storm.LocalCluster;
import backtype.storm.generated.AlreadyAliveException;
import backtype.storm.generated.InvalidTopologyException;
import backtype.storm.topology.TopologyBuilder;
import backtype.storm.tuple.Fields;

public class LearningStormTopology{
	public static void main(String[] args) throws AlreadyAliveException, InvalidTopologyException {
		// create an instance of TopologyBuilder class
		TopologyBuilder builder = new TopologyBuilder();
		builder.setSpout("LearningStormSpout", new LearningStormSpout(),2);
		// set the bolt class
		builder.setBolt("LearningStormBolt", new LearningStormBolt(),2).fieldsGrouping("LearningStormSpout",new Fields("word"));
		Config conf = new Config();
		conf.setDebug(true);
		//create an instance of LocalCluster class for executing topology in local mode
		LocalCluster cluster = new LocalCluster();
		
		//LearningStormTopology in the name of submitted topology.
		cluster.submitTopology("LearningStormTopology", conf, builder.createTopology());
		try{
			Thread.sleep(10000);
		} catch (Exception exception){
			System.out.println("Thread interrupted exception: " + exception);
		} 
		
		// kill the LearningStormTopology
		cluster.killTopology("LearningStormTopology");
		// shutdown the storm test cluster
		cluster.shutdown();
		
	}
}


