package com.frameworks.storm.topology;

import backtype.storm.Config;
import backtype.storm.LocalCluster;
import backtype.storm.spout.SchemeAsMultiScheme;
import backtype.storm.tuple.Fields;
import com.frameworks.storm.aggregators.BasicAggregator;
import com.frameworks.storm.debug.Debug;
import com.frameworks.storm.operation.FruitParser;
import com.frameworks.storm.operation.KafkaFieldGenerator;
import com.frameworks.storm.providers.SpoutProvider;
import lombok.extern.slf4j.Slf4j;
import storm.kafka.BrokerHosts;
import storm.kafka.StringScheme;
import storm.kafka.ZkHosts;
import storm.kafka.trident.OpaqueTridentKafkaSpout;
import storm.kafka.trident.TridentKafkaConfig;
import storm.kafka.trident.TridentKafkaStateFactory;
import storm.kafka.trident.TridentKafkaUpdater;
import storm.kafka.trident.mapper.FieldNameBasedTupleToKafkaMapper;
import storm.kafka.trident.selector.DefaultTopicSelector;
import storm.trident.Stream;
import storm.trident.TridentTopology;
import storm.trident.state.StateFactory;

import java.util.Properties;

@Slf4j
public class AggregatorTopology {

  /*Helper Functions*/

  private void getTopology()throws Exception{
    TridentTopology topology = new TridentTopology();
    SpoutProvider sp = new SpoutProvider();
    Stream stream = topology.newStream("spout1", sp.createSpout())
            .each(new Fields("str"),new FruitParser(),new Fields("id","fruit","color","weight"))
            .groupBy(new Fields("fruit"))
            .aggregate(new Fields("fruit"),new BasicAggregator(),new Fields("fruitcount"))
            .each(new Fields("fruitcount"),new Debug(),new Fields());

           //.each(new Fields("id","fruit","color","weight"),new Debug(),new Fields());

    Config conf = new Config();

    LocalCluster cluster = new LocalCluster();

    //Local Mode
    cluster.submitTopology("kafkaTridentTest", conf, topology.build());

    //Submit to Cluster Mode
    //StormSubmitter.submitTopology("kafkaTridentTest", conf, topology.build());

  }

  public static void main(String args[]){

    try{new AggregatorTopology().getTopology();}
    catch(Exception e){
      e.printStackTrace();

    }
  }
}
