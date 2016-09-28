package com.frameworks.storm.topology;

import backtype.storm.Config;
import backtype.storm.LocalCluster;
import backtype.storm.spout.SchemeAsMultiScheme;
import backtype.storm.tuple.Fields;
import com.frameworks.storm.aggregators.BasicAggregator;
import com.frameworks.storm.debug.Debug;
import com.frameworks.storm.operation.FruitParser;
import com.frameworks.storm.providers.LineProvider;
import com.frameworks.storm.state.custom.FruitState;
import com.frameworks.storm.state.custom.FruitStateFactory;
import com.frameworks.storm.state.custom.FruitStateQuery;
import com.frameworks.storm.state.custom.FruitStateUpdater;
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
import storm.trident.TridentState;
import storm.trident.TridentTopology;

import java.util.Properties;

@Slf4j
public class AggregatorTopology {


  int batchSize = 5;
  String filePath = "src/main/resources/fruitdata";

  private void getTopology()throws Exception{
    TridentTopology topology = new TridentTopology();
    LineProvider sp = new LineProvider(filePath,batchSize); //(path to file,batchsize)

    Stream aggregatedStream = topology.newStream("spout1", sp.createSpout()) //create aggregated stream
            .each(new Fields("str"),new FruitParser(),new Fields("id","fruit","color","weight"))
            //.each(new Fields("str"),new Debug(),new Fields())
            //.each(new Fields("id","fruit","color","weight"),new Debug(),new Fields())
            .groupBy(new Fields("fruit"))
            .aggregate(new Fields("fruit"),new BasicAggregator(),new Fields("fruitName","fruitCount"))
            .each(new Fields("fruitName","fruitCount"),new Debug(),new Fields());

    TridentState FruitState = aggregatedStream //persist the data into the state
            .partitionPersist(new FruitStateFactory(),new Fields("fruitName","fruitCount"),new FruitStateUpdater("fruitName","fruitCount"),new Fields("fruitName"));

    Stream queryStream = FruitState.newValuesStream()
            .stateQuery(FruitState,new Fields("fruitName"),new FruitStateQuery("fruitName"),new Fields("fruitTotals"));
            //.each(new Fields("fruitName","fruitTotals"),new Debug(),new Fields());

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
