package com.frameworks.storm.topology;

import backtype.storm.LocalCluster;
import storm.kafka.BrokerHosts;
import storm.kafka.trident.*;
import storm.kafka.trident.mapper.FieldNameBasedTupleToKafkaMapper;
import storm.kafka.trident.selector.DefaultTopicSelector;
import storm.kafka.trident.TridentKafkaConfig;
import backtype.storm.spout.SchemeAsMultiScheme;
import lombok.Setter;
import lombok.extern.slf4j.Slf4j;
import storm.trident.Stream;
import storm.trident.state.StateFactory;
import storm.trident.state.StateUpdater;
import backtype.storm.Config;
import backtype.storm.StormSubmitter;
import storm.kafka.StringScheme;
import storm.kafka.ZkHosts;
import org.apache.storm.hbase.trident.state.HBaseStateFactory;

import storm.trident.TridentTopology;
import backtype.storm.tuple.Fields;

import java.util.Arrays;
import java.util.Properties;
@Slf4j
public class StsAgrTopology {


  private void persistToHBaseKafka(Stream stream) {

  }

  /*Helper Functions*/

  private OpaqueTridentKafkaSpout createKafkaSpout() {


    BrokerHosts zk = new ZkHosts("hw0002.dev1.awse1a.datasciences.tmcs");
    TridentKafkaConfig spoutConf = new TridentKafkaConfig(zk, "sts.debug.topic");
    spoutConf.scheme = new SchemeAsMultiScheme(new StringScheme());
    OpaqueTridentKafkaSpout spout = new OpaqueTridentKafkaSpout(spoutConf);
    return(spout);
  }

  private void getTopology()throws Exception{
    TridentTopology topology = new TridentTopology();
    Stream stream = topology.newStream("spout1", createKafkaSpout())
            .each(new Fields("str"),new com.frameworks.storm.debug.Debug(),new Fields());

    TridentKafkaStateFactory stateFactory = new TridentKafkaStateFactory()
            .withKafkaTopicSelector(new DefaultTopicSelector("sts.debug.topic"))
            .withTridentTupleToKafkaMapper(new FieldNameBasedTupleToKafkaMapper("word","string"));
    stream.partitionPersist(stateFactory, new Fields("str"), new TridentKafkaUpdater(), new Fields());

    Config conf = new Config();

    LocalCluster cluster = new LocalCluster();

    Properties props = new Properties();
    props.put("metadata.broker.list", "hw0002.dev1.awse1a.datasciences.tmcs:6667");
    props.put("request.required.acks", "1");
    props.put("serializer.class", "kafka.serializer.StringEncoder");
    props.put("key.serializer.class","kafka.serializer.StringEncoder");
    conf.put("kafka.broker.properties", props);

    StormSubmitter.submitTopology("kafkaTridentTest", conf, topology.build());

  }

  public static void main(String args[]){

    try{new StsAgrTopology().getTopology();}
    catch(Exception e){
      e.printStackTrace();

    }
  }
}
