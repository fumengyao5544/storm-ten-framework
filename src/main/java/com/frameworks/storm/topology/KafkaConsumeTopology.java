package com.frameworks.storm.topology;

import backtype.storm.LocalCluster;
import com.frameworks.storm.operation.KafkaFieldGenerator;
import com.frameworks.storm.providers.SpoutProvider;
import lombok.Setter;
import lombok.SneakyThrows;
import org.yaml.snakeyaml.Yaml;
import storm.kafka.BrokerHosts;
import storm.kafka.trident.*;
import storm.kafka.trident.mapper.FieldNameBasedTupleToKafkaMapper;
import storm.kafka.trident.selector.DefaultTopicSelector;
import storm.kafka.trident.TridentKafkaConfig;
import backtype.storm.spout.SchemeAsMultiScheme;
import lombok.extern.slf4j.Slf4j;
import storm.trident.Stream;
import backtype.storm.Config;
import backtype.storm.StormSubmitter;
import storm.kafka.StringScheme;
import storm.kafka.ZkHosts;

import storm.trident.TridentTopology;
import backtype.storm.tuple.Fields;
import storm.trident.state.StateFactory;

import java.io.InputStream;
import java.util.Properties;

@Slf4j
@Setter
public class KafkaConsumeTopology {

  String zkNodeAddress;
  String brokerNodeAddress;
  String hbaseNodeAddress;
  String topicName;
  String tableName;
  String filePath;
  int batchSize;

  private void persistToHBaseKafka(Stream stream) {

  }

  /*Helper Functions*/

  private OpaqueTridentKafkaSpout createKafkaSpout() {
    BrokerHosts zk = new ZkHosts(zkNodeAddress);
    TridentKafkaConfig spoutConf = new TridentKafkaConfig(zk, topicName);
    spoutConf.scheme = new SchemeAsMultiScheme(new StringScheme());
    OpaqueTridentKafkaSpout spout = new OpaqueTridentKafkaSpout(spoutConf);
    return(spout);
  }

  private void getTopology()throws Exception{

    TridentTopology topology = new TridentTopology();
    Stream stream = topology.newStream("spout1", createKafkaSpout())
            .each(new Fields("str"),new com.frameworks.storm.debug.Debug(),new Fields());

    StateFactory stateFactory = new TridentKafkaStateFactory()
            .withKafkaTopicSelector(new DefaultTopicSelector(topicName))
            .withTridentTupleToKafkaMapper(new FieldNameBasedTupleToKafkaMapper<String, String>("key", "string"));

    //stream.partitionPersist(stateFactory, new Fields("key","string"), new TridentKafkaUpdater(), new Fields("key","string"));

    Config conf = new Config();

    LocalCluster cluster = new LocalCluster();

    Properties props = new Properties();
    props.put("metadata.broker.list", brokerNodeAddress);
    props.put("request.required.acks", "1");
    props.put("serializer.class", "kafka.serializer.StringEncoder");
    props.put("key.serializer.class","kafka.serializer.StringEncoder");
    conf.put("kafka.broker.properties", props);

    //Local Mode
    cluster.submitTopology("kafkaTridentTest", conf, topology.build());

    //Submit to Cluster Mode
    //StormSubmitter.submitTopology("kafkaTridentTest", conf, topology.build());

  }

  @SneakyThrows
  public static void main(String args[]){
    Yaml yaml = new Yaml();
    InputStream in = ClassLoader.getSystemResourceAsStream("credentials.yml");
    KafkaConsumeTopology holoTopo= yaml.loadAs(in, KafkaConsumeTopology.class);
    holoTopo.getTopology();

  }
}
