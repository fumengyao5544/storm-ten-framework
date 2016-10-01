package com.frameworks.storm.topology;

import backtype.storm.Config;
import backtype.storm.LocalCluster;
import backtype.storm.spout.SchemeAsMultiScheme;
import backtype.storm.tuple.Fields;
import com.frameworks.storm.debug.Debug;
import com.frameworks.storm.operation.HBaseFieldGenerator;
import com.frameworks.storm.operation.HBaseFruitParser;
import com.frameworks.storm.operation.KafkaFieldGenerator;
import com.frameworks.storm.providers.LineProvider;
import com.frameworks.storm.state.hbase.standard.HBaseStandardMapperWithTs;
import com.frameworks.storm.state.hbase.standard.HBaseStandardValueMapperWithTs;
import lombok.Setter;
import lombok.SneakyThrows;
import lombok.extern.slf4j.Slf4j;
import org.apache.hadoop.hbase.client.Durability;
import org.apache.storm.hbase.bolt.mapper.HBaseProjectionCriteria;
import org.apache.storm.hbase.trident.state.HBaseState;
import org.apache.storm.hbase.trident.state.HBaseStateFactory;
import org.apache.storm.hbase.trident.state.HBaseUpdater;
import org.yaml.snakeyaml.Yaml;
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

import java.io.InputStream;
import java.util.Properties;

@Slf4j
@Setter
public class HBaseWriterTopo {

  String zkNodeAddress;
  String brokerNodeAddress;
  String topicName;
  String filePath;
  int batchSize;
  String tableName;
  StateFactory factory;
  String zkQuorum;
  String znodeParent;

  private Fields fields= new Fields("key","family","qualifier","ts","value");


  private void persistToHBaseKafka(Stream stream) {

  }

  private void setOptions(){
    HBaseStandardMapperWithTs tridentHBaseMapper = new HBaseStandardMapperWithTs("key","family","qualifier","ts","value");

    HBaseStandardValueMapperWithTs tridentHBaseValueMapper = new HBaseStandardValueMapperWithTs();

    HBaseProjectionCriteria projectionCriteria = new HBaseProjectionCriteria();
    //projectionCriteria.addColumn(new HBaseProjectionCriteria.ColumnMetaData("DATA", "value"));
    projectionCriteria.addColumnFamily("DATA");

    Durability durability = Durability.SYNC_WAL;

    HBaseState.Options options = new HBaseState.Options()
            .withConfigKey("hbase.config")
            .withDurability(durability)
            .withMapper(tridentHBaseMapper)
            .withProjectionCriteria(projectionCriteria)
            .withRowToStormValueMapper(tridentHBaseValueMapper)
            .withTableName(tableName);

    this.factory = new HBaseStateFactory(options);
  }

  /*Helper Functions*/

  private void getTopology()throws Exception{
    setOptions();
    TridentTopology topology = new TridentTopology();
    LineProvider lp = new LineProvider(filePath,batchSize); //(path to file,batchsize)

    topology.newStream("spout1", lp.createSpout())
            .each(new Fields("str"),new HBaseFruitParser(),fields)
            .each(new Fields("key"),new Debug(),new Fields())
            .partitionPersist(factory, fields,  new HBaseUpdater(), new Fields());

    Config conf = new Config();
    Properties props = new Properties();
    props.put("hbase.zookeeper.quorum", "zk0001.dev2.awse1a.datasciences.tmcs,zk0002.dev2.awse1a.datasciences.tmcs,zk0003.dev2.awse1a.datasciences.tmcs");
    props.put("zookeeper.znode.parent", "/hbase-unsecure");
    props.put("hbase.zookeeper.property.clientPort",2181);
    conf.put("hbase.config", props);

    LocalCluster cluster = new LocalCluster();

    //Local Mode
    cluster.submitTopology("kafkaTridentTest", conf, topology.build());

    //Submit to Cluster Mode
    //StormSubmitter.submitTopology("kafkaTridentTest", conf, topology.build());

  }

  @SneakyThrows
  public static void main(String args[]){

    Yaml yaml = new Yaml();
    InputStream in = ClassLoader.getSystemResourceAsStream("credentials.yml");
    HBaseWriterTopo kafkaTopo= yaml.loadAs(in, HBaseWriterTopo.class);
    kafkaTopo.getTopology();
  }
}
