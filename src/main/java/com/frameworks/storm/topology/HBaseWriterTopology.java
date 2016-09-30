package com.frameworks.storm.topology;

import backtype.storm.Config;
import backtype.storm.LocalCluster;
import backtype.storm.StormSubmitter;
import backtype.storm.spout.SchemeAsMultiScheme;
import backtype.storm.tuple.Fields;
import com.frameworks.storm.debug.Debug;
import com.frameworks.storm.operation.HBaseFieldGenerator;
import com.frameworks.storm.providers.SpoutProvider;
import com.frameworks.storm.state.hbase.standard.HBaseStandardMapperWithTs;
import com.frameworks.storm.state.hbase.standard.HBaseStandardValueMapperWithTs;
import lombok.Setter;
import lombok.SneakyThrows;
import lombok.extern.slf4j.Slf4j;
import org.apache.hadoop.hbase.client.Durability;
import org.apache.storm.hbase.bolt.mapper.HBaseProjectionCriteria;
import org.apache.storm.hbase.bolt.mapper.HBaseValueMapper;
import org.apache.storm.hbase.trident.mapper.SimpleTridentHBaseMapper;
import org.apache.storm.hbase.trident.mapper.TridentHBaseMapper;
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
public class HBaseWriterTopology {

  String zkNodeAddress;
  String brokerNodeAddress;
  String topicName;
  String filePath;
  int batchSize;
  String hbaseNodeAddress;
  String tableName;
  StateFactory factory;

  private Fields fields= new Fields("key","family","qualifier","value","ts");

  private void setOptions(){
    HBaseStandardMapperWithTs tridentHBaseMapper = new HBaseStandardMapperWithTs("key","family","qualifier","value","ts");

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

  private void getTopology()throws Exception{
    TridentTopology topology = new TridentTopology();
    topology.newStream("spout1", new SpoutProvider().createSpout())
            .each(new Fields(),new HBaseFieldGenerator(),fields)
            .each(new Fields("key"),new Debug(),new Fields())
            .partitionPersist(factory, fields,  new HBaseUpdater(), new Fields());

    Stream stream =  topology.newStream("spout1", new SpoutProvider().createSpout());

    Config conf = new Config();

    LocalCluster cluster = new LocalCluster();

    Properties props = new Properties();
    props.put("hbase.zookeeper.quorum", hbaseNodeAddress);
    props.put("zookeeper.znode.parent", "/hbase-unsecure");
    conf.put("hbase.config", props);

    //cluster.submitTopology("kafkaTridentTest", conf, topology.build());
    StormSubmitter.submitTopology("kafkaTridentTest", conf, topology.build());
  }

  @SneakyThrows
  public static void main(String args[]){
    Yaml yaml = new Yaml();
    InputStream in = ClassLoader.getSystemResourceAsStream("credentials.yml");
    HBaseWriterTopology hbaseTopo= yaml.loadAs(in, HBaseWriterTopology.class);
    hbaseTopo.getTopology();
  }
}
