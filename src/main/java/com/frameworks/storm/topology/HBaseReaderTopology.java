package com.frameworks.storm.topology;

import backtype.storm.Config;
import backtype.storm.LocalCluster;
import backtype.storm.StormSubmitter;
import backtype.storm.tuple.Fields;
import com.frameworks.storm.debug.Debug;
import com.frameworks.storm.operation.HBaseFieldGenerator;
import com.frameworks.storm.providers.LineProvider;
import com.frameworks.storm.state.hbase.PermeableHBaseUpdater;
import com.frameworks.storm.state.hbase.standard.HBaseStandardMapperWithTs;
import com.frameworks.storm.state.hbase.standard.HBaseStandardValueMapperWithTs;
import lombok.Setter;
import lombok.SneakyThrows;
import lombok.extern.slf4j.Slf4j;
import org.apache.hadoop.hbase.client.Durability;
import org.apache.storm.hbase.bolt.mapper.HBaseProjectionCriteria;
import org.apache.storm.hbase.trident.state.HBaseQuery;
import org.apache.storm.hbase.trident.state.HBaseState;
import org.apache.storm.hbase.trident.state.HBaseStateFactory;
import org.yaml.snakeyaml.Yaml;
import storm.trident.Stream;
import storm.trident.TridentTopology;
import storm.trident.state.StateFactory;

import java.io.InputStream;
import java.util.Properties;

@Slf4j
@Setter
public class HBaseReaderTopology {

  String zkNodeAddress;
  String brokerNodeAddress;
  String topicName;
  String filePath;
  int batchSize;
  String tableName;
  StateFactory factory;
  String zkQuorum;
  String znodeParent;

  private Fields fields= new Fields("key","family","qualifier","value","ts");

  private void setOptions(){ //set options for HBASE statefactory
    HBaseStandardMapperWithTs tridentHBaseMapper = new HBaseStandardMapperWithTs("key","family","qualifier","value","ts"); //

    HBaseStandardValueMapperWithTs tridentHBaseValueMapper = new HBaseStandardValueMapperWithTs();

    HBaseProjectionCriteria projectionCriteria = new HBaseProjectionCriteria();
    projectionCriteria.addColumn(new HBaseProjectionCriteria.ColumnMetaData("DATA", "count"));

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
    LineProvider sp = new LineProvider(filePath,batchSize); //(path to file,batchsize)

    topology.newStream("spout1", sp.createSpout())
            .each(new Fields(),new HBaseFieldGenerator(),fields)
            .each(new Fields("key"),new Debug(),new Fields())
            .partitionPersist(factory, fields,  new PermeableHBaseUpdater(), new Fields("key"))
            .newValuesStream()
            .stateQuery(topology.newStaticState(factory), new Fields("key"), new HBaseQuery(), new Fields("eventIdKey", "family", "qualifier", "value")); //queries HBASE for all data;

    Config conf = new Config();

    LocalCluster cluster = new LocalCluster();

    Properties props = new Properties();
    props.put("hbase.zookeeper.quorum", "zk0001.dev2.awse1a.datasciences.tmcs,zk0002.dev2.awse1a.datasciences.tmcs,zk0003.dev2.awse1a.datasciences.tmcs");
    props.put("zookeeper.znode.parent", "/hbase-unsecure");
    props.put("zookeeper.znode.parent", "/hbase-unsecure");
    conf.put("hbase.config", props);

    StormSubmitter.submitTopology("kafkaTridentTest", conf, topology.build());
  }

  @SneakyThrows
  public static void main(String args[]){

    Yaml yaml = new Yaml();
    InputStream in = ClassLoader.getSystemResourceAsStream("credentials.yml");
    HBaseReaderTopology hbaseTopo= yaml.loadAs(in, HBaseReaderTopology.class);
    hbaseTopo.getTopology();
  }
}
