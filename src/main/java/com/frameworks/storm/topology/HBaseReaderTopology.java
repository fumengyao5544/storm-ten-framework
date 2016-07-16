package com.frameworks.storm.topology;

import backtype.storm.Config;
import backtype.storm.LocalCluster;
import backtype.storm.StormSubmitter;
import backtype.storm.tuple.Fields;
import com.frameworks.storm.debug.Debug;
import com.frameworks.storm.operation.HBaseFieldGenerator;
import com.frameworks.storm.providers.LineProvider;
import com.frameworks.storm.providers.SpoutProvider;
import com.frameworks.storm.state.hbase.PermeableHBaseUpdater;
import com.frameworks.storm.state.hbase.standard.HBaseStandardMapperWithTs;
import com.frameworks.storm.state.hbase.standard.HBaseStandardValueMapperWithTs;
import lombok.extern.slf4j.Slf4j;
import org.apache.hadoop.hbase.client.Durability;
import org.apache.storm.hbase.bolt.mapper.HBaseProjectionCriteria;
import org.apache.storm.hbase.trident.state.HBaseQuery;
import org.apache.storm.hbase.trident.state.HBaseState;
import org.apache.storm.hbase.trident.state.HBaseStateFactory;
import org.apache.storm.hbase.trident.state.HBaseUpdater;
import storm.trident.Stream;
import storm.trident.TridentTopology;
import storm.trident.state.StateFactory;

import java.util.Properties;

@Slf4j
public class HBaseReaderTopology {

  StateFactory factory;
  private Fields fields= new Fields("key","family","qualifier","value","ts");
  int batchSize = 15;
  String filePath = "src/main/resources/fruitdata";

  private void setOptions(){ //set options for HBASE statefactory
    HBaseStandardMapperWithTs tridentHBaseMapper = new HBaseStandardMapperWithTs("key","family","qualifier","value","ts"); //

    HBaseStandardValueMapperWithTs tridentHBaseValueMapper = new HBaseStandardValueMapperWithTs();

    HBaseProjectionCriteria projectionCriteria = new HBaseProjectionCriteria();
    projectionCriteria.addColumn(new HBaseProjectionCriteria.ColumnMetaData("cf", "count"));

    Durability durability = Durability.SYNC_WAL;

    HBaseState.Options options = new HBaseState.Options()
            .withConfigKey("hbase.config")
            .withDurability(durability)
            .withMapper(tridentHBaseMapper)
            .withProjectionCriteria(projectionCriteria)
            .withRowToStormValueMapper(tridentHBaseValueMapper)
            .withTableName("sem_campaigns");

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

    Stream stream =  topology.newStream("spout1", new SpoutProvider().createSpout());

    Config conf = new Config();

    LocalCluster cluster = new LocalCluster();

    Properties props = new Properties();
    props.put("hbase.zookeeper.quorum", "hw0002.dev1.awse1a.datasciences.tmcs:6667");
    props.put("zookeeper.znode.parent", "/hbase-unsecure");
    conf.put("hbase.config", props);

    StormSubmitter.submitTopology("kafkaTridentTest", conf, topology.build());
  }

  public static void main(String args[]){

    try{new HBaseReaderTopology().getTopology();}
    catch(Exception e){
      e.printStackTrace();
    }
  }
}
