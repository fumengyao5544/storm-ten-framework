package com.frameworks.storm.state.hbase.increment;

import backtype.storm.topology.OutputFieldsDeclarer;
import backtype.storm.tuple.Fields;
import backtype.storm.tuple.ITuple;
import backtype.storm.tuple.Values;
import lombok.extern.slf4j.Slf4j;
import org.apache.hadoop.hbase.Cell;
import org.apache.hadoop.hbase.client.Result;
import org.apache.hadoop.hbase.util.Bytes;
import org.apache.storm.hbase.bolt.mapper.HBaseValueMapper;

import java.util.ArrayList;
import java.util.List;

@Slf4j
public class HBaseValueMapperLong implements HBaseValueMapper {
  private static final long serialVersionUID = 4302384551156570816L;

  @Override
  public List<Values> toValues(ITuple iTuple, Result result) throws Exception {
    List<Values> values = new ArrayList<Values>();

    for(Cell cell : result.rawCells()) {

      String qualifier = Bytes.toString(cell.getQualifierArray(), cell.getQualifierOffset(), cell.getQualifierLength());
      if(!qualifier.equals("BLOOM")){
        String key = Bytes.toString(cell.getRowArray(), cell.getRowOffset(), cell.getRowLength());
        String family = Bytes.toString(cell.getFamilyArray(), cell.getFamilyOffset(), cell.getFamilyLength());
        long value = Bytes.toLong(cell.getValueArray(), cell.getValueOffset(), cell.getValueLength());
        long timestamp = cell.getTimestamp();
        values.add(new Values(key, family, qualifier, value, timestamp));}
    }
    log.info("emit to hbase: {}", values);
    return values;
  }

  @Override
  public void declareOutputFields(OutputFieldsDeclarer declarer) {
    declarer.declare(new Fields("cellRowKey", "cellColumnFamily", "cellQualifier", "cellValue", "cellTimestamp"));
  }
}
