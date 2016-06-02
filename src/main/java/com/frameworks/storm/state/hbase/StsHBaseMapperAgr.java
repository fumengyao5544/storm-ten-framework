package com.frameworks.storm.state.hbase;

import org.apache.storm.hbase.common.ColumnList;
import org.apache.storm.hbase.trident.mapper.TridentHBaseMapper;
import storm.trident.tuple.TridentTuple;

import static org.apache.storm.hbase.common.Utils.toBytes;

public class StsHBaseMapperAgr implements TridentHBaseMapper {
  private static final long serialVersionUID = -5774550203021487254L;
  private String rowKeyField;
  private String columnFamilyField;
  private String qualifierField;
  private String valueField;

  public StsHBaseMapperAgr(String rowKeyField, String columnFamilyField, String qualifierField, String valueField) {
    this.rowKeyField = rowKeyField;
    this.columnFamilyField = columnFamilyField;
    this.qualifierField = qualifierField;
    this.valueField = valueField;
  }

  @Override
  public byte[] rowKey(TridentTuple tuple) {
    return toBytes(tuple.getValueByField(this.rowKeyField));
  }

  @Override
  public ColumnList columns(TridentTuple tuple) {
    ColumnList cols = new ColumnList();

    cols.addCounter(
        toBytes(tuple.getValueByField(columnFamilyField)),
        toBytes(tuple.getValueByField(qualifierField)),
        tuple.getLongByField(valueField));
    return cols;

  }
}


