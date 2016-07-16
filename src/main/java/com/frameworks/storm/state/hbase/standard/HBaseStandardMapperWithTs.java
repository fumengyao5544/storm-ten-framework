package com.frameworks.storm.state.hbase.standard;

import org.apache.storm.hbase.common.ColumnList;
import org.apache.storm.hbase.trident.mapper.TridentHBaseMapper;
import storm.trident.tuple.TridentTuple;

import static org.apache.storm.hbase.common.Utils.toBytes;

public class HBaseStandardMapperWithTs implements TridentHBaseMapper { /*for querying from hbase */
    private static final long serialVersionUID = -5774550203021487254L;
    private String rowKeyField;
    private String columnFamilyField;
    private String qualifierField;
    private String valueField;
    private String timestamp;

    public HBaseStandardMapperWithTs(String rowKeyField, String columnFamilyField, String qualifierField, String timestamp, String valueField){
        this.rowKeyField = rowKeyField;
        this.columnFamilyField = columnFamilyField;
        this.qualifierField = qualifierField;
        this.valueField = valueField;
        this.timestamp = timestamp;
    }

    @Override
    public byte[] rowKey(TridentTuple tuple) {
        return toBytes(tuple.getValueByField(this.rowKeyField));
    }

    @Override
    public ColumnList columns(TridentTuple tuple) { //serializing stuff
        ColumnList cols = new ColumnList();

        cols.addColumn(toBytes(tuple.getValueByField(columnFamilyField)),
                toBytes(tuple.getValueByField(qualifierField)),
                Long.valueOf((String)tuple.getValueByField(timestamp)),
                toBytes(tuple.getValueByField(valueField)));

        return cols;
    }
}
