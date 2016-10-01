package com.frameworks.storm.state.hbase.standard;

import backtype.storm.topology.FailedException;
import backtype.storm.tuple.Values;
import com.google.common.collect.Lists;
import java.io.Serializable;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.Iterator;
import java.util.List;
import java.util.Map;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.hbase.HBaseConfiguration;
import org.apache.hadoop.hbase.client.Durability;
import org.apache.hadoop.hbase.client.Result;
import org.apache.storm.hbase.bolt.mapper.HBaseProjectionCriteria;
import org.apache.storm.hbase.bolt.mapper.HBaseValueMapper;
import org.apache.storm.hbase.common.ColumnList;
import org.apache.storm.hbase.common.HBaseClient;
import org.apache.storm.hbase.trident.mapper.TridentHBaseMapper;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import storm.trident.operation.TridentCollector;
import storm.trident.state.State;
import storm.trident.tuple.TridentTuple;

public class HBaseStandardState implements State {
    private static final Logger LOG = LoggerFactory.getLogger(HBaseStandardState.class);
    private HBaseStandardState.Options options;
    private HBaseClient hBaseClient;
    private Map map;
    private int numPartitions;
    private int partitionIndex;

    protected HBaseStandardState(Map map, int partitionIndex, int numPartitions, HBaseStandardState.Options options) {
        this.options = options;
        this.map = map;
        this.partitionIndex = partitionIndex;
        this.numPartitions = numPartitions;
    }

    protected void prepare() {
        Configuration hbConfig = HBaseConfiguration.create();
        Map conf = (Map)this.map.get(this.options.configKey);
        if(conf == null) {
            LOG.info("HBase configuration not found using key \'" + this.options.configKey + "\'");
            LOG.info("Using HBase config from first hbase-site.xml found on classpath.");
        } else {
            if(conf.get("hbase.rootdir") == null) {
                LOG.warn("No \'hbase.rootdir\' value found in configuration! Using HBase defaults.");
            }

            Iterator hbaseConfMap = conf.keySet().iterator();

            while(hbaseConfMap.hasNext()) {
                String key = (String)hbaseConfMap.next();
                hbConfig.set(key, String.valueOf(conf.get(key)));
            }
        }

        System.out.println("hbase.zookeeper.quorum"+hbConfig.get("hbase.zookeeper.quorum"));

        HashMap hbaseConfMap1 = new HashMap(conf);
        hbaseConfMap1.put("topology.auto-credentials", this.map.get("topology.auto-credentials"));
        this.hBaseClient = new HBaseClient(hbaseConfMap1, hbConfig, this.options.tableName);
    }

    public void beginCommit(Long aLong) {
        LOG.debug("beginCommit is noop.");
    }

    public void commit(Long aLong) {
        LOG.debug("commit is noop.");
    }

    public void updateState(List<TridentTuple> tuples, TridentCollector collector) {
        ArrayList mutations = Lists.newArrayList();
        Iterator e = tuples.iterator();

        while(e.hasNext()) {
            TridentTuple tuple = (TridentTuple)e.next();
            byte[] rowKey = this.options.mapper.rowKey(tuple);
            ColumnList cols = this.options.mapper.columns(tuple);
            mutations.addAll(this.hBaseClient.constructMutationReq(rowKey, cols, this.options.durability));
        }

        try {
            this.hBaseClient.batchMutate(mutations);
        } catch (Exception var8) {
            collector.reportError(var8);
            throw new FailedException(var8);
        }
    }

    public List<List<Values>> batchRetrieve(List<TridentTuple> tridentTuples) {
        ArrayList batchRetrieveResult = Lists.newArrayList();
        ArrayList gets = Lists.newArrayList();
        Iterator e = tridentTuples.iterator();

        while(e.hasNext()) {
            TridentTuple i = (TridentTuple)e.next();
            byte[] result = this.options.mapper.rowKey(i);
            gets.add(this.hBaseClient.constructGetRequests(result, this.options.projectionCriteria));
        }

        try {
            Result[] var10 = this.hBaseClient.batchGet(gets);

            for(int var11 = 0; var11 < var10.length; ++var11) {
                Result var12 = var10[var11];
                TridentTuple tuple = (TridentTuple)tridentTuples.get(var11);
                List values = this.options.rowToStormValueMapper.toValues(tuple, var12);
                batchRetrieveResult.add(values);
            }

            return batchRetrieveResult;
        } catch (Exception var9) {
            LOG.warn("Batch get operation failed. Triggering replay.", var9);
            throw new FailedException(var9);
        }
    }

    public static class Options implements Serializable {
        private TridentHBaseMapper mapper;
        private Durability durability;
        private HBaseProjectionCriteria projectionCriteria;
        private HBaseValueMapper rowToStormValueMapper;
        private String configKey;
        private String tableName;

        public Options() {
            this.durability = Durability.SKIP_WAL;
        }

        public HBaseStandardState.Options withDurability(Durability durability) {
            this.durability = durability;
            return this;
        }

        public HBaseStandardState.Options withProjectionCriteria(HBaseProjectionCriteria projectionCriteria) {
            this.projectionCriteria = projectionCriteria;
            return this;
        }

        public HBaseStandardState.Options withConfigKey(String configKey) {
            this.configKey = configKey;
            return this;
        }

        public HBaseStandardState.Options withTableName(String tableName) {
            this.tableName = tableName;
            return this;
        }

        public HBaseStandardState.Options withRowToStormValueMapper(HBaseValueMapper rowToStormValueMapper) {
            this.rowToStormValueMapper = rowToStormValueMapper;
            return this;
        }

        public HBaseStandardState.Options withMapper(TridentHBaseMapper mapper) {
            this.mapper = mapper;
            return this;
        }
    }
}
