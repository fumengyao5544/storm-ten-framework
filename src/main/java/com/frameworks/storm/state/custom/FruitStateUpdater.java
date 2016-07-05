package com.frameworks.storm.state.custom;
import lombok.extern.slf4j.Slf4j;
import storm.trident.operation.TridentCollector;
import storm.trident.state.BaseStateUpdater;
import storm.trident.tuple.TridentTuple;

import java.util.ArrayList;
import java.util.List;

/**
 * Created by christiangao on 6/14/16.
 */
@Slf4j
public class FruitStateUpdater extends BaseStateUpdater<FruitState> {
    String tsFieldName;
    String msgName;

    public FruitStateUpdater(String msgName, String tsFieldName){
        this.tsFieldName = tsFieldName;
        this.msgName = msgName;
    }

    public void updateState(FruitState state, List<TridentTuple> tuples, TridentCollector collector) {
        List<String> Fruits = new ArrayList<String>();
        for(TridentTuple t: tuples) {
            log.info("TS: "+t.getLongByField(tsFieldName));
            Fruits.add(t.getStringByField(msgName));
        }
        for (TridentTuple tuple : tuples) {
            collector.emit(tuple);
        }
    }
}
