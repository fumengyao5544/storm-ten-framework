package com.frameworks.storm.operation;

import storm.trident.operation.BaseFunction;
import storm.trident.operation.TridentCollector;
import storm.trident.tuple.TridentTuple;

/**
 * Created by christiantest on 5/2/16.
 */
public class TupleFilterSeat extends BaseFunction {

    String filterField;

    public TupleFilterSeat(String filterField) {
        this.filterField = filterField;
    }

    public void execute(TridentTuple tuple,TridentCollector collector){

        if(!tuple.getBooleanByField("isBF")){
            collector.emit(tuple);
        }
    }

    public boolean isKeep(TridentTuple tuple) {
        return tuple.getBooleanByField(filterField);
    }


}