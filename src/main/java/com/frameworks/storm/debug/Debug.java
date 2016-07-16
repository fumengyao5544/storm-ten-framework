
package com.frameworks.storm.debug;
/*
 * Need Packages:
 * commons-codec-1.4.jar
 *
 * commons-logging-1.1.1.jar
 *
 * hadoop-0.20.2-core.jar
 *
 * hbase-0.90.2.jar
 *
 * log4j-1.2.16.jar
 *
 * zookeeper-3.3.2.jar
 *
 */

import lombok.extern.slf4j.Slf4j;
import storm.trident.operation.BaseFunction;
import storm.trident.operation.TridentCollector;
import storm.trident.tuple.TridentTuple;

@Slf4j
public class Debug extends BaseFunction
{
  @Override
    public void execute(TridentTuple tuple, TridentCollector collector) {

        //System.out.println("TEST: "+rowKey);
        log.info("TUPLE: " + tuple.toString());

    collector.emit(tuple);

    }
}
