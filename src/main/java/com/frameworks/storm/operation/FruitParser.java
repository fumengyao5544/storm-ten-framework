
package com.frameworks.storm.operation;
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

import backtype.storm.tuple.Values;
import com.frameworks.storm.objects.Fruit;
import com.google.gson.Gson;
import com.google.gson.GsonBuilder;
import lombok.extern.slf4j.Slf4j;
import storm.trident.operation.BaseFunction;
import storm.trident.operation.TridentCollector;
import storm.trident.tuple.TridentTuple;

@Slf4j
public class FruitParser extends BaseFunction
{
  @Override
    public void execute(TridentTuple tuple, TridentCollector collector) {

      Gson gson = new GsonBuilder().create();
      Fruit fruit = gson.fromJson(tuple.getStringByField("str"), Fruit.class);
     collector.emit(new Values(fruit.getFruitId(),fruit.getFruit(),fruit.getColor(),fruit.getWeight()));

    }
}
