package com.frameworks.storm.providers;

import backtype.storm.Config;
import backtype.storm.task.TopologyContext;
import backtype.storm.tuple.Fields;
import backtype.storm.tuple.Values;
import lombok.RequiredArgsConstructor;
import lombok.Setter;
import lombok.extern.slf4j.Slf4j;
import storm.trident.operation.TridentCollector;
import storm.trident.spout.IBatchSpout;

import java.text.SimpleDateFormat;
import java.util.*;

@Slf4j
@Setter
public class SpoutProvider {
  public static final Fields FIELDS = new Fields("timestamp");

  private int batchSize;
  private int tickerPause; //pause in milliseconds between each tuple

  public IBatchSpout createSpout() {
    return new Spout(batchSize,tickerPause);
  }

  @RequiredArgsConstructor
  protected static class Spout implements IBatchSpout {
    private static final long serialVersionUID = -3587144552523719158L;

    //private final long delay;
    private final int batchSize;
    private final int tickerPause;


    private long i;
    private long lastRun = 0L;
    private List<Map<String, String>> eventsBatch = new ArrayList<>();

    @Override
    public void open(Map conf, TopologyContext context) {
      i = 0;
    }

    public Values getValues(){
      String str = String.valueOf(System.currentTimeMillis());

      return(new Values(str));
    }

    @Override
    public void emitBatch(long batchId, TridentCollector collector) {

        for (i = 1; i<= batchSize;i++) {

          try{Thread.sleep(tickerPause);}catch(InterruptedException ex){Thread.currentThread().interrupt();}
          collector.emit(new Values(getValues()));
        }
        return;
    }

    @Override
    public void ack(long batchId) {

    }

    @Override
    public void close() {
      i = 0;
    }

    @Override
    public Map getComponentConfiguration() {
      Config conf = new Config();
      conf.setMaxTaskParallelism(1);
      return conf;
    }

    @Override
    public Fields getOutputFields() {
      return FIELDS;
    }
  }
}
