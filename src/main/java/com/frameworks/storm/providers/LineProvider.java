package com.frameworks.storm.providers;

import backtype.storm.Config;
import backtype.storm.task.TopologyContext;
import backtype.storm.tuple.Fields;
import backtype.storm.tuple.Values;
import lombok.Setter;
import lombok.extern.slf4j.Slf4j;
import storm.trident.operation.TridentCollector;
import storm.trident.spout.IBatchSpout;

import java.io.BufferedReader;
import java.io.FileReader;
import java.io.IOException;
import java.util.Map;

@Slf4j
@Setter
public class LineProvider {
  private String fileName;
  private int batchSize;
  public LineProvider(String fileName,int batchSize){

    this.fileName = fileName;
    this.batchSize = batchSize;
  }
  public static final Fields FIELDS = new Fields("str","ts");

  public IBatchSpout createSpout() {
    return new Spout(fileName,batchSize);
  }

  //@RequiredArgsConstructor
  protected static class Spout implements IBatchSpout {

    private String fileName;
    private int batchSize;
    public Spout(String fileName,int batchSize){
      this.batchSize = batchSize;
      this.fileName = fileName;
    }
    private static final long serialVersionUID = -3587144552523719158L;

    //private final long delay;
    private int tickerPause=1000; //pause in milliseconds between each tuple
    FileReader fileReader;
    TopologyContext context;
    BufferedReader bufferedReader;

    @Override
    public void open(Map conf, TopologyContext context) {

      this.context = context;
      try {
        this.fileReader = new FileReader(this.fileName);
        log.info("preparing file reader");
        this.bufferedReader = new BufferedReader(fileReader);
      } catch (IOException ex) {
        ex.printStackTrace();
      }
    }


    public long getTimestamp() {return System.nanoTime();}

    @Override
    public void emitBatch(long batchId, TridentCollector collector) {

      //this.bufferedReader = new BufferedReader(fileReader);

      try {
        for (int i = 1; (i <= batchSize); i++) {
          final long timestamp = getTimestamp();
          final String line = bufferedReader.readLine();
          if (line == null) {
            break;
          }
          //log.info("Emitting {} with timestamp {}", line, timestamp);
          collector.emit(new Values(line, timestamp));
        }
        //log.info("Batchsize"+batchSize);
        //Thread.sleep(tickerPause);
        return;
      }
      catch (IOException ex) {ex.printStackTrace();}
//      catch(InterruptedException ex){Thread.currentThread().interrupt();}
    }

    @Override
    public void ack(long batchId) {

    }

    @Override
    public void close() {
      try{bufferedReader.close();}catch(Exception e){
        log.info("BufferReader close error");
      }
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