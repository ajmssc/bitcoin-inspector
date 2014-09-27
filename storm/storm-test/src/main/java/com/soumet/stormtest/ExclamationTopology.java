/**
 * Licensed to the Apache Software Foundation (ASF) under one
 * or more contributor license agreements.  See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership.  The ASF licenses this file
 * to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License.  You may obtain a copy of the License at
 *
 * http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */
package com.soumet.stormtest;



import com.soumet.stormtest.Bitcoin.TransactionFull;
import com.soumet.stormtest.Bitcoin.RecordIn;
import com.soumet.stormtest.Bitcoin.RecordOut;
import org.apache.commons.codec.binary.Base64;
import org.apache.commons.codec.binary.StringUtils;

import backtype.storm.Config;
import backtype.storm.LocalCluster;
import backtype.storm.StormSubmitter;
import backtype.storm.task.OutputCollector;
import backtype.storm.task.TopologyContext;
import backtype.storm.testing.TestWordSpout;
import backtype.storm.topology.OutputFieldsDeclarer;
import backtype.storm.topology.TopologyBuilder;
import backtype.storm.topology.base.BaseRichBolt;
import backtype.storm.tuple.Fields;
import backtype.storm.tuple.Tuple;
import backtype.storm.tuple.Values;
import backtype.storm.utils.Utils;


import com.soumet.stormtest.KafkaSpout;

import java.util.Map;
import java.util.Arrays;
import java.util.List;
import java.util.Properties;

/**
 * This is a basic example of a Storm topology.
 */
public class ExclamationTopology {

  public static class ExclamationBolt extends BaseRichBolt {
    OutputCollector _collector;

    @Override
    public void prepare(Map conf, TopologyContext context, OutputCollector collector) {
      _collector = collector;
    }

    @Override
    public void execute(Tuple tuple) {
      //
      byte[] message = (byte [])tuple.getValueByField("bytes");
      String test = new String(message);

      try {
        TransactionFull mytransaction = TransactionFull.parseFrom(Base64.decodeBase64(test));
        //context.write(new Text(mytransaction.getHash()), new IntWritable(1));
        _collector.emit(tuple, new Values(mytransaction.getTxid()));
      } catch (Exception e) {
        System.out.println("*** exception:");
        e.printStackTrace();
      }

      //_collector.emit(tuple, new Values(tuple.getString(0) + "!!!"));
      //_collector.emit(tuple, new Values(String(tuple.getValueByField("bytes")) + "!!!"));

      _collector.ack(tuple);
    }

    @Override
    public void declareOutputFields(OutputFieldsDeclarer declarer) {
      declarer.declare(new Fields("word"));
    }


  }

  public static void main(String[] args) throws Exception {

   


    TopologyBuilder builder = new TopologyBuilder();

    // builder.setSpout("word", new TestWordSpout(), 1);
    builder.setSpout("kafka", new KafkaSpout());
    builder.setBolt("exclaim1", new ExclamationBolt()).shuffleGrouping("kafka");
    // builder.setBolt("exclaim2", new ExclamationBolt(), 1).shuffleGrouping("exclaim1");

    Config conf = new Config();
    conf.setDebug(true);
    // configure kafka spout (values are available as constants on ConfigUtils)
    conf.put("kafka.spout.topic", "realtime");
    conf.put("group.id", "realtime");

    // kafka consumer configuration, see below
    conf.put("kafka.zookeeper.connect", "localhost:2181");
    conf.put("kafka.consumer.timeout.ms", 100);


    if (args != null && args.length > 0) {
      conf.setNumWorkers(1);

      StormSubmitter.submitTopologyWithProgressBar(args[0], conf, builder.createTopology());
    }
    else {

      LocalCluster cluster = new LocalCluster();
      cluster.submitTopology("test", conf, builder.createTopology());
      Utils.sleep(10000);
      cluster.killTopology("test");
      cluster.shutdown();
    }
  }
}
