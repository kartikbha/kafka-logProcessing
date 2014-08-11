package com.poc.standard.topology.cluster;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import storm.kafka.BrokerHosts;
import storm.kafka.KafkaSpout;
import storm.kafka.SpoutConfig;
import storm.kafka.StringScheme;
import storm.kafka.ZkHosts;
import backtype.storm.Config;
import backtype.storm.LocalCluster;
import backtype.storm.StormSubmitter;
import backtype.storm.generated.AlreadyAliveException;
import backtype.storm.generated.InvalidTopologyException;
import backtype.storm.spout.SchemeAsMultiScheme;
import backtype.storm.topology.TopologyBuilder;
import backtype.storm.tuple.Fields;

import com.poc.standard.AggregatorBolt;
import com.poc.standard.KafkaMsgBolt;
import com.poc.standard.PersistanceBolt;
import com.poc.standard.PersistancePreprationBolt;

public class KafkaLogAnalysisTopology {

	private static final Logger logger = LoggerFactory
			.getLogger(KafkaLogAnalysisTopology.class);

	public static void main(String[] args) throws Exception {

		try {

			TopologyBuilder builder = new TopologyBuilder();
			// Topology configuration
			Config conf = new Config();
			
			conf.setMessageTimeoutSecs(30000);
			conf.setMaxSpoutPending(500000);
			conf.setDebug(false);

			int kafkaReadingSpoutExecutor = 1;

			int kafkaMsgBoltExecutor = 1;
			int kafkaMsgBoltTask = 1;

			int aggregatorBoltExecutor = 1;
			int aggregatorBoltTask = 1;

			int persistancePrepreationBoltExecutor = 1;
			int persistancePrepreationBoltTask = 1;

			int persistanceBoltExector = 1;
			int persistanceBoltTask = 1;
			int worker = 4;
	        int acker = 10;
			
			System.out.println(" args.length " + args.length);
            if (args.length != 0) {
				if (args.length != 11) {
					throw new Exception(
							"Needed  total 11 arguments to run in cluster mode else don't give any args.");
				}
			}
			if (args != null && args.length > 0) {

				kafkaReadingSpoutExecutor  = Integer.parseInt(args[0]);
				
				kafkaMsgBoltExecutor = Integer.parseInt(args[1]);
				kafkaMsgBoltTask = Integer.parseInt(args[2]);
				
				aggregatorBoltExecutor = Integer.parseInt(args[3]);
				aggregatorBoltTask = Integer.parseInt(args[4]);
				
				persistancePrepreationBoltExecutor = Integer.parseInt(args[5]);
				persistancePrepreationBoltTask = Integer.parseInt(args[6]);

				persistanceBoltExector = Integer.parseInt(args[7]);
				persistanceBoltTask = Integer.parseInt(args[8]);
				
				worker = Integer.parseInt(args[9]);
				
				acker = Integer.parseInt(args[10]);
				
			}
			
			conf.setNumAckers(acker);
			conf.setNumWorkers(worker);
			
			conf.put(Config.TOPOLOGY_EXECUTOR_SEND_BUFFER_SIZE,16384);
            conf.put(Config.TOPOLOGY_EXECUTOR_RECEIVE_BUFFER_SIZE,16384);
         
			
            BrokerHosts brokerHosts = new ZkHosts(
					"ec2-54-237-148-55.compute-1.amazonaws.com:2181");

			SpoutConfig kafkaConfig = new SpoutConfig(brokerHosts,
					"adnetwork-topic", "/home/ec2-user/software/storm-kafka",
					"1");

			//kafkaConfig.stateUpdateIntervalMs = 2000;
			kafkaConfig.scheme = new SchemeAsMultiScheme(new StringScheme());

			// Spout Config
			builder.setSpout("kafkaLogReadingSpout",
					new KafkaSpout(kafkaConfig), kafkaReadingSpoutExecutor);

			builder.setBolt("kafkaMsgBolt", new KafkaMsgBolt(),
					kafkaMsgBoltExecutor).setNumTasks(kafkaMsgBoltTask)
					.shuffleGrouping("kafkaLogReadingSpout");

			builder.setBolt("aggregatorBolt", new AggregatorBolt(),
					aggregatorBoltExecutor)
					.setNumTasks(aggregatorBoltTask)
					.fieldsGrouping("kafkaMsgBolt",
							new Fields("dateUpToMinute"));

			builder.setBolt("persistancePrepreationBolt",
					new PersistancePreprationBolt(),
					persistancePrepreationBoltExecutor)
					.setNumTasks(persistancePrepreationBoltTask)
					.shuffleGrouping("aggregatorBolt");

			builder.setBolt("persistanceBolt", new PersistanceBolt(),
					persistanceBoltExector).setNumTasks(persistanceBoltTask)
					.shuffleGrouping("persistancePrepreationBolt");

			if (args != null && args.length > 0) {
				// Submit topology
				StormSubmitter.submitTopology("kafka-logProcessing", conf,
						builder.createTopology());

			} else {
				LocalCluster cluster = new LocalCluster();
				cluster.submitTopology("kafka-logProcessing", conf,
						builder.createTopology());
			}
		} catch (AlreadyAliveException | InvalidTopologyException e) {
			logger.error("RequestException", e);
		}
	}

}
