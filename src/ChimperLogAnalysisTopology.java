
import java.util.ArrayList;
import java.util.List;
import java.util.Random;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import storm.kafka.BrokerHosts;
import storm.kafka.KafkaConfig;
import storm.kafka.KafkaSpout;
import storm.kafka.SpoutConfig;
import storm.kafka.StaticHosts;
import storm.kafka.StringScheme;
import storm.kafka.ZkHosts;
import backtype.storm.Config;
import backtype.storm.LocalCluster;
import backtype.storm.StormSubmitter;
import backtype.storm.generated.AlreadyAliveException;
import backtype.storm.generated.InvalidTopologyException;
import backtype.storm.spout.SchemeAsMultiScheme;
import backtype.storm.topology.TopologyBuilder;

public class ChimperLogAnalysisTopology {

	private static final Logger logger = LoggerFactory
			.getLogger(ChimperLogAnalysisTopology.class);

	
	public static void main(String[] args) {

		try {

			TopologyBuilder builder = new TopologyBuilder();
			// Topology configuration
			Config conf = new Config();
			conf.setNumAckers(10);
			conf.setMessageTimeoutSecs(300);
			conf.setMaxSpoutPending(100);
			conf.setDebug(true);
			// conf.setMaxTaskParallelism(10);

			BrokerHosts brokerHosts = new ZkHosts(
					"ec2-54-237-148-55.compute-1.amazonaws.com:2181");

			SpoutConfig kafkaConfig = new SpoutConfig(brokerHosts,
					"adnetwork-topic", "/home/ec2-user/software/storm-kafka",
					"1");

			kafkaConfig.stateUpdateIntervalMs = 5000;
			kafkaConfig.scheme = new SchemeAsMultiScheme(new StringScheme());

			// Spout Config 
			builder.setSpout("kafkaLogReadingSpout",
					new KafkaSpout(kafkaConfig), 1);

			builder.setBolt("print", new VerboseCollectorBolt(), 1)
					.setNumTasks(1).shuffleGrouping("kafkaLogReadingSpout");

			builder.setBolt("print2", new VerboseCollectorBolt1(), 1)
					.shuffleGrouping("print");

			/*
			 * builder.setBolt("BatchSizeFilterBolt", new BatchSizeFilterBolt(),
			 * 5).setNumTasks(5).fieldsGrouping("kafkaLogReadingSpout",new
			 * Fields("dateUpToMinute"));
			 * 
			 * builder.setBolt("PersistancePrepreationBolt", new
			 * PersistancePreprationBolt(), 3)
			 * .setNumTasks(3).shuffleGrouping("BatchSizeFilterBolt");
			 * 
			 * builder.setBolt("PersistanceBolt", new PersistanceBolt(), 1)
			 * .setNumTasks(1).shuffleGrouping("PersistancePrepreationBolt");
			 * 
			 * *
			 */

			if (args != null && args.length > 0) {
				// Submit topology
				StormSubmitter.submitTopology("kafkaLog", conf,
						builder.createTopology());

			} else {
				LocalCluster cluster = new LocalCluster();
				cluster.submitTopology("kafkaLog", conf,
						builder.createTopology());
			}
		} catch (AlreadyAliveException | InvalidTopologyException e) {
			logger.error("RequestException", e);
		}
	}

}
