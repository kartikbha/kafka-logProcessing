

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import backtype.storm.topology.BasicOutputCollector;
import backtype.storm.topology.OutputFieldsDeclarer;
import backtype.storm.topology.base.BaseBasicBolt;
import backtype.storm.tuple.Tuple;

public class VerboseCollectorBolt1 extends BaseBasicBolt {

	private int countReceivedMessages = 0;


	private static final Logger LOG = LoggerFactory
			.getLogger(VerboseCollectorBolt1.class);
	
	public VerboseCollectorBolt1() {

	}

	public void prepare(java.util.Map stormConf,
			backtype.storm.task.TopologyContext context) {

	}

	@Override
	public void declareOutputFields(OutputFieldsDeclarer declarer) {
	}

	@Override
	public void execute(Tuple tuple, BasicOutputCollector collector) {
		final String msg = (String) tuple.getValueByField("batch");

		countReceivedMessages++;
		String info = " KARTIK BHATNAGAR recvd: " + countReceivedMessages + " expected: ";
		System.out.println(info + " >>>>>>>>>>>>>" + msg);
    	LOG.error(info + " >>>>>>>>>>>>>" + msg);
	}
}
