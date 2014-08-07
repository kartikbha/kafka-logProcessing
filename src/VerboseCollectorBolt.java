

import java.util.List;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import backtype.storm.topology.BasicOutputCollector;
import backtype.storm.topology.OutputFieldsDeclarer;
import backtype.storm.topology.base.BaseBasicBolt;
import backtype.storm.tuple.Fields;
import backtype.storm.tuple.Tuple;
import backtype.storm.tuple.Values;

public class VerboseCollectorBolt extends BaseBasicBolt {

	private int countReceivedMessages = 0;


	private static final Logger LOG = LoggerFactory
			.getLogger(VerboseCollectorBolt.class);
	
	public VerboseCollectorBolt() {

	}

	public void prepare(java.util.Map stormConf,
			backtype.storm.task.TopologyContext context) {

	}

	@Override
	public void declareOutputFields(OutputFieldsDeclarer declarer) {
		declarer.declare(new Fields("batch"));
	}

	@Override
	public void execute(Tuple tuple, BasicOutputCollector collector) {
		final String msg = tuple.toString();

		countReceivedMessages++;
		String info = " KARTIK BHATNAGAR recvd: " + countReceivedMessages + " expected: ";
		System.out.println(info + " >>>>>>>>>>>>>" + msg);
        LOG.error(info + " >>>>>>>>>>>>>" + msg);

        Values val = new Values();
     	val.add(msg);
		collector.emit((val));
	}
}
