package com.poc.standard;

import java.io.IOException;
import java.text.ParseException;
import java.text.SimpleDateFormat;
import java.util.Map;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import backtype.storm.spout.SpoutOutputCollector;
import backtype.storm.task.TopologyContext;
import backtype.storm.topology.BasicOutputCollector;
import backtype.storm.topology.OutputFieldsDeclarer;
import backtype.storm.topology.base.BaseBasicBolt;
import backtype.storm.tuple.Fields;
import backtype.storm.tuple.Tuple;
import backtype.storm.tuple.Values;

import com.fasterxml.jackson.core.JsonFactory;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.fasterxml.jackson.databind.node.ObjectNode;

public class KafkaMsgBolt extends BaseBasicBolt {

	private static final Logger LOG = LoggerFactory
			.getLogger(KafkaMsgBolt.class);

	
	public KafkaMsgBolt() {
	}

	@Override
	public void execute(Tuple arg0, BasicOutputCollector collector) {

		/*
		 * 
		 * Msg Receiving format from kafka
		 * 
		 * [{ "timestamp":1407394935908, "publisher":"publisher_0",
		 * "advertiser":"advertiser_2" ,"website":"website_7689.com",
		 * "geo":"MI", "bid":0.14403514698631104, "cookie":"cookie_7447"}]
		 */

		String inputValue = arg0.getString(0).toString();
		ObjectNode obj2 = null;
		try {
			obj2 = mapper.readValue(inputValue,ObjectNode.class);
		} catch (IOException e) {
		}
	
		//System.out.println(" obj2-------> "+obj2);
		
		Values val = new Values();
		String geo = obj2.get("geo").asText();
		val.add(geo);
		String pub = obj2.get("publisher").asText();
		val.add(pub);
        val.add(obj2.get("advertiser").asText());
		val.add(obj2.get("website").asText());
		double bid = obj2.get("bid").asDouble();
		val.add(new Double(bid).toString());
		val.add(obj2.get("cookie").asText());
        String timeStampStr = obj2.get("timestamp").asText();
		//System.out.println(" timeStamp---str------> "+timeStampStr);
		long timeStamp = Long.parseLong(timeStampStr);
		//System.out.println(" timeStamp---------> "+timeStamp);
	    SimpleDateFormat DATE_FORMAT = new SimpleDateFormat(
				"yyyy-MM-dd hh:mm:ss aa");
     
        String fullTime = DATE_FORMAT.format(timeStamp);
        val.add(fullTime);
        val.add(geo + pub + getDateUptoMinute(fullTime));
	
        //System.out.println(" val ---------> "+val);
        collector.emit(val);
       
	}

	@Override
	public void declareOutputFields(OutputFieldsDeclarer declarer) {
		declarer.declare(new Fields("geo", "pub", "adv", "website", "bid",
				"cookie", "date", "dateUpToMinute"));
	}

	private static JsonFactory factory;
	ObjectMapper mapper;

	@Override
	public void prepare(Map stormConf, TopologyContext context) {

		mapper = new ObjectMapper(factory);
	}

	// --- Helper methods --- //
	// SimpleDateFormat is not thread safe!
	private SimpleDateFormat DATE_FORMAT = new SimpleDateFormat(
			"yyyy-MM-dd hh:mm:ss aa");

	private String getDateUptoMinute(String field) {
		SimpleDateFormat df = new SimpleDateFormat("yyyy-MM-dd hh:mm");
		String dateUptoMinute = null;
		try {
			dateUptoMinute = df.format(df.parse(field));
		} catch (ParseException e) {
		}
		return dateUptoMinute;
	}

}
