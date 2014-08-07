package com.poc.standard;

import java.text.ParseException;
import java.text.SimpleDateFormat;
import java.util.ArrayList;
import java.util.Date;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import backtype.storm.task.TopologyContext;
import backtype.storm.topology.BasicOutputCollector;
import backtype.storm.topology.OutputFieldsDeclarer;
import backtype.storm.topology.base.BaseBasicBolt;
import backtype.storm.tuple.Fields;
import backtype.storm.tuple.Tuple;
import backtype.storm.tuple.Values;

public class AggregatorBolt extends BaseBasicBolt {

	private static final Logger LOG = LoggerFactory
			.getLogger(AggregatorBolt.class);

	// List<String> keyGeoPubTimeUpToMinuteList = null;
	List<String> keyGeoPubFullTimeList = null;
	Map<String, Map<String, List<List<String>>>> tmpholderForGeoPubTimeFreqMap = null;
	Map<String, List<List<String>>> geoPubTimeHolderMap = null;
	String previousKey = null;
	String currentKey = null;
	int batchSize;
	Integer id;
	String name;

	@Override
	public void prepare(Map stormConf, TopologyContext context) {
		// keyGeoPubTimeUpToMinuteList = new ArrayList<String>();
		keyGeoPubFullTimeList = new ArrayList<String>();
		tmpholderForGeoPubTimeFreqMap = new HashMap<String, Map<String, List<List<String>>>>();

		this.name = context.getThisComponentId();
		this.id = context.getThisTaskId();
	}

	@Override
	public void execute(Tuple input, BasicOutputCollector collector) {
		processEmit(collector);

		// declarer.declare(new Fields("geo", "pub", "adv", "website",
		// "bid", "cookie", "date", "dateUpToMinute"));

		//System.out.println(" ........ 1 .. ");
		List<String> logRow = new ArrayList<String>();
		// geo
		String geo = input.getString(0);
		logRow.add(geo);
		//System.out.println(" ........ 2. ");
		// pub
		String pub = input.getString(1);
		logRow.add(pub);
		// website
		String website = input.getString(3);
		logRow.add(website);
		// bid
		String bid = input.getString(4);
		logRow.add(bid);
		// date
		String keyTime = input.getString(6);
		logRow.add(keyTime);
		//System.out.println(" ........ 3.. ");
		String keyGeoPub = geo + "~" + pub;
		String keyGeoPubFullTime = keyGeoPub + "~" + keyTime;
		String keyGeoPubMinute = keyGeoPub + "~" + getDateUptoMinute(keyTime);
		//System.out.println(" ........ 4.. ");
		if (keyGeoPubFullTimeList.size() > 0
				&& keyGeoPubFullTimeList.contains(keyGeoPubFullTime)
				&& tmpholderForGeoPubTimeFreqMap.containsKey(keyGeoPubMinute)) {
			// here you are getting duplicate, count these duplicates.
			// this will become candidates of total impression
			// System.out.println(" duplicate... " + keyGeoPubFullTimePub);
			geoPubTimeHolderMap = tmpholderForGeoPubTimeFreqMap
					.get(keyGeoPubMinute);
			List<List<String>> holderList = geoPubTimeHolderMap
					.get(keyGeoPubMinute + "impression");
			// System.out.println(" holderList... " + holderList);
			int total = Integer.parseInt(holderList.get(0).get(0)) + 1;
			// System.out.println(" total... " + total);
			holderList.get(0).remove(0);
			holderList.get(0).add(0, (new Integer(total)).toString());
			// System.out.println("total holderList ...... "+holderList);
			geoPubTimeHolderMap.put(keyGeoPubMinute + "impression", holderList);

		} else {
			// you got key with slight differences in second.
			if (keyGeoPubFullTimeList.size() > 0
					&& CheckStartWithInList(keyGeoPubFullTimeList,
							keyGeoPubMinute)
					&& tmpholderForGeoPubTimeFreqMap
							.containsKey(keyGeoPubMinute)) {
				// These rows of logs can go for analysis together
				// club them in the single hash map.
				geoPubTimeHolderMap = tmpholderForGeoPubTimeFreqMap
						.get(keyGeoPubMinute);
				// geoPubTimeFreqMap.
				List<List<String>> holderList = geoPubTimeHolderMap
						.get(keyGeoPubMinute);
				holderList.add(logRow);
				geoPubTimeHolderMap.put(keyGeoPubMinute, holderList);
				// impressions...
				List<List<String>> holderList1 = geoPubTimeHolderMap
						.get(keyGeoPubMinute + "impression");
				int total = Integer.parseInt(holderList1.get(0).get(0)) + 1;
				holderList1.get(0).remove(0);
				holderList1.get(0).add(0, (new Integer(total)).toString());
				// System.out.println("total holderList ...... "+holderList);
				geoPubTimeHolderMap.put(keyGeoPubMinute + "impression",
						holderList1);
				tmpholderForGeoPubTimeFreqMap.put(keyGeoPubMinute,
						geoPubTimeHolderMap);

			} else { // you got brand new key

				geoPubTimeHolderMap = new HashMap<String, List<List<String>>>();
				List<List<String>> holderList = new ArrayList<List<String>>();
				holderList.add(logRow);
				geoPubTimeHolderMap.put(keyGeoPubMinute, holderList);
				// impression
				List<List<String>> holderList1 = new ArrayList<List<String>>();
				List<String> impression = new ArrayList<String>();
				impression.add(0, new Integer(1).toString());
				holderList1.add(impression);
				geoPubTimeHolderMap.put(keyGeoPubMinute + "impression",
						holderList1);
				tmpholderForGeoPubTimeFreqMap.put(keyGeoPubMinute,
						geoPubTimeHolderMap);
			}
			keyGeoPubFullTimeList.add(keyGeoPubFullTime);
		}
	}

	private void processEmit(BasicOutputCollector collector) {
		// TODO Auto-generated method stub
		SimpleDateFormat DATE_FORMAT = new SimpleDateFormat(
				"yyyy-MM-dd hh:mm:ss aa");
		String currentTime = DATE_FORMAT.format(System.currentTimeMillis());
		for (int i = 0; i < keyGeoPubFullTimeList.size(); i++) {
			String[] geoPubFullTimeKey = keyGeoPubFullTimeList.get(i)
					.split("~");
			String geo = geoPubFullTimeKey[0];
			String pub = geoPubFullTimeKey[1];
			String fullTime = geoPubFullTimeKey[2];
			long diff = getFullTimeByDate(currentTime).getTime()
					- getFullTimeByDate(fullTime).getTime();
			long diffSeconds = diff / 1000 % 60;
			String geoPubMinuteKey = geo + "~" + pub + "~"
					+ getDateUptoMinute(fullTime);
			if (diffSeconds > 10
					&& tmpholderForGeoPubTimeFreqMap
							.containsKey(geoPubMinuteKey)) {
				Values val = new Values();
				val.add(tmpholderForGeoPubTimeFreqMap.get(geoPubMinuteKey));
				// emit
				collector.emit(val);
				tmpholderForGeoPubTimeFreqMap.remove(geoPubMinuteKey);
				for (int index = 0; index < prepareStartWithInList(
						keyGeoPubFullTimeList, geoPubMinuteKey).size(); index++) {
					List<String> currentIndexes = prepareStartWithInList(
							keyGeoPubFullTimeList, geoPubMinuteKey);
					// System.out.println("currentIndex  ..."+currentIndexes);
					keyGeoPubFullTimeList.remove(currentIndexes.get(0));
				}
			}
		}
	}

	private String getDateUptoMinute(String field) {
		SimpleDateFormat df = new SimpleDateFormat("yyyy-MM-dd hh:mm");
		String dateUptoMinute = null;
		try {
			dateUptoMinute = df.format(df.parse(field));
		} catch (ParseException e) {
		}
		return dateUptoMinute;
	}

	private Date getFullTimeByDate(String field) {
		SimpleDateFormat df = new SimpleDateFormat("yyyy-MM-dd hh:mm:ss aa");
		Date dateUptoMinute = null;
		try {
			dateUptoMinute = df.parse(field);
		} catch (ParseException e) {
		}
		return dateUptoMinute;
	}

	private boolean CheckStartWithInList(List<String> keyGeoPubFullTimeList,
			String keyGeoPubMinutePub) {
		if (keyGeoPubFullTimeList.size() > 0) {
			for (int i = 0; i < keyGeoPubFullTimeList.size(); i++) {
				if (keyGeoPubFullTimeList.get(i).startsWith(keyGeoPubMinutePub)) {
					return true;
				}
			}
		}
		return false;
	}

	private List<String> prepareStartWithInList(
			List<String> keyGeoPubFullTimeList, String keyGeoPubMinutePub) {
		List<String> indexToRemove = new ArrayList<String>();
		if (keyGeoPubFullTimeList.size() > 0) {
			for (int i = 0; i < keyGeoPubFullTimeList.size(); i++) {
				if (keyGeoPubFullTimeList.get(i).startsWith(keyGeoPubMinutePub)) {
					indexToRemove.add(new Integer(i).toString());
				}
			}
		}
		return indexToRemove;
	}

	@Override
	public void declareOutputFields(OutputFieldsDeclarer declarer) {
		declarer.declare(new Fields("batch"));
	}

}
