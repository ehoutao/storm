package storm.samples.wordcount.bolt;

import java.util.ArrayList;
import java.util.Collections;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

import storm.samples.wordcount.util.Logging;
import backtype.storm.task.OutputCollector;
import backtype.storm.task.TopologyContext;
import backtype.storm.topology.OutputFieldsDeclarer;
import backtype.storm.topology.base.BaseRichBolt;
import backtype.storm.tuple.Tuple;

public class ReportBolt extends BaseRichBolt {
	
	private static final long serialVersionUID = 7624339253560245452L;
	
	private HashMap<String, Long> counts = null;

	public void declareOutputFields(OutputFieldsDeclarer declarer) {
	}

	public void prepare(Map config, TopologyContext context, OutputCollector collector) {
		Logging.get(getClass()).info(this.getClass() + " - prepare ");
		this.counts = new HashMap<String, Long>();
	}

	public void execute(Tuple tuple) {
		String word = tuple.getStringByField("word");
		
		Long count = tuple.getLongByField("count");

		Logging.get(getClass()).info("execute - "+word+" - "+count);
		
		this.counts.put(word, count);
		
	}
	
	public void cleanup(){
		Logging.get(getClass()).info("cleanup --- FINAL COUNTS ---");
		
		List<String> keys = new ArrayList<String>();
		
		keys.addAll(this.counts.keySet());
		
		Collections.sort(keys);
		
		for(String key : keys){
			Logging.get(getClass()).info(key + " : "+ this.counts.get(key));
		}
		
		Logging.get(getClass()).info("cleanup --------------------");
	}
}
