package storm.samples.wordcount.bolt;

import java.util.HashMap;
import java.util.Map;

import storm.samples.wordcount.util.Logging;
import backtype.storm.task.OutputCollector;
import backtype.storm.task.TopologyContext;
import backtype.storm.topology.OutputFieldsDeclarer;
import backtype.storm.topology.base.BaseRichBolt;
import backtype.storm.tuple.Fields;
import backtype.storm.tuple.Tuple;
import backtype.storm.tuple.Values;

public class WordCountBolt extends BaseRichBolt {
	
	private static final long serialVersionUID = 7624339253560245452L;
	
	private OutputCollector collector;
	private HashMap<String, Long> counts = null;

	public void declareOutputFields(OutputFieldsDeclarer declarer) {
		Logging.get(getClass()).info(this.getClass() + " - declareOutputFields ");
		declarer.declare(new Fields("word", "count"));
	}

	public void prepare(Map config, TopologyContext context, OutputCollector collector) {
		Logging.get(getClass()).info(this.getClass() + " - prepare ");
		this.collector = collector;
		this.counts = new HashMap<String, Long>();
	}

	public void execute(Tuple tuple) {
		
		String word = tuple.getStringByField("word");
		
		Logging.get(getClass()).info(this.getClass() + " - execute "+word);
		
		Long count = this.counts.get(word);
		
		if(count == null){
			count = 0L;
		}
		
		count++;
		
		this.counts.put(word, count);
		
		this.collector.emit(new Values(word, count));
		
	}
}
