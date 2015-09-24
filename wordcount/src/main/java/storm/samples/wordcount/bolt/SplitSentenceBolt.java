package storm.samples.wordcount.bolt;

import java.util.Map;

import storm.samples.wordcount.util.Logging;
import backtype.storm.task.OutputCollector;
import backtype.storm.task.TopologyContext;
import backtype.storm.topology.OutputFieldsDeclarer;
import backtype.storm.topology.base.BaseRichBolt;
import backtype.storm.tuple.Fields;
import backtype.storm.tuple.Tuple;
import backtype.storm.tuple.Values;

public class SplitSentenceBolt extends BaseRichBolt {

	private static final long serialVersionUID = 3641142672799769847L;
	
	private OutputCollector collector;

	public void declareOutputFields(OutputFieldsDeclarer declarer) {
		Logging.get(getClass()).info(this.getClass() + " - declareOutputFields ");
		declarer.declare(new Fields("word"));
	}

	public void prepare(Map config, TopologyContext context, OutputCollector collector) {
		Logging.get(getClass()).info(this.getClass() + " - prepare ");
		this.collector = collector;
	}

	public void execute(Tuple tuple) {
		
		String stentence = tuple.getStringByField("stentence");
		
		Logging.get(getClass()).info(this.getClass() + " - execute " + stentence);
		
		String[] words = stentence.split(" ");
		
		for(String word : words){
			this.collector.emit(new Values(word));
		}
	}

}
