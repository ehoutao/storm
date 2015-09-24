package storm.samples.wordcount.spout;

import java.util.Map;

import storm.samples.wordcount.util.Logging;
import backtype.storm.spout.SpoutOutputCollector;
import backtype.storm.task.TopologyContext;
import backtype.storm.topology.OutputFieldsDeclarer;
import backtype.storm.topology.base.BaseRichSpout;
import backtype.storm.tuple.Fields;
import backtype.storm.tuple.Values;

public class SentenceSpout extends BaseRichSpout {

	private static final long serialVersionUID = 4545377567720437233L;
	
	private SpoutOutputCollector conllector;

	private String[] stentences = { "my dog has fleas",
			"i like cold beverages", "the dog ate my homework",
			"don't have a cow man", "i don't think i like fleas" };

	private int index = 0;

	public void declareOutputFields(OutputFieldsDeclarer declarer) {
		Logging.get(getClass()).info(this.getClass() + " - declareOutputFields ");
		declarer.declare(new Fields("stentence"));
	}

	public void open(Map config, TopologyContext context, SpoutOutputCollector collector) {
		Logging.get(getClass()).info(this.getClass() + " - open ");
		this.conllector = collector;
	}

	public void nextTuple() {
		if(index < stentences.length){
			String sentence = stentences[index];
			Logging.get(getClass()).info(this.getClass() + " - nextTuple - "+sentence);
			this.conllector.emit(new Values(sentence));	
			index ++ ;
		}
	}	
}
