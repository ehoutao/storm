package storm.samples.wordcount.util;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public class Logging {
	
	public static Logger get(Class clazz){
		return LoggerFactory.getLogger(clazz);
	}
}
