package openstack.summit.bolt;

import java.util.HashMap;
import java.util.Map;

import org.apache.kafka.clients.producer.KafkaProducer;
import org.apache.kafka.clients.producer.ProducerConfig;
import org.apache.kafka.clients.producer.ProducerRecord;

import backtype.storm.task.OutputCollector;
import backtype.storm.task.TopologyContext;
import backtype.storm.topology.OutputFieldsDeclarer;
import backtype.storm.topology.base.BaseRichBolt;
import backtype.storm.tuple.Fields;
import backtype.storm.tuple.Tuple;

public class FilterSaharaTweets extends BaseRichBolt {

	private static final long serialVersionUID = -3752711690604033901L;
	private OutputCollector collector;
	private String hostBroker;

	public FilterSaharaTweets(String hostBroker) {
		this.hostBroker = hostBroker;

	}

	@SuppressWarnings("rawtypes")
	public void prepare(Map stormConf, TopologyContext context,
			OutputCollector collector) {
		this.collector = collector;
	}

	public void execute(Tuple input) {
		String tweet = input.getString(0);
		if (tweet.contains("storm")|| tweet.contains("Storm") || tweet.contains("STORM") ) {
			Map<String, Object> props = getKafkaConfigs(hostBroker);
			try (KafkaProducer<String, String> producer = new KafkaProducer<>(
					props)) {
				producer.send(new ProducerRecord<String, String>("alarm", tweet));
			}
		}
		collector.ack(input);
	}
	
	private Map<String, Object> getKafkaConfigs(String hostBroker) {
		Map<String, Object> props = new HashMap<>();
		props.put(ProducerConfig.BOOTSTRAP_SERVERS_CONFIG, hostBroker + ":9092");
		props.put(ProducerConfig.VALUE_SERIALIZER_CLASS_CONFIG, "org.apache.kafka.common.serialization.StringSerializer");
		props.put(ProducerConfig.KEY_SERIALIZER_CLASS_CONFIG, "org.apache.kafka.common.serialization.StringSerializer");
		props.put(ProducerConfig.CLIENT_ID_CONFIG, "storm-output");
		
		return props;
	}

	public void declareOutputFields(OutputFieldsDeclarer declarer) {
	    declarer.declare(new Fields("component", "alarm"));
	}
	
}
