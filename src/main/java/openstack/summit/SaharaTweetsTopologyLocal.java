package openstack.summit;

import java.util.UUID;

import openstack.summit.bolt.FilterSaharaTweets;
import openstack.summit.crawler.Crawler;
import storm.kafka.Broker;
import storm.kafka.KafkaSpout;
import storm.kafka.SpoutConfig;
import storm.kafka.StaticHosts;
import storm.kafka.StringScheme;
import storm.kafka.trident.GlobalPartitionInformation;
import backtype.storm.Config;
import backtype.storm.LocalCluster;
import backtype.storm.spout.SchemeAsMultiScheme;
import backtype.storm.topology.TopologyBuilder;
import backtype.storm.utils.Utils;


public class SaharaTweetsTopologyLocal {
	
	public static void main(String[] args) {
		final int numSpouts = 1;
		final Number numBolts = 1;
		final String hostBroker = "localhost";
		final int brokerPort = 9092;
		final String topic = "logs";
		final int tempoExecucao = 300;
		
	    TopologyBuilder topologyBuilder = new TopologyBuilder();

	    GlobalPartitionInformation globalPartitionInformation = new GlobalPartitionInformation();
	    
		for (int i = 0; i < numSpouts; i++) {
		    globalPartitionInformation.addPartition(i, new Broker(hostBroker, brokerPort));
	    }

	    StaticHosts staticHosts = new StaticHosts(globalPartitionInformation);
	    
		SpoutConfig spoutConfig = new SpoutConfig(staticHosts, topic, "/" + topic, UUID.randomUUID().toString());
		spoutConfig.scheme = new SchemeAsMultiScheme(new StringScheme());

		topologyBuilder.setSpout("spout", new KafkaSpout(spoutConfig), numSpouts);
		topologyBuilder.setBolt("filterSaharaTweetsBolt", new FilterSaharaTweets(hostBroker), 4).shuffleGrouping("spout");
		

		Config config = new Config();
		config.setNumWorkers(3);
		
		Thread thread = new Thread() {
	    	public void run() {
			    Utils.sleep(30000);
			    new Crawler().getTweets();
	    	}
	    };
	    
	    thread.setDaemon(true);
		thread.start();
		
		LocalCluster cluster = new LocalCluster();
		cluster.submitTopology("storm-log-analyzer", config, topologyBuilder.createTopology());
		Utils.sleep(tempoExecucao * 1000);
//		cluster.killTopology("calculo-media-movel");
		cluster.shutdown();
	}

}
