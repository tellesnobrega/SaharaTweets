package openstack.summit.crawler;

import java.io.IOException;
import java.io.InputStream;
import java.util.HashMap;
import java.util.Map;
import java.util.Properties;

import org.apache.kafka.clients.producer.KafkaProducer;
import org.apache.kafka.clients.producer.ProducerConfig;
import org.apache.kafka.clients.producer.ProducerRecord;
import org.apache.log4j.Logger;

import twitter4j.FilterQuery;
import twitter4j.StallWarning;
import twitter4j.Status;
import twitter4j.StatusDeletionNotice;
import twitter4j.StatusListener;
import twitter4j.TwitterException;
import twitter4j.TwitterStream;
import twitter4j.TwitterStreamFactory;
import twitter4j.conf.ConfigurationBuilder;

/**
 * Receives tweets from Twitter streaming API. You need to pass the right
 * parameters in the config.properties file.
 *
 */
public class Crawler {
	
	private static final Logger log = Logger.getLogger(Crawler.class);
	private String hostBroker;
	
	public Crawler(String hostBroker) {
		this.hostBroker = hostBroker;
	}
	public void getTweets() {
		StatusListener listener = new StatusListener() {
			int tweetCount = 0;

			public void onStatus(Status status) {
				String lang = status.getUser().getLang();
				try (KafkaProducer<String, String> producer = new KafkaProducer<>(
						getKafkaConfigs())) {
					if (tweetCount < 50000) {
						if (lang.equals("en")) {
							System.out.println(status.getText());
							producer.send(new ProducerRecord<String, String>(
									"tweets", status.getText()));
							tweetCount++;
						}
					} else {
						System.exit(0);
					}
				}
			}

			private Map<String, Object> getKafkaConfigs() {
				Map<String, Object> props = new HashMap<>();
				props.put(ProducerConfig.BOOTSTRAP_SERVERS_CONFIG,
						hostBroker + ":9092");
				props.put(ProducerConfig.VALUE_SERIALIZER_CLASS_CONFIG,
						"org.apache.kafka.common.serialization.StringSerializer");
				props.put(ProducerConfig.KEY_SERIALIZER_CLASS_CONFIG,
						"org.apache.kafka.common.serialization.StringSerializer");
				props.put(ProducerConfig.CLIENT_ID_CONFIG, "my-producer");

				return props;
			}

			public void onDeletionNotice(
					StatusDeletionNotice statusDeletionNotice) {
			}

			public void onTrackLimitationNotice(int numberOfLimitedStatuses) {
			}

			public void onException(Exception ex) {
				log.error(ex.toString());
			}

			public void onScrubGeo(long userId, long upToStatusId) {
			}

			public void onStallWarning(StallWarning warning) {
			}
		};
		// twitter stream authentication setup
		Properties prop = new Properties();
		try {
			InputStream in = Crawler.class.getClassLoader()
					.getResourceAsStream("twitter4j.properties");
			prop.load(in);
		} catch (IOException e) {
			log.error(e.toString());
		}
		// set the configuration
		ConfigurationBuilder twitterConf = new ConfigurationBuilder();
		twitterConf.setIncludeEntitiesEnabled(true);
		twitterConf.setDebugEnabled(Boolean.valueOf(prop.getProperty("debug")));
		twitterConf.setOAuthAccessToken(prop.getProperty("oauth.accessToken"));
		twitterConf.setOAuthAccessTokenSecret(prop
				.getProperty("oauth.accessTokenSecret"));
		twitterConf.setOAuthConsumerKey(prop.getProperty("oauth.consumerKey"));
		twitterConf.setOAuthConsumerSecret(prop
				.getProperty("oauth.consumerSecret"));
		twitterConf.setJSONStoreEnabled(true);

		TwitterStream twitterStream = new TwitterStreamFactory(
				twitterConf.build()).getInstance();
		
		FilterQuery fq = new FilterQuery();
	    
        String keywords[] = {"Sahara"};

		twitterStream.addListener(listener);
		fq.track(keywords);

		twitterStream.sample();
		twitterStream.filter(fq);

	}

	public static void main(String[] args) throws TwitterException, IOException {
		Crawler crawler = new Crawler(args[0]);
		crawler.getTweets();

	}

}
