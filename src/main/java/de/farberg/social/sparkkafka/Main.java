package de.farberg.social.sparkkafka;

import java.util.Map;
import java.util.Properties;
import java.util.stream.Collectors;
import java.util.stream.Stream;

import org.apache.kafka.clients.producer.KafkaProducer;
import org.apache.kafka.clients.producer.ProducerRecord;
import org.apache.kafka.clients.producer.RecordMetadata;
import org.apache.kafka.common.serialization.StringSerializer;
import org.apache.spark.SparkConf;
import org.apache.spark.streaming.Durations;
import org.apache.spark.streaming.api.java.JavaPairDStream;
import org.apache.spark.streaming.api.java.JavaPairReceiverInputDStream;
import org.apache.spark.streaming.api.java.JavaStreamingContext;
import org.apache.spark.streaming.kafka.KafkaUtils;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.jayway.jsonpath.JsonPath;

import de.uniluebeck.itm.util.logging.LogLevel;
import de.uniluebeck.itm.util.logging.Logging;
import scala.Tuple2;

public class Main {
	static {
		Logging.setLoggingDefaults(LogLevel.INFO, "[%-5p; %c{1}::%M] %m%n");
	}

	public static void main(String[] args) throws Exception {
		// Obtain an instance of a logger for this class
		Logger log = LoggerFactory.getLogger(Main.class);
		CommandLineOptions options = CommandLineOptions.parseCmdLineOptions(args);

		// Create context with a 2 seconds batch interval
		SparkConf sparkConf = new SparkConf().setAppName("Kafka-Spark-Kafka-Demo").setMaster("local[*]");
		JavaStreamingContext jssc = new JavaStreamingContext(sparkConf, Durations.seconds(10));

		// Create Kafka stream (returns DStream of (Kafka message key, Kafka message value))
		Map<String, Integer> topicMap = Stream.of(options.kafkaSourceTopic).collect(Collectors.toMap(key -> key, value -> 1));
		JavaPairReceiverInputDStream<String, String> kafkaMessages = KafkaUtils.createStream(jssc, options.kafkaBootstrapServer,
				options.kafkaGroupId, topicMap);

		// Map to (keyword-sentiment, 1)-pairs
		JavaPairDStream<String, Integer> keywordAndSentimentToOneMapping = kafkaMessages.mapToPair(jsonMessage -> {
			String key = JsonPath.read(jsonMessage._2, "$.keyword") + "-" + JsonPath.read(jsonMessage._2, "$.sentiment");
			return new Tuple2<>(key, 1);
		});

		JavaPairDStream<String, Integer> sumPerKeywordAndSentiment = keywordAndSentimentToOneMapping.reduceByKey((i1, i2) -> i1 + i2);

		sumPerKeywordAndSentiment.foreachRDD(rdd -> {
			rdd.foreachPartition(iterator -> {

				Properties props = new Properties();
				props.put("bootstrap.servers", options.zookeeperServer);
				props.put("group.id", options.kafkaGroupId);
				props.put("client.id", "bla");
				props.put("key.serializer", StringSerializer.class.getName());
				props.put("value.serializer", StringSerializer.class.getName());

				KafkaProducer<String, String> kafkaProducer = new KafkaProducer<String, String>(props);

				iterator.forEachRemaining(data -> {
					String value = "{ \"keyword-sentiment\" : \"" + data._1() + "\", \"count\": " + data._2 + " }";

					kafkaProducer.send(new ProducerRecord<String, String>(options.kafkaDestinationTopic, value),
							(RecordMetadata metadata, Exception exception) -> {

								// Display some data about the message transmission
								if (metadata != null) {
									log.info("Message(" + value + ") sent to partition(" + metadata.partition() + "), " + "offset("
											+ metadata.offset() + ")");
								} else {
									exception.printStackTrace();
								}

							});
				});

				kafkaProducer.close();
			});
		});

		// Start the computation
		jssc.start();
		jssc.awaitTermination();
	}
}
