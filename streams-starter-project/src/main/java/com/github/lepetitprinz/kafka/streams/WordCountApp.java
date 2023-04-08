package com.github.lepetitprinz.kafka.streams;

import org.apache.kafka.clients.consumer.ConsumerConfig;
import org.apache.kafka.common.serialization.Serdes;
import org.apache.kafka.streams.KafkaStreams;
import org.apache.kafka.streams.StreamsBuilder;
import org.apache.kafka.streams.StreamsConfig;
import org.apache.kafka.streams.Topology;
import org.apache.kafka.streams.kstream.KStream;
import org.apache.kafka.streams.kstream.KTable;
import org.apache.kafka.streams.kstream.Materialized;
import org.apache.kafka.streams.kstream.Produced;

import java.util.Arrays;
import java.util.Properties;

public class WordCountApp {

    private static final String APPLICATION_ID_CONFIG = "word-count";
    private static final String BOOTSTRAP_SERVERS_CONFIG = "localhost:9092";
    private static final String AUTO_OFFSET_RESET_CONFIG = "earliest";

    public Topology createToplogy() {

        StreamsBuilder builder = new StreamsBuilder();

        // Stream from Kafka
        KStream<String, String> textdLines = builder.stream("word-count-input");

        KTable<String, Long> wordCounts = textdLines
            .mapValues(textLine -> textLine.toLowerCase())  // map values to lowercase
            .flatMapValues(lowercaseTextLine -> Arrays.asList(lowercaseTextLine.split("\\W+")))
            .selectKey((key, word) -> word)    // select key to apply a key
            .groupByKey()    // group by key before aggregation
            .count(Materialized.as("Counts"));

        // in order to write the results back to kafka
        wordCounts.toStream().to(
            "word-count-output",
            Produced.with(Serdes.String(), Serdes.Long()));

        return builder.build();
    }

    public static void main(String[] args) {
        Properties config = new Properties();
        config.put(StreamsConfig.APPLICATION_ID_CONFIG, APPLICATION_ID_CONFIG);
        config.put(StreamsConfig.BOOTSTRAP_SERVERS_CONFIG, APPLICATION_ID_CONFIG);
        config.put(ConsumerConfig.AUTO_OFFSET_RESET_CONFIG, APPLICATION_ID_CONFIG);
        config.put(StreamsConfig.DEFAULT_KEY_SERDE_CLASS_CONFIG, Serdes.String().getClass());
        config.put(StreamsConfig.DEFAULT_VALUE_SERDE_CLASS_CONFIG, Serdes.String().getClass());

        WordCountApp wordCountApp = new WordCountApp();

        KafkaStreams streams = new KafkaStreams(wordCountApp.createToplogy(), config);
        streams.start();

        // print the topology
        System.out.println(streams.toString());

        // shutdown hook to correctly close the streams application
        Runtime.getRuntime().addShutdownHook(new Thread(streams::close));
    }
}
