package com.billwenboli.kafka;

import org.apache.flink.api.common.serialization.SimpleStringSchema;
import org.apache.flink.api.common.typeinfo.Types;
import org.apache.flink.api.java.tuple.Tuple2;
import org.apache.flink.streaming.api.datastream.DataStream;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.api.windowing.assigners.TumblingEventTimeWindows;
import org.apache.flink.streaming.api.windowing.time.Time;
import org.apache.flink.streaming.connectors.kafka.FlinkKafkaConsumer;
import org.apache.flink.streaming.connectors.kafka.FlinkKafkaProducer;
import org.apache.kafka.clients.consumer.ConsumerConfig;

import java.util.Properties;

public class FlinkWordCountWithWindow {

    public static void main(String[] args) throws Exception {

        String inputTopic = "flink-input";
        String outputTopic = "flink-output";
        String consumerGroup = "kafka-flink-group";
        String address = "127.0.0.1:9092";

        // Flink Stream execution
        StreamExecutionEnvironment environment = StreamExecutionEnvironment.getExecutionEnvironment();

        Properties properties = new Properties();
        properties.setProperty(ConsumerConfig.BOOTSTRAP_SERVERS_CONFIG, address);
        properties.setProperty(ConsumerConfig.GROUP_ID_CONFIG, consumerGroup);

        // Define input consumer
        FlinkKafkaConsumer<String> consumer = new FlinkKafkaConsumer<>(inputTopic, new SimpleStringSchema(), properties);

        // Define output producer
        FlinkKafkaProducer<String> producer = new FlinkKafkaProducer<>(outputTopic, new SimpleStringSchema(), properties);

        // Attach consumer to input data stream
        DataStream<String> stringInputStream = environment.addSource(consumer);

        // Map process
        DataStream<Tuple2<String, Integer>> mapper = stringInputStream
                .map(value -> new Tuple2<>(value, Integer.valueOf(1)))
                .returns(Types.TUPLE(Types.STRING, Types.INT));

        // Reduce process
        DataStream<Tuple2<String, Integer>> reducer = mapper
                .keyBy(0)
                .window(TumblingEventTimeWindows.of(Time.seconds(3)))
                .sum(1);

        DataStream<String> output = reducer.map(tuple -> tuple.f0 + " -> " + tuple.f1);

        // Attach producer to output data stream
        output.addSink(producer);

        environment.execute("Kafka Flink");
    }
}
