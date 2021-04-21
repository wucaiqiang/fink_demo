package com.wu.flink;

import lombok.extern.slf4j.Slf4j;
import org.apache.flink.api.common.serialization.SimpleStringSchema;
import org.apache.flink.streaming.api.CheckpointingMode;
import org.apache.flink.streaming.api.datastream.DataStream;
import org.apache.flink.streaming.api.environment.CheckpointConfig;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.api.functions.sink.SinkFunction;
import org.apache.flink.streaming.connectors.kafka.FlinkKafkaConsumer;
import org.apache.flink.streaming.connectors.kafka.internals.KafkaTopicPartition;

import java.util.HashMap;
import java.util.Map;
import java.util.Properties;

/**
 * description:TODO
 *
 * @author simpson
 * @create 2021/04/19
 **/
@Slf4j
public class KafkaSource {
    public static void main(String[] args) throws Exception {
        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();

        //checkpoint配置
        env.enableCheckpointing(3000);
        env.getCheckpointConfig().setCheckpointingMode(CheckpointingMode.EXACTLY_ONCE);
        env.getCheckpointConfig().setMinPauseBetweenCheckpoints(500);
        env.getCheckpointConfig().setCheckpointTimeout(60000);
        env.getCheckpointConfig().setMaxConcurrentCheckpoints(1);
        env.getCheckpointConfig().enableExternalizedCheckpoints(CheckpointConfig.ExternalizedCheckpointCleanup.RETAIN_ON_CANCELLATION);

        String topic = "test1";
        Properties prop = new Properties();
        prop.setProperty("bootstrap.servers", "localhost:9092");
        prop.setProperty("group.id", "group_1");

        FlinkKafkaConsumer consumer = new FlinkKafkaConsumer<>(topic, new SimpleStringSchema(), prop);
        consumer.setStartFromGroupOffsets();
        consumer.setCommitOffsetsOnCheckpoints(true);
        /*Map<KafkaTopicPartition, Long> specificStartOffsets = new HashMap<>();
        specificStartOffsets.put(new KafkaTopicPartition(topic, 0), 3L);
        consumer.setStartFromSpecificOffsets(specificStartOffsets);
        consumer.setCommitOffsetsOnCheckpoints(true);*/

        DataStream<String> text = env
                .addSource(consumer);

//        DataStream<String> text = env.socketTextStream("localhost", 10001, "\n");
        text.addSink(new SinkFunction<String>() {
            @Override
            public void invoke(String value, Context context) throws Exception {
                log.info("================value={}", value.toString());
            }
        });
//        text.print().setParallelism(1);
        env.execute("StreamingFromCollection");
    }
}
