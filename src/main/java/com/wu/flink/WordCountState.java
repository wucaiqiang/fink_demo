package com.wu.flink;

import com.wu.flink.dto.WordWithCountDTO;
import com.wu.flink.function.MyKeyOperatorStateFunction;
import com.wu.flink.watermark.MyWaterMark;
import lombok.extern.slf4j.Slf4j;
import org.apache.flink.api.common.functions.MapFunction;
import org.apache.flink.runtime.state.filesystem.FsStateBackend;
import org.apache.flink.streaming.api.CheckpointingMode;
import org.apache.flink.streaming.api.TimeCharacteristic;
import org.apache.flink.streaming.api.datastream.DataStream;
import org.apache.flink.streaming.api.environment.CheckpointConfig;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.api.functions.sink.SinkFunction;


/**
 * description:TODO
 *
 * @author simpson
 * @create 2021/04/07
 **/
@Slf4j
public class WordCountState {

    public static void main(String[] args) throws Exception {
        log.info("======================WordCountState-[begin]");
        final StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
        env.setStreamTimeCharacteristic(TimeCharacteristic.EventTime);
        env.setParallelism(2);
        env.enableCheckpointing(60000);
        env.getCheckpointConfig().setCheckpointingMode(CheckpointingMode.EXACTLY_ONCE);
        env.getCheckpointConfig().setMinPauseBetweenCheckpoints(30000);
        env.getCheckpointConfig().setCheckpointTimeout(20000);
        env.getCheckpointConfig().setMaxConcurrentCheckpoints(1);
//        env.getCheckpointConfig().enableExternalizedCheckpoints(CheckpointConfig.ExternalizedCheckpointCleanup.RETAIN_ON_CANCELLATION);
        env.setStateBackend(new FsStateBackend("file:///my_project/flink_demo/checkpoints", false));
        // 通过连接 socket 获取输入数据，这里连接到本地9000端口，如果9000端口已被占用，请换一个端口
        DataStream<String> text = env.socketTextStream("localhost", 10001, "\n");

        DataStream<WordWithCountDTO> inputMap = text.map(new MapFunction<String, WordWithCountDTO>() {
            @Override
            public WordWithCountDTO map(String s) throws Exception {
                log.info("=========参数={}", s);
                String[] arr = s.split(",");
                return new WordWithCountDTO(arr[0], Integer.parseInt(arr[2]), Long.parseLong(arr[1]));
            }
        });
        DataStream<WordWithCountDTO> waterMap = inputMap.assignTimestampsAndWatermarks(new MyWaterMark());
        DataStream<WordWithCountDTO> result = waterMap.keyBy(WordWithCountDTO::getWord).flatMap(new MyKeyOperatorStateFunction());
        result.addSink(new SinkFunction<WordWithCountDTO>() {
            @Override
            public void invoke(WordWithCountDTO value, Context context) throws Exception {
                log.info("================value={}", value.toString());
            }
        });

        env.execute("WordCountState");
    }
}
