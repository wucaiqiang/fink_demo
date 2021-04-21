package com.wu.flink;

import com.wu.flink.watermark.MyWaterMark;
import com.wu.flink.dto.WordWithCountDTO;
import lombok.extern.slf4j.Slf4j;
import org.apache.flink.api.common.functions.MapFunction;
import org.apache.flink.api.java.tuple.Tuple;
import org.apache.flink.api.java.tuple.Tuple1;
import org.apache.flink.streaming.api.TimeCharacteristic;
import org.apache.flink.streaming.api.datastream.DataStream;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.api.functions.sink.SinkFunction;
import org.apache.flink.streaming.api.functions.windowing.WindowFunction;
import org.apache.flink.streaming.api.windowing.time.Time;
import org.apache.flink.streaming.api.windowing.windows.TimeWindow;
import org.apache.flink.util.Collector;

import java.text.SimpleDateFormat;
import java.util.Iterator;


/**
 * description:TODO
 *
 * @author simpson
 * @create 2021/04/07
 **/
@Slf4j
public class SocketWindowWordCount {
//    private final static Logger logger = LoggerFactory.getLogger(SocketWindowWordCount.class);

    public static void main(String[] args) throws Exception {
        log.info("======================SocketWindowWordCount-[begin]");
        // 创建 execution environment
        final StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
        env.setStreamTimeCharacteristic(TimeCharacteristic.EventTime);
//        env.getConfig().setAutoWatermarkInterval(1000);
        env.setParallelism(2);
        // 通过连接 socket 获取输入数据，这里连接到本地9000端口，如果9000端口已被占用，请换一个端口
        DataStream<String> text = env.socketTextStream("localhost", 9000, "\n");

        DataStream<WordWithCountDTO> inputMap = text.map(new MapFunction<String, WordWithCountDTO>() {
            @Override
            public WordWithCountDTO map(String s) throws Exception {
                String[] arr = s.split(",");
                return new WordWithCountDTO(arr[0], Integer.parseInt(arr[2]), Long.parseLong(arr[1]));
            }
        });
        DataStream<WordWithCountDTO> waterMap = inputMap.assignTimestampsAndWatermarks(new MyWaterMark());
        DataStream<WordWithCountDTO> result = waterMap
                .keyBy("word")
//                .countWindow(3)
                .timeWindow(Time.seconds(3))
                .allowedLateness(Time.seconds(1))
                .apply(new WindowFunction<WordWithCountDTO, WordWithCountDTO, Tuple, TimeWindow>() {
                    @Override
                    public void apply(Tuple tuple, TimeWindow window, Iterable<WordWithCountDTO> input, Collector<WordWithCountDTO> out) throws Exception {
                        SimpleDateFormat sdf = new SimpleDateFormat("yyyy-MM-dd HH:mm:ss.SSS");
                        Tuple1<String> tp = (Tuple1) tuple;
                        log.info("=========tuple={},beginTime={},endTime={}", tp.f0, sdf.format(window.getStart()), sdf.format(window.getEnd()));
                        Iterator<WordWithCountDTO> it = input.iterator();
                        int sum = 0;
                        long time = 0;
                        boolean flag = true;
                        while (it.hasNext()) {
                            WordWithCountDTO next = it.next();
                            if (flag) {
                                time = next.timestamp;
                                flag = false;
                            }
                            sum = sum + next.count;
                        }
                        out.collect(new WordWithCountDTO(tp.f0, sum, time));
                    }
                })
                /*.apply(new WindowFunction<WordWithCount, WordWithCount, Tuple, GlobalWindow>() {
                    @Override
                    public void apply(Tuple tuple, GlobalWindow window, Iterable<WordWithCount> input, Collector<WordWithCount> out) throws Exception {
                        Tuple1<String> tp = (Tuple1) tuple;
                        log.info("=========tuple={},window={}", tp.f0, window.toString());
                        Iterator<WordWithCount> it = input.iterator();
                        int sum = 0;
                        long time = 0;
                        boolean flag = true;
                        while (it.hasNext()) {
                            WordWithCount next = it.next();
                            if (flag) {
                                time = next.timestamp;
                                flag = false;
                            }
                            sum = sum + next.count;
                        }
                        out.collect(new WordWithCount(tp.f0, sum, time));
                    }
                })*/
//                .sum("count")
                ;
        // 将结果打印到控制台，注意这里使用的是单线程打印，而非多线程
//        text.print().setParallelism(1);
        result.addSink(new SinkFunction<WordWithCountDTO>() {
            @Override
            public void invoke(WordWithCountDTO value, Context context) throws Exception {
                log.info("================value={}", value.toString());
            }
        });
        env.execute("Socket Window WordCount");
    }


}
