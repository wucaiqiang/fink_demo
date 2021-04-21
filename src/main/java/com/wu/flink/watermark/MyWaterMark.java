package com.wu.flink.watermark;

import com.wu.flink.dto.WordWithCountDTO;
import org.apache.flink.streaming.api.functions.timestamps.BoundedOutOfOrdernessTimestampExtractor;
import org.apache.flink.streaming.api.windowing.time.Time;

/**
 * description:TODO
 *
 * @author simpson
 * @create 2021/04/14
 **/
public class MyWaterMark extends BoundedOutOfOrdernessTimestampExtractor<WordWithCountDTO> {

    public MyWaterMark() {
        super(Time.seconds(0));
    }

    @Override
    public long extractTimestamp(WordWithCountDTO wordWithCountDTO) {
        return wordWithCountDTO.timestamp;

    }
}
