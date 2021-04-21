package com.wu.flink.dto;

import lombok.Data;

@Data
public class WordWithCountDTO {
    public String word;
    public Integer count;
    public Integer sum;
    public long timestamp;

    public WordWithCountDTO() {
    }

    public WordWithCountDTO(String word, Integer count, long timestamp) {
        this.word = word;
        this.count = count;
        this.timestamp = timestamp;
    }

    public WordWithCountDTO(String word, Integer count, Integer sum, long timestamp) {
        this.word = word;
        this.count = count;
        this.sum = sum;
        this.timestamp = timestamp;
    }

    @Override
    public String toString() {
        return "WordWithCount{" +
                "word='" + word + '\'' +
                ", count=" + count +
                ",sum=" + sum +
                ",timestamp=" + timestamp +
                '}';
    }
}