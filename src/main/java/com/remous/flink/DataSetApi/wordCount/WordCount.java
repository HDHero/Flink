package com.remous.flink.DataSetApi.wordCount;

import org.apache.flink.api.common.functions.FlatMapFunction;
import org.apache.flink.api.java.DataSet;
import org.apache.flink.api.java.ExecutionEnvironment;
import org.apache.flink.api.java.tuple.Tuple2;
import org.apache.flink.streaming.api.datastream.DataStream;
import org.apache.flink.streaming.api.datastream.DataStreamSource;
import org.apache.flink.streaming.api.datastream.SingleOutputStreamOperator;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.util.Collector;

/**
 * 流处理Word count
 */
public class WordCount {
    public static void main(String[] args) throws Exception {
        // 1.创建执行环境
        StreamExecutionEnvironment executionEnvironment = StreamExecutionEnvironment.getExecutionEnvironment();
        // 设置并行度
        executionEnvironment.setParallelism(4);
        // 2.从文件中获取数据 DataSource<String>
        DataStream<String> stream = executionEnvironment.readTextFile("D:\\workplace\\ideaUtlimate_works\\flink\\src\\main\\resources\\Hellow.txt");

        // 3.基于数据流进行转换计算
        DataStream<Tuple2<String, Integer>> ret = stream.flatMap(new myFlatMapper())
                .keyBy(0)
                .sum(1);
        // 5> (name,2) 5对应执行的分区的编号，默认的并行度是
        ret.print();
        // 执行任务
        executionEnvironment.execute();
        System.out.println("finish");

    }

    static class myFlatMapper implements FlatMapFunction<String, Tuple2<String, Integer>> {
        @Override
        public void flatMap(String s, Collector<Tuple2<String, Integer>> collector) throws Exception {
            String[] ret = s.split(" ");
            for (String temp : ret) {
                Tuple2<String, Integer> tuple2 = new Tuple2<>(temp, 1);
                collector.collect(tuple2);
            }
        }
    }
}
