package com.remous.flink.streamAPi.wordCount;

import org.apache.flink.api.common.functions.FlatMapFunction;
import org.apache.flink.api.java.DataSet;
import org.apache.flink.api.java.ExecutionEnvironment;
import org.apache.flink.api.java.tuple.Tuple2;
import org.apache.flink.util.Collector;

/**
 * 批处理Word count
 */
public class WordCount {
    public static void main(String[] args) throws Exception {
        // 1.创建执行环境
        ExecutionEnvironment executionEnvironment = ExecutionEnvironment.getExecutionEnvironment();

        // 2.从文件中获取数据 DataSource<String>
        DataSet<String> ret = executionEnvironment.readTextFile("D:\\workplace\\ideaUtlimate_works\\flink\\src\\main\\resources\\Hellow.txt");

        // 3.对数据进行处理，并按照空格分词展开，转换成二元组(word,1)进行统计
        DataSet<Tuple2<String, Integer>> sets = ret.flatMap(new myFlatMapper()).
                groupBy(0). // 按照第一个元素进行分组
                sum(1); // 按照第二个元素进行统计求和
        sets.print();
        //sets.print()
        //System.out.println("finish");

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
