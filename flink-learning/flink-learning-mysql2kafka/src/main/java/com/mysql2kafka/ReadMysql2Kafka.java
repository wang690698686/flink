package com.mysql2kafka;

import org.apache.flink.streaming.api.datastream.DataStreamSource;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.api.windowing.time.Time;

/**
 * Created by admin on 2019/11/10.
 * 读取mysql中的数据，发送到kafka中
 */
public class ReadMysql2Kafka {
    public static void main(String[] args) throws Exception {
        //获取运行环境
        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
        env.getConfig().disableSysoutLogging();
        //数据来源操作
        DataStreamSource dataStream = env.addSource(new ReaderMySQL());
        dataStream.timeWindowAll(Time.seconds(60));
        //处理结果输出
        dataStream.addSink(new WriterKafka());
        env.execute("flink mysql to kafka demo");
    }
}
