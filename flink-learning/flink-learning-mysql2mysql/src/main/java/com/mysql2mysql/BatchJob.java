package com.mysql2mysql;

/**
 * Created by admin on 2019/6/8.
 */
import org.apache.flink.streaming.api.datastream.DataStreamSource;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.api.windowing.time.Time;

/**
 * 将数据库中的数据查询出来再写入数据库
 */

public class BatchJob {

    public static void main(String[] args) throws Exception {
        //获取运行环境
        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
        env.getConfig().disableSysoutLogging();
        //数据来源操作
        DataStreamSource dataStream = env.addSource(new JdbcReader());
        dataStream.timeWindowAll(Time.seconds(60));
        //处理结果输出
        dataStream.addSink(new JdbcWriter());
        env.execute("flink mysql demo");
    }
}