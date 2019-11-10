package com.mysql2kafka;

/**
 * Created by admin on 2019/6/8.
 */
import com.flink.utils.GsonUtil;
import entity.UserInfo;
import org.apache.flink.api.java.tuple.Tuple3;
import org.apache.flink.configuration.Configuration;
import org.apache.flink.streaming.api.functions.sink.RichSinkFunction;
import org.apache.kafka.clients.producer.*;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.Properties;


public class WriterKafka extends RichSinkFunction<Tuple3<String,String,String>> {
    private static final Logger logger = LoggerFactory.getLogger(ReaderMySQL.class);
    private static KafkaProducer<String, String> producer;
    private static final String topicName = "KAFKA_TEST";
    private static final String GROUPID = "groupA";

    @Override
    public void open(Configuration parameters) throws Exception {
        Properties props = new Properties();
        props.put("bootstrap.servers", "192.168.191.128:9092");
        props.put("acks", "all");
        props.put("retries", 0);
        props.put("group.id", GROUPID);
        props.put("batch.size", 16384);
        props.put("linger.ms", 1);
        props.put("buffer.memory", 33554432);
        props.put("key.serializer", "org.apache.kafka.common.serialization.StringSerializer");
        props.put("value.serializer", "org.apache.kafka.common.serialization.StringSerializer");
        producer = new KafkaProducer<String, String>(props);
    }

    @Override
    public void close() throws Exception {
        super.close();
        if(producer != null){
            producer.close();
        }
        super.close();
    }

    @Override
    public void invoke(Tuple3<String, String,String> stringStringTuple3) throws Exception {
        try {
            UserInfo userInfo = new UserInfo();
            userInfo.setId(stringStringTuple3.f0);
            userInfo.setUsername(stringStringTuple3.f1);
            userInfo.setPassword(stringStringTuple3.f2);
            producer.send(new ProducerRecord<>(topicName, GsonUtil.toJson(userInfo)));
            logger.info("发送消息：" + GsonUtil.toJson(userInfo));
        }catch (Exception e){
            e.printStackTrace();
        }
    }
}

