package com.kafka2es;


import org.apache.flink.api.common.functions.MapFunction;
import org.apache.flink.api.common.functions.RuntimeContext;
import org.apache.flink.api.common.serialization.SimpleStringSchema;
import org.apache.flink.api.java.tuple.Tuple5;
import org.apache.flink.streaming.api.TimeCharacteristic;
import org.apache.flink.streaming.api.datastream.DataStream;
import org.apache.flink.streaming.api.datastream.DataStreamSource;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.connectors.elasticsearch.ElasticsearchSinkFunction;
import org.apache.flink.streaming.connectors.elasticsearch.RequestIndexer;
import org.apache.flink.streaming.connectors.elasticsearch6.ElasticsearchSink;
import org.apache.flink.streaming.connectors.kafka.FlinkKafkaConsumer09;
import org.apache.http.HttpHost;
import org.apache.kafka.common.serialization.StringDeserializer;
import org.elasticsearch.action.index.IndexRequest;
import org.elasticsearch.client.Requests;

import java.util.*;

/**
 * Created by developer on 7/16/18.
 */

public class Kafka2Es {
    private static final String TOPIC = "KAFKA_TEST";
    private static final String GROUPID = "groupA";
    public static void main(String[] args) throws Exception {
        System.out.println("===============》 flink任务开始  ==============》");
        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
        //设置kafka连接参数
        Properties properties = new Properties();
        properties.setProperty("enable.auto.commit", "true");
        properties.setProperty("auto.commit.interval.ms", "1000");
        properties.setProperty("session.timeout.ms", "30000");
        properties.setProperty("auto.offset.reset", "earliest");
        properties.setProperty("key.deserializer", StringDeserializer.class.getName());
        properties.setProperty("value.deserializer", StringDeserializer.class.getName());
        properties.setProperty("bootstrap.servers", "hadoop1:9092");
        properties.setProperty("zookeeper.connect", "hadoop1:2181");
        properties.setProperty("group.id", GROUPID);
        //设置时间类型
        env.setStreamTimeCharacteristic(TimeCharacteristic.EventTime);
        //设置检查点时间间隔
        env.enableCheckpointing(5000);
        //创建kafak消费者，获取kafak中的数据
        FlinkKafkaConsumer09<String> kafkaConsumer010 = new FlinkKafkaConsumer09<>(TOPIC, new SimpleStringSchema(), properties);
        kafkaConsumer010.setStartFromEarliest();
        DataStreamSource<String> kafkaData = env.addSource(kafkaConsumer010);
        kafkaData.print();
        //解析kafka数据流 转化成固定格式数据流
        DataStream<Tuple5<String, String, String, String, String>> userData = kafkaData.map(new MapFunction<String, Tuple5<String, String, String, String, String>>() {
            @Override
            public Tuple5<String, String, String, String, String> map(String s) throws Exception {
                Tuple5<String, String, String, String, String> userInfo=null;
                String[] split = s.split("\t");
                if(split.length!=5){
                    System.out.println("========"+s);
                }else {
                    String userID = split[0];
                    String itemId = split[1];
                    String categoryId = split[2];
                    String behavior = split[3];
                    String timestamp = split[4];
                    userInfo = new Tuple5<>(userID, itemId, categoryId, behavior, timestamp);
                }
                return userInfo;
            }
        });
        List<HttpHost> httpHosts = new ArrayList<>();
        httpHosts.add(new HttpHost("192.168.191.128", 9200, "http"));
        ElasticsearchSink.Builder<Tuple5<String, String, String, String, String>> esSinkBuilder = new ElasticsearchSink.Builder<>(
                httpHosts,
                new ElasticsearchSinkFunction<Tuple5<String, String, String, String, String>>() {
                    public IndexRequest createIndexRequest(Tuple5<String, String, String, String, String> element) {
                        Map<String, String> json = new HashMap<>();
                        json.put("userid", element.f0.toString());
                        json.put("itemid", element.f1.toString());
                        json.put("categoryid", element.f2.toString());
                        json.put("behavior", element.f3);
                        json.put("timestamp", element.f4.toString());
                        return Requests.indexRequest()
                                .index("kafka2es")
                                .type("user")
                                .source(json);
                    }
                    @Override
                    public void process(Tuple5<String, String, String, String, String> element, RuntimeContext ctx, RequestIndexer indexer) {
                        indexer.add(createIndexRequest(element));
                    }
                }
        );
        /*     必须设置flush参数     */
        //刷新前缓冲的最大动作量
        esSinkBuilder.setBulkFlushMaxActions(1);
        //刷新前缓冲区的最大数据大小（以MB为单位）
        esSinkBuilder.setBulkFlushMaxSizeMb(500);
        //论缓冲操作的数量或大小如何都要刷新的时间间隔
        esSinkBuilder.setBulkFlushInterval(5000);
        userData.addSink(esSinkBuilder.build());
        env.execute("data2es");
    }
}
