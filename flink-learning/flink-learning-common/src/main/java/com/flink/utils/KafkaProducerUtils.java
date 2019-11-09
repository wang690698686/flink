package com.flink.utils;

import com.alibaba.fastjson.JSONObject;
import org.apache.kafka.clients.producer.KafkaProducer;
import org.apache.kafka.clients.producer.ProducerRecord;

import java.io.InputStream;
import java.util.List;
import java.util.Map;
import java.util.Properties;

public class KafkaProducerUtils {

    private static KafkaProducer<String, String> producer;

    private static String bootstrapServers = null ;

    static{
        try{
            Properties prop = new Properties() ;
            InputStream in = JdbcUtil.class.getResourceAsStream("/kafka.properties") ;
            prop.load(in);
            bootstrapServers = prop.getProperty("bootstrap.servers") ;
        }catch(Exception e){
            e.printStackTrace();
        }
    }
    /**
     *
     * 私有静态方法，创建Kafka生产者
     *
     * @author IG
     * @Date 2019年11月09日 上午10:32:32
     * @version 1.0.0
     * @return KafkaProducer
     */
    public static KafkaProducer<String, String> getProducer() {

            Properties props = new Properties();
            props.put("bootstrap.servers", bootstrapServers);
            props.put("acks", "all");
            props.put("retries", 0);
            props.put("batch.size", 0);
            props.put("linger.ms", 1);
            props.put("buffer.memory", 33554432);
            props.put("key.serializer", "org.apache.kafka.common.serialization.StringSerializer");
            props.put("value.serializer", "org.apache.kafka.common.serialization.StringSerializer");
            producer = new KafkaProducer<String, String>((props));
        return producer;
    }

    /**
     *
     * 传入kafka约定的topicName,json格式字符串，发送给kafka集群
     *
     * @author IG
     * @Date 2019年11月09日 下午1:29:09
     * @version 1.0.0
     * @param topicName
     * @param jsonMessage
     */
    public static void sendMessage(String topicName, String jsonMessage) {
        KafkaProducer<String, String> producer = getProducer();
        producer.send(new ProducerRecord<>(topicName, jsonMessage));
        producer.close();
    }

    /**
     *
     * 传入kafka约定的topicName,json格式字符串数组，发送给kafka集群<br>
     * 用于批量发送消息，性能较高。
     *
     * @author IG
     * @Date 2019年11月09日 下午2:00:12
     * @version 1.0.0
     * @param topicName
     * @param jsonMessages
     * @throws InterruptedException
     */
    public void sendMessage(String topicName, String... jsonMessages) {
        KafkaProducer<String, String> producer = getProducer();
        for (String jsonMessage : jsonMessages) {
            producer.send(new ProducerRecord<>(topicName, jsonMessage));
        }
        producer.close();
    }

    /**
     *
     * 传入kafka约定的topicName,Map集合，内部转为json发送给kafka集群 <br>
     * 用于批量发送消息，性能较高。
     *
     * @author IG
     * @Date 2019年11月09日 下午2:01:18
     * @version 1.0.0
     * @param topicName
     * @param mapMessageToJSONForArray
     */
    public void sendMessage(String topicName, List<Map<Object, Object>> mapMessageToJSONForArray) {
        KafkaProducer<String, String> producer = getProducer();
        for (Map<Object, Object> mapMessageToJSON : mapMessageToJSONForArray) {
            String array = JSONObject.toJSONString(mapMessageToJSON);
            producer.send(new ProducerRecord<>(topicName, array));
        }
        producer.close();
    }

    /**
     *
     * 传入kafka约定的topicName,Map，内部转为json发送给kafka集群
     *
     * @author IG
     * @Date 2019年11月09日 下午1:30:10
     * @version 1.0.0
     * @param topicName
     * @param mapMessageToJSON
     */
    public void sendMessage(String topicName, Map<Object, Object> mapMessageToJSON) {
        KafkaProducer<String, String> producer = getProducer();
        String array = JSONObject.toJSONString(mapMessageToJSON);
        producer.send(new ProducerRecord<>(topicName, array));
        producer.close();
    }
}