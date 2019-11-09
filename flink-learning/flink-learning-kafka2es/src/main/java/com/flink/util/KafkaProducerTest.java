package com.flink.util;

import com.flink.utils.KafkaProducerUtils;

/**
 *
 * Title: KafkaProducerTest
 * Description:
 * kafka
 * Version:2.2.1
 * @author pancm
 * @date 2019年6月15日
 */
public class KafkaProducerTest implements Runnable {

    private final String topic;
    public KafkaProducerTest(String topicName) {
        this.topic = topicName;
    }

    @Override
    public void run() {
        int messageNo = 1;
        try {
            for(;;) {
                String messageStr="1.74.103.143\t2018-12-20 18:12:00\t \"GET /class/130.html HTTP/1.1\" \t404\thttps://search.yahoo.com/search?p=Flink实战\n";
                KafkaProducerUtils.sendMessage(topic, messageStr);
                //producer.send(new ProducerRecord<String, String>(topic, "Message", messageStr));
                //生产了100条就打印
                if(messageNo%100==0){
                    System.out.println("发送的信息:" + messageStr);
                }
                //生产1000条就退出
                if(messageNo%1000==0){
                    System.out.println("成功发送了"+messageNo+"条");
                    break;
                }
                messageNo++;
            }
        } catch (Exception e) {
            e.printStackTrace();
        }
    }

    public static void main(String args[]) {
        KafkaProducerTest test = new KafkaProducerTest("KAFKA_TEST");
        Thread thread = new Thread(test);
        thread.start();
    }
}
