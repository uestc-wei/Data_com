package com.wei.kafka;

import java.util.Properties;
import java.util.Random;
import java.util.concurrent.Future;
import org.apache.kafka.clients.producer.KafkaProducer;
import org.apache.kafka.clients.producer.Producer;
import org.apache.kafka.clients.producer.ProducerRecord;
import org.apache.kafka.clients.producer.RecordMetadata;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public class ProduceDataKafka {

    private static Logger logger = LoggerFactory.getLogger(ProduceDataKafka.class);
    public static void main(String[] args) throws InterruptedException {
        Properties prop=new Properties();
        String topic="userBehavior";
        prop.put("bootstrap.servers","9.135.100.240:9092");
        prop.put("key.serializer","org.apache.kafka.common.serialization.StringSerializer");
        prop.put("value.serializer","org.apache.kafka.common.serialization.StringSerializer");
        prop.put("acks","all");
        prop.put("retries",0);
        prop.put("batch.size",16384);
        prop.put("buffer.memory",33554432);
        Producer<String,String> producer= new
                KafkaProducer<>(prop);
        logger.info("开始发送数据");
        while (true) {
            String value = ProduceData.produce();
            System.out.println(value);
            Future<RecordMetadata> userBehavior = producer
                    .send(new ProducerRecord<>(topic, "userBehavior", value));
            System.out.println("正在发送数据...");
           if (userBehavior.isDone()){
               System.out.println("发送数据成功！");
            }else {
               System.out.println("发送失败！");
               break;
           }
            //Thread.sleep(100);
        }

    }

    /**
     *
     * userId,itemId,categoryId,behavior,timestamp
     * Long,Long,int,String,Long
     */
    private static class ProduceData{
        private static String behaviorString="pv,uv";
        private static Random random=new Random();
        public static String produce(){
            String[] behaviorList = behaviorString.split(",");
            //随机userId
            long userId=(long) Math.floor((random.nextDouble()*100000.0));
            //随机itemId
            long itemId=(long) Math.floor((random.nextDouble()*100000.0));
            //随机categoryId
            long categoryId=(long) Math.floor((random.nextDouble()*100000.0));
            //随机behavior
            String behavior = behaviorList[random.nextInt(1)];
            //递增时间戳
            long timeStamp= System.currentTimeMillis();
            return userId+","+itemId+","+categoryId+","+behavior+","+timeStamp;
        }
    }

}
