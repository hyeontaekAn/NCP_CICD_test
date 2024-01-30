package com.tmax.hf.batch;


import com.fasterxml.jackson.databind.ObjectMapper;
import com.sun.net.httpserver.HttpExchange;
import com.sun.net.httpserver.HttpHandler;
import org.apache.kafka.clients.producer.KafkaProducer;
import org.apache.kafka.clients.producer.ProducerConfig;
import org.apache.kafka.clients.producer.ProducerRecord;
import org.apache.kafka.common.serialization.StringSerializer;
import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;

import java.io.IOException;
import java.util.*;


public class KafkaHandler implements HttpHandler {
    private final static String TOPIC_NAME = "eventReg";
    private final static String BOOTSTRAP_SERVERS = "localhost:9092";

    private static final Logger logger = LogManager.getLogger(KafkaHandler.class);

    private void produce(String sendData){
        Properties configs = new Properties();
        configs.put(ProducerConfig.BOOTSTRAP_SERVERS_CONFIG, BOOTSTRAP_SERVERS);
        configs.put(ProducerConfig.KEY_SERIALIZER_CLASS_CONFIG, StringSerializer.class.getName());
        configs.put(ProducerConfig.VALUE_SERIALIZER_CLASS_CONFIG, StringSerializer.class.getName());

        KafkaProducer<String, String> producer = new KafkaProducer<>(configs);

        ProducerRecord<String, String> record = new ProducerRecord<>(TOPIC_NAME, sendData);
        producer.send(record);
        logger.info("produce data = {}", record);
        producer.flush();
        producer.close();
    }
    @Override
    public void handle(HttpExchange exchange) throws IOException{
            try{
                List<String> headerEmail = exchange.getRequestHeaders().get("emailaddress");
                List<String> headerInfo = exchange.getRequestHeaders().get("infoYN");
                List<String> headerGift = exchange.getRequestHeaders().get("giftcd");
                List<String> headerEvent = exchange.getRequestHeaders().get("eventcd");
                List<String> headerRegdt = exchange.getRequestHeaders().get("regdt");
                Map<String,String> produceMap = new HashMap<>();
                ObjectMapper mapper = new ObjectMapper();

                produceMap.put("emailaddress",headerEmail.get(0));
                produceMap.put("infoYN",headerInfo.get(0));
                produceMap.put("giftcd",headerGift.get(0));
                produceMap.put("eventcd",headerEvent.get(0));
                produceMap.put("regdt",headerRegdt.get(0));

                produce(mapper.writeValueAsString(produceMap));

                exchange.sendResponseHeaders(200, 0);
                logger.info("success event reg");
                //이벤트 정상 메시지

            } catch (IOException e){
                logger.error("Exception",e);
            }finally {
                exchange.close();
            }
    }
}

