package org.conan.websocket;

import java.io.IOException;
import java.util.Arrays;
import java.util.Properties;

import javax.websocket.OnClose;
import javax.websocket.OnMessage;
import javax.websocket.OnOpen;
import javax.websocket.Session;
import javax.websocket.server.ServerEndpoint;

import org.apache.kafka.clients.consumer.KafkaConsumer;
import org.apache.kafka.clients.consumer.ConsumerRecord;
import org.apache.kafka.clients.consumer.ConsumerRecords;


@ServerEndpoint("/Outputrate")
public class DemoServlet2 {

    private Session session;
    
    
    KafkaConsumer<String, String> consumer;
    Properties prop = new Properties();

    
    //consumer = new KafkaConsumer<>(properties);
    
	@OnOpen
	public void cumsumerasStart(Session session)throws IOException {
		this.session = session;
		session.getBasicRemote().sendText("0");
		prop.put("bootstrap.servers", "172.16.20.80:9092,172.16.20.77:9092,172.16.20.78:9092,172.16.20.79:9092,172.16.20.81:9092");
		prop.put("group.id", "test");
		prop.put("key.deserializer", "org.apache.kafka.common.serialization.StringDeserializer");
		prop.put("value.deserializer", "org.apache.kafka.common.serialization.StringDeserializer");
		
		
		consumer = new KafkaConsumer<>(prop);
		
		//session.getBasicRemote().sendText("Kafka is coming");
		consumer.subscribe(Arrays.asList("Outputrate"));
		while (true) {
            ConsumerRecords<String, String> records = consumer.poll(500);
            for (ConsumerRecord<String, String> record : records) {
                  session.getBasicRemote().sendText(record.value());
            }
       }
		
	}
	
    @OnMessage
    public String echoTextMessage(Session session, String msg){
    	return msg;
    }
    @OnClose
    public void shutdonw(){
    	consumer.close();
    }


}
