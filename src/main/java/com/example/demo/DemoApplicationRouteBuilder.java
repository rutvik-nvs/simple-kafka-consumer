package com.example.demo;

import java.util.HashMap;
import java.util.regex.Pattern;

import org.apache.camel.Exchange;
import org.apache.camel.Processor;
import org.apache.camel.builder.RouteBuilder;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.stereotype.Component;

@Component
public class DemoApplicationRouteBuilder extends RouteBuilder {

    private static HashMap<String, Object>  hashMap         = new HashMap<String, Object>();

    private static String                   regex           = "\\d{4}-\\d{1,2}-\\d{1,2}";
    private static Pattern                  regexPattern    = Pattern.compile(regex);

    private static Logger                   LOGGER          = LoggerFactory.getLogger(DemoApplicationRouteBuilder.class);

    @Override
    public void configure() throws Exception {

        // Kafka Route for consuming messages and logging them to the console
        from("kafka:{{kafka.topic}}?brokers={{kafka.host}}:{{kafka.port}}&reconnectBackoffMaxMs={{kafka.timeout}}&groupId={{kafka.groupId}}")
            
            .setHeader("body").jsonpath("$.message", true)
            .setHeader("namespace").jsonpath("$.kubernetes.namespace_name", true)

            .process(new Processor() {
                @Override
                public void process(Exchange exchange) throws Exception
                {
                    if(exchange.getIn().getHeader("body") != null && exchange.getIn().getHeader("namespace") != null){
                        String body =       exchange.getIn().getHeader("body").toString();
                        String namespace =  exchange.getIn().getHeader("namespace").toString();

                        // Regex lookup
                        if(regexPattern.matcher(body).find()){
                            exchange.getIn().setBody(hashMap.get(namespace) != null ? hashMap.get(namespace).toString() : "");
                            if(hashMap.get(namespace) != null){
                                exchange.getIn().setHeader("shouldCallErrorDb", true);
                            }
                            else{
                                exchange.getIn().setHeader("shouldCallErrorDb", false);
                                hashMap.put(namespace, body);
                            }
                        }
                        else{
                            hashMap.put(namespace, hashMap.get(namespace) != null ? (hashMap.get(namespace).toString() + "\n" + body) : body);
                        }
                    }
                }
            })

            .choice()
                .when().simple("${headers.shouldCallErrorDb} == true")
                    .process(new Processor() {
                        @Override
                        public void process(Exchange exchange) throws Exception
                        {
                            LOGGER.info(exchange.getIn().getBody().toString());

                            String namespace =  exchange.getIn().getHeader("namespace").toString();
                            hashMap.put(namespace, exchange.getIn().getHeader("body").toString());
                        }
                    })
            .end()
            .to("mock:done");
    }
}