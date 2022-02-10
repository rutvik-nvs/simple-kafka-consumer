package com.example.demo;

import org.apache.camel.LoggingLevel;
import org.apache.camel.builder.RouteBuilder;
import org.springframework.stereotype.Component;

@Component
public class DemoApplicationRouteBuilder extends RouteBuilder {

    @Override
    public void configure() throws Exception {

        // Kafka Route for consuming messages and logging them to the console
        from("kafka:{{kafka.topic}}?brokers={{kafka.host}}:{{kafka.port}}&reconnectBackoffMaxMs={{kafka.timeout}}&groupId={{kafka.groupId}}")
            .setBody().jsonpath("$.message")
            .log(LoggingLevel.INFO, "Kafka", "${body}")
            .to("mock:done");
    }
}