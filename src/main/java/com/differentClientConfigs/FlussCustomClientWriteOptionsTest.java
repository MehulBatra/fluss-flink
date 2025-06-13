package com.differentClientConfigs;

import com.alibaba.fluss.flink.sink.FlussSink;
import com.alibaba.fluss.flink.source.FlussSource;
import com.alibaba.fluss.flink.source.enumerator.initializer.OffsetsInitializer;
import com.common.Person;
import com.example.PersonPKSerializationSchema;
import com.exampleUpsert.PersonPKDeserializationSchema;
import org.apache.flink.api.common.eventtime.WatermarkStrategy;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;

public class FlussCustomClientWriteOptionsTest {

    public static void main(String[] args) throws Exception {
        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
        env.setParallelism(1);

        FlussSource<Person> source = FlussSource.<Person>builder()
                .setBootstrapServers("localhost:9123")
                .setDatabase("fluss")
                .setTable("Fluss_PK_A")
                .setStartingOffsets(OffsetsInitializer.earliest())
                .setDeserializationSchema(new PersonPKDeserializationSchema())
                .build();

        // Test with custom options
        FlussSink<Person> customOptionsSink = FlussSink.<Person>builder()
                .setBootstrapServers("localhost:9123")
                .setDatabase("fluss")
                .setTable("Fluss_PK_B")
                .setOption("client.writer.batch-size", "5mb")           // Larger batch size
                .setOption("client.writer.batch-timeout", "200ms")      // Higher timeout
                .setOption("client.writer.buffer.memory-size", "128mb") // More buffer memory
                .setOption("client.writer.acks", "all")                 // Wait for all replicas
                .setOption("client.connect-timeout", "30s")             // Custom timeout
                .setOption("client.request-timeout", "60s")             // Custom request timeout
                .setSerializationSchema(new PersonPKSerializationSchema())
                .build();

        env.fromSource(source, WatermarkStrategy.noWatermarks(), "Custom Options Test")
                .map(person -> {
                    person.processedTime = System.currentTimeMillis();
                    person.score = person.score != null ? person.score + 10.0 : null;
                    System.out.println("CUSTOM-OPTIONS: " + person);
                    return person;
                })
                .sinkTo(customOptionsSink)
                .name("Custom Options Sink");

        env.execute("Fluss DataStream API - Custom Options Test");
    }
}