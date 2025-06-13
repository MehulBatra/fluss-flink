package com.multiTable;

import com.alibaba.fluss.flink.sink.FlussSink;
import com.alibaba.fluss.flink.source.FlussSource;
import com.alibaba.fluss.flink.source.enumerator.initializer.OffsetsInitializer;
import com.common.Person;
import com.example.PersonPKSerializationSchema;
import com.exampleAppend.PersonDeserializationSchema;
import org.apache.flink.api.common.eventtime.WatermarkStrategy;
import org.apache.flink.streaming.api.datastream.DataStream;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;

public class FlussMultiTableTest {

    public static void main(String[] args) throws Exception {
        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
        env.setParallelism(2);

        // Source: Read from 5-field table
        FlussSource<Person> sourceA = FlussSource.<Person>builder()
                .setBootstrapServers("localhost:9123")
                .setDatabase("fluss")
                .setTable("Fluss_PK_A") // 5-field source
                .setStartingOffsets(OffsetsInitializer.earliest())
                .setDeserializationSchema(new PersonDeserializationSchema())
                .build();

        DataStream<Person> streamA = env.fromSource(sourceA, WatermarkStrategy.noWatermarks(), "Source A");

        // Sink 1: Write to 5-field table
        FlussSink<Person> sink5Field = FlussSink.<Person>builder()
                .setBootstrapServers("localhost:9123")
                .setDatabase("fluss")
                .setTable("Fluss_PK_Target") // 5-field target table
                .setSerializationSchema(new PersonPKSerializationSchema()) // 5-field serialization
                .build();

        // Process and route to different sinks
        streamA
                .filter(person -> person.id != null && person.id % 2 == 0) // Even IDs
                .map(person -> {
                    person.processedTime = System.currentTimeMillis();
                    person.score = person.score != null ? person.score + 5.0 : null;
                    System.out.println("MULTI-TABLE-EVEN: " + person);
                    return person;
                })
                .sinkTo(sink5Field)
                .name("Even ID Sink");

        streamA
                .filter(person -> person.id != null && person.id % 2 == 1) // Odd IDs
                .map(person -> {
                    person.processedTime = System.currentTimeMillis();
                    person.score = person.score != null ? person.score + 10.0 : null;
                    System.out.println("MULTI-TABLE-ODD: " + person);
                    return person;
                })
                .sinkTo(sink5Field) // Use same sink for now
                .name("Odd ID Sink");

        env.execute("Fluss DataStream API - Multi-Table Fixed Test");
    }
}