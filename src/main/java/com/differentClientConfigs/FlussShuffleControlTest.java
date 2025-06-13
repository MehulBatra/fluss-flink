package com.differentClientConfigs;

import com.alibaba.fluss.flink.sink.FlussSink;
import com.alibaba.fluss.flink.source.FlussSource;
import com.alibaba.fluss.flink.source.enumerator.initializer.OffsetsInitializer;
import com.common.Person;
import com.exampleAppend.PersonDeserializationSchema;
import org.apache.flink.api.common.eventtime.WatermarkStrategy;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;

public class FlussShuffleControlTest {

    public static void main(String[] args) throws Exception {
        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
        env.setParallelism(2);

        FlussSource<Person> source = FlussSource.<Person>builder()
                .setBootstrapServers("localhost:9123")
                .setDatabase("fluss")
                .setTable("Fluss_PK_A")
                .setStartingOffsets(OffsetsInitializer.earliest())
                .setDeserializationSchema(new PersonDeserializationSchema())
                .build();

        // Test with shuffle disabled
        FlussSink<Person> noShuffleSink = FlussSink.<Person>builder()
                .setBootstrapServers("localhost:9123")
                .setDatabase("fluss")
                .setTable("Fluss_PK_B")
                .setShuffleByBucketId(false) // Disable bucket shuffling
                .setSerializationSchema(new com.example.PersonPKSerializationSchema())
                .build();

        // Test with shuffle enabled (default)
        FlussSink<Person> shuffleSink = FlussSink.<Person>builder()
                .setBootstrapServers("localhost:9123")
                .setDatabase("fluss")
                .setTable("Fluss_PK_B")
                .setShuffleByBucketId(true) // Enable bucket shuffling (default)
                .setSerializationSchema(new com.example.PersonPKSerializationSchema())
                .build();

        env.fromSource(source, WatermarkStrategy.noWatermarks(), "Shuffle Test Source")
                .map(person -> {
                    person.processedTime = System.currentTimeMillis();
                    System.out.println("SHUFFLE-TEST: " + person);
                    return person;
                })
                .sinkTo(noShuffleSink) // Use the no-shuffle sink
                .name("No Shuffle Sink");

        env.execute("Fluss DataStream API - Shuffle Control Test");
    }
}
