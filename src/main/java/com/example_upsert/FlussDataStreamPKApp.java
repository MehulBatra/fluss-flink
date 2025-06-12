package com.example_upsert;

import com.alibaba.fluss.flink.sink.FlussSink;
import com.alibaba.fluss.flink.source.FlussSource;
import com.alibaba.fluss.flink.source.enumerator.initializer.OffsetsInitializer;
import com.common.Person;

import com.example_append.PersonPKDeserializationSchema;
import org.apache.flink.api.common.eventtime.WatermarkStrategy;
import org.apache.flink.streaming.api.datastream.DataStream;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.api.functions.ProcessFunction;
import org.apache.flink.util.Collector;

public class FlussDataStreamPKApp {

    public static void main(String[] args) throws Exception {
        // Create streaming execution environment
        final StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();

        // todo revesit again
//        env.enableCheckpointing(5000);

        // Configure parallelism
        env.setParallelism(1);

        // Create Fluss source to read from PrimaryKey table Fluss_PK_A
        FlussSource<Person> flussPKSource = FlussSource.<Person>builder()
                .setBootstrapServers("localhost:9123")
                .setDatabase("fluss")
                .setTable("Fluss_PK_A")
                .setStartingOffsets(OffsetsInitializer.full()) // Read snapshot + changelogs
                .setDeserializationSchema(new PersonPKDeserializationSchema())
                .build();

        // Create data stream from Fluss PrimaryKey source
        DataStream<Person> pkSourceStream = env.fromSource(
                        flussPKSource,
                        WatermarkStrategy.noWatermarks(),
                        "Fluss PK Source")
                .uid("fluss-pk-source");

        // Process the data: add timestamp and some transformation
        DataStream<Person> processedPKStream = pkSourceStream
                .process(new ProcessFunction<Person, Person>() {
                    @Override
                    public void processElement(Person person, Context ctx, Collector<Person> out) {
                        // Add processing timestamp
                        person.processedTime = System.currentTimeMillis();

                        // Simple transformation: increase score by 2 for PK table processing
                        if (person.score != null) {
                            person.score += 2.0;
                        }

                        out.collect(person);

                        // Log processing
                        System.out.println("Processed PK Record: " + person);
                    }
                })
                .uid("pk-data-processor");

        // Create Fluss sink to write to PrimaryKey table Fluss_PK_B
        FlussSink<Person> flussPKSink = FlussSink.<Person>builder()
                .setBootstrapServers("localhost:9123")
                .setDatabase("fluss")
                .setTable("Fluss_PK_B")
                .setSerializationSchema(new com.example.PersonPKSerializationSchema())
                .build();

        // Write processed data to Fluss_PK_B
        processedPKStream.sinkTo(flussPKSink)
                .uid("fluss-pk-sink")
                .name("Fluss PK Sink");

        // Execute the pipeline
        env.execute("Fluss DataStream PrimaryKey Test Application");
    }
}