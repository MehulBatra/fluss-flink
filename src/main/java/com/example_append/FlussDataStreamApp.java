package com.example_append;

import com.alibaba.fluss.flink.sink.FlussSink;
import com.alibaba.fluss.flink.source.FlussSource;
import com.alibaba.fluss.flink.source.enumerator.initializer.OffsetsInitializer;
import org.apache.flink.api.common.eventtime.WatermarkStrategy;
import org.apache.flink.streaming.api.datastream.DataStream;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.api.functions.ProcessFunction;
import org.apache.flink.util.Collector;

public class FlussDataStreamApp {

    public static void main(String[] args) throws Exception {
        // Create streaming execution environment
        final StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();

//        env.enableCheckpointing(5000);

        // Configure parallelism
        env.setParallelism(1);

        // Create Fluss source to read from Fluss_A (following documented API)
        FlussSource<Person> flussSource = FlussSource.<Person>builder()
                .setBootstrapServers("127.0.0.1:9123,localhost:9123")
                .setDatabase("fluss")
                .setTable("Fluss_A")
                .setStartingOffsets(OffsetsInitializer.earliest())
                .setDeserializationSchema(new PersonDeserializationSchema())
                .build();

        // Create data stream from Fluss source
        DataStream<Person> sourceStream = env.fromSource(
                        flussSource,
                        WatermarkStrategy.noWatermarks(),
                        "Fluss Source")
                .uid("fluss-source");

        // Process the data: add timestamp and some transformation
        DataStream<Person> processedStream = sourceStream
                .process(new ProcessFunction<Person, Person>() {
                    @Override
                    public void processElement(Person person, Context ctx, Collector<Person> out) {
                        // Add processing timestamp
                        person.processedTime = System.currentTimeMillis();

                        // Simple transformation: increase score by 5 if age > 25
                        if (person.age != null && person.age > 25 && person.score != null) {
                            person.score += 5.0;
                        }

                        out.collect(person);

                        // Log processing
                        System.out.println("Processed: " + person);
                    }
                })
                .uid("data-processor");

        // Create Fluss sink to write to Fluss_B (following documented API)
        FlussSink<Person> flussSink = FlussSink.<Person>builder()
                .setBootstrapServers("127.0.0.1:9123,localhost:9123")
                .setDatabase("fluss")
                .setTable("Fluss_B")
                .setSerializationSchema(new PersonSerializationSchema())
                .build();

        // Write processed data to Fluss_B
        processedStream.sinkTo(flussSink)
                .uid("fluss-sink")
                .name("Fluss Sink");

        // Execute the pipeline
        env.execute("Fluss DataStream Test Application");
    }
}