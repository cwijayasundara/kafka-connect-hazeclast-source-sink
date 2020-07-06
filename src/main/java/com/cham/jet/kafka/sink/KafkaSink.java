
package com.cham.jet.kafka.sink;

import com.cham.jet.kafka.base.KafkaBase;
import com.hazelcast.jet.Jet;
import com.hazelcast.jet.JetInstance;
import com.hazelcast.jet.Job;
import com.hazelcast.jet.kafka.KafkaSinks;
import com.hazelcast.jet.pipeline.Pipeline;
import com.hazelcast.jet.pipeline.Sources;
import com.hazelcast.map.IMap;
import kafka.utils.TestUtils;
import lombok.extern.slf4j.Slf4j;
import org.apache.kafka.clients.consumer.ConsumerRecords;
import org.apache.kafka.clients.consumer.KafkaConsumer;
import org.apache.kafka.common.security.auth.SecurityProtocol;
import org.apache.kafka.common.serialization.IntegerDeserializer;
import org.apache.kafka.common.serialization.IntegerSerializer;
import org.apache.kafka.common.serialization.StringDeserializer;
import org.apache.kafka.common.serialization.StringSerializer;
import scala.Option;
import java.io.File;
import java.util.Collections;
import java.util.Properties;

import static java.util.concurrent.TimeUnit.NANOSECONDS;

@Slf4j
public class KafkaSink extends KafkaBase {

    private static final int MESSAGE_COUNT = 2000;
    private static final String BOOTSTRAP_SERVERS = "localhost:9092";
    private static final String AUTO_OFFSET_RESET = "earliest";
    private static final String SOURCE_NAME = "source_map";
    private static final String SINK_TOPIC_NAME = "t1";
    private KafkaConsumer kafkaConsumer;

    private static Pipeline buildPipeline() {
        Pipeline pipeline = Pipeline.create();
        pipeline.readFrom(Sources.map(SOURCE_NAME))
         .writeTo(KafkaSinks.kafka(props(
                 "bootstrap.servers", BOOTSTRAP_SERVERS,
                 "key.serializer", StringSerializer.class.getCanonicalName(),
                 "value.serializer", IntegerSerializer.class.getCanonicalName()),
                 SINK_TOPIC_NAME));
        return pipeline;
    }

    public static void main(String[] args) throws Exception {
        new KafkaSink().execute();
    }

    private void execute() throws Exception {
        try {
            createKafkaCluster();

            JetInstance jet = Jet.bootstrappedInstance();
            IMap<String, Integer> sourceMap = jet.getMap(SOURCE_NAME);
            fillIMap(sourceMap);

            Pipeline p = buildPipeline();

            long start = System.nanoTime();
            Job job = jet.newJob(p);
            log.info("Consuming Topics");

            kafkaConsumer = getKafkaConsumer();

            kafkaConsumer.subscribe(Collections.singleton(SINK_TOPIC_NAME));

            int totalMessagesSeen = 0;
            while (true) {
                ConsumerRecords records = kafkaConsumer.poll(10000);
                totalMessagesSeen += records.count();
                System.out.format("Received %d entries in %d milliseconds.%n",
                        totalMessagesSeen, NANOSECONDS.toMillis(System.nanoTime() - start));
                if (totalMessagesSeen == MESSAGE_COUNT) {
                    job.cancel();
                    break;
                }
                Thread.sleep(100);
            }
        } finally {
            Jet.shutdownAll();
            shutdownKafkaCluster();
            kafkaConsumer.close();
        }
    }

    private KafkaConsumer<String, Integer> getKafkaConsumer() {
        return TestUtils.createConsumer(
                BOOTSTRAP_SERVERS,
                "verification-consumer",
                AUTO_OFFSET_RESET,
                true,
                true,
                4096,
                SecurityProtocol.PLAINTEXT,
                Option.<File>empty(),
                Option.<Properties>empty(),
                new StringDeserializer(),
                new IntegerDeserializer());
    }

    private void fillIMap(IMap<String, Integer> sourceMap) {
        log.info("Filling IMap");
        for (int i = 1; i <= MESSAGE_COUNT; i++) {
            sourceMap.put("t1-" + i, i);
        }
        log.info("Published " + MESSAGE_COUNT + " messages to IMap -> " + SOURCE_NAME);
    }
}
