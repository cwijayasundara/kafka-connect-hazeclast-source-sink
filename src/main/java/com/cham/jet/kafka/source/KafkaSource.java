/*
 * Copyright (c) 2008-2020, Hazelcast, Inc. All Rights Reserved.
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 * http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package com.cham.jet.kafka.source;

import com.cham.jet.kafka.base.KafkaBase;
import com.hazelcast.jet.Jet;
import com.hazelcast.jet.JetInstance;
import com.hazelcast.jet.Job;
import com.hazelcast.jet.kafka.KafkaSources;
import com.hazelcast.jet.pipeline.Pipeline;
import com.hazelcast.jet.pipeline.Sinks;
import com.hazelcast.map.IMap;
import org.apache.kafka.common.serialization.IntegerDeserializer;
import org.apache.kafka.common.serialization.StringDeserializer;

import static java.util.concurrent.TimeUnit.NANOSECONDS;

public class KafkaSource extends KafkaBase {

    private static final int MESSAGE_COUNT_PER_TOPIC = 1_000_000;
    private static final String BOOTSTRAP_SERVERS = "localhost:9092";
    private static final String AUTO_OFFSET_RESET = "earliest";

    private static final String SINK_NAME = "sink";

    private static Pipeline buildPipeline() {
        Pipeline p = Pipeline.create();
        p.readFrom(KafkaSources.kafka(props(
                "bootstrap.servers", BOOTSTRAP_SERVERS,
                "key.deserializer", StringDeserializer.class.getCanonicalName(),
                "value.deserializer", IntegerDeserializer.class.getCanonicalName(),
                "auto.offset.reset", AUTO_OFFSET_RESET)
                , "t1", "t2"))
         .withoutTimestamps()
         .writeTo(Sinks.map(SINK_NAME));
        return p;
    }

    public static void main(String[] args) throws Exception {
        new KafkaSource().execute();
    }

    private void execute() throws Exception {
        try {
            createKafkaCluster();
            fillTopics();

            JetInstance jet = Jet.bootstrappedInstance();
            IMap<String, Integer> sinkMap = jet.getMap(SINK_NAME);

            Pipeline p = buildPipeline();

            long start = System.nanoTime();
            Job job = jet.newJob(p);
            while (true) {
                int mapSize = sinkMap.size();
                System.out.format("Received %d entries in %d milliseconds.%n",
                        mapSize, NANOSECONDS.toMillis(System.nanoTime() - start));
                if (mapSize == MESSAGE_COUNT_PER_TOPIC * 2) {
                    job.cancel();
                    break;
                }
                Thread.sleep(100);
            }
        } finally {
            Jet.shutdownAll();
            shutdownKafkaCluster();
        }
    }
}
