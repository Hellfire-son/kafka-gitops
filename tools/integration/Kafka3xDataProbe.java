// SPDX-License-Identifier: Apache-2.0
package com.devshawn.kafka.gitops;

import ch.qos.logback.classic.Level;
import org.apache.kafka.clients.admin.*;
import org.apache.kafka.clients.consumer.*;
import org.apache.kafka.clients.producer.*;
import org.apache.kafka.common.*;
import org.slf4j.LoggerFactory;
import tools.jackson.databind.json.JsonMapper;

import java.nio.file.*;
import java.time.Duration;
import java.util.*;
import java.util.concurrent.TimeUnit;

/** Explicit lab-only data/observation probe, outside every Gradle source set. No commits or topic/config writes. */
public final class Kafka3xDataProbe {
    private static final JsonMapper JSON = JsonMapper.builder().build();

    private static TopicDescription topic(Admin admin, String name) throws Exception {
        if (!Kafka3xLabProbe.expectedClusterId().equals(admin.describeCluster(new DescribeClusterOptions().timeoutMs(5000))
                .clusterId().get(5, TimeUnit.SECONDS))) throw new IllegalStateException("Wrong lab cluster");
        TopicDescription t = admin.describeTopics(List.of(name), new DescribeTopicsOptions().timeoutMs(5000))
                .allTopicNames().get(5, TimeUnit.SECONDS).get(name);
        if (t == null || t.isInternal() || t.topicId().equals(Uuid.ZERO_UUID)) throw new IllegalStateException("Invalid test topic");
        return t;
    }

    public static void main(String[] original) throws Exception {
        String[] args=Kafka3xLabProbe.configure(original);
        ((ch.qos.logback.classic.Logger)LoggerFactory.getLogger(org.slf4j.Logger.ROOT_LOGGER_NAME)).setLevel(Level.OFF);
        Properties props = Kafka3xLabProbe.properties(args[1]);
        String prefix=args[2], name=args[3];
        if (!prefix.matches("gitops-it-[0-9]{8}-[0-9]{6}-[a-f0-9]{6}-") || !name.equals(prefix+"rf"))
            throw new IllegalArgumentException("Only the exact new-run RF topic is allowed");
        Path output=Path.of(args[4]);
        try (Admin admin=Admin.create(props)) {
            TopicDescription initial=topic(admin,name);
            if (args[0].equals("watch")) {
                Path ready=Path.of(args[5]), stop=Path.of(args[6]);
                long deadline=System.nanoTime()+Duration.ofSeconds(60).toNanos();
                try (var writer=Files.newBufferedWriter(output)) {
                    do {
                        TopicDescription t=topic(admin,name);
                        var active=admin.listPartitionReassignments(new ListPartitionReassignmentsOptions().timeoutMs(5000))
                                .reassignments().get(5,TimeUnit.SECONDS);
                        var rows=t.partitions().stream().map(p -> Map.of("id",p.partition(),"leader",p.leader()==null?-1:p.leader().id(),
                                "replicas",p.replicas().stream().map(Node::id).toList(),"isr",p.isr().stream().map(Node::id).sorted().toList(),
                                "elr",p.elr()==null?"UNKNOWN":p.elr().stream().map(Node::id).sorted().toList(),
                                "lastKnownElr",p.lastKnownElr()==null?"UNKNOWN":p.lastKnownElr().stream().map(Node::id).sorted().toList())).toList();
                        writer.write(JSON.writeValueAsString(Map.of("time",java.time.Instant.now().toString(),"topicId",t.topicId().toString(),
                                "partitions",rows,"active",active.keySet().stream().filter(tp -> tp.topic().equals(name)).map(Object::toString).toList())));
                        writer.newLine();writer.flush();
                        if (!Files.exists(ready)) Files.writeString(ready,"ready");
                        Thread.sleep(100);
                    } while (!Files.exists(stop) && System.nanoTime()<deadline);
                }
            } else if (args[0].equals("seed")) {
                if (args.length!=6 || !args[5].equals("--allow-writes") || Files.exists(output))
                    throw new IllegalArgumentException("Explicit opt-in and a new evidence file are required");
                props.setProperty("key.serializer","org.apache.kafka.common.serialization.StringSerializer");
                props.setProperty("value.serializer","org.apache.kafka.common.serialization.StringSerializer");
                props.setProperty("acks","all");props.setProperty("enable.idempotence","true");
                props.setProperty("max.block.ms","10000");props.setProperty("delivery.timeout.ms","30000");
                List<Map<String,Object>> records=new ArrayList<>();
                try (KafkaProducer<String,String> producer=new KafkaProducer<>(props)) {
                    for (var p:initial.partitions()) for (int i=0;i<6;i++) {
                        String key=prefix+p.partition()+"-"+i, value="ELR-RF-test:"+key;
                        RecordMetadata m=producer.send(new ProducerRecord<>(name,p.partition(),key,value)).get(30,TimeUnit.SECONDS);
                        records.add(Map.of("partition",m.partition(),"offset",m.offset(),"key",key,"value",value));
                    }
                }
                Files.writeString(output,JSON.writeValueAsString(Map.of("topic",name,"topicId",initial.topicId().toString(),"records",records)));
            } else if (args[0].equals("check-data")) {
                var expected=JSON.readTree(Files.readString(Path.of(args[5])));
                if (!name.equals(expected.get("topic").asText()) || !initial.topicId().toString().equals(expected.get("topicId").asText()))
                    throw new IllegalStateException("Data topic identity changed");
                props.setProperty("key.deserializer","org.apache.kafka.common.serialization.StringDeserializer");
                props.setProperty("value.deserializer","org.apache.kafka.common.serialization.StringDeserializer");
                props.setProperty("enable.auto.commit","false");props.setProperty("auto.offset.reset","none");
                Map<String,String> wanted=new TreeMap<>(), actual=new TreeMap<>();
                Map<TopicPartition,Long> starts=new HashMap<>();
                for (var r:expected.get("records")) {
                    int p=r.get("partition").asInt();long o=r.get("offset").asLong();
                    wanted.put(p+":"+o,r.get("key").asText()+"="+r.get("value").asText());
                    starts.merge(new TopicPartition(name,p),o,Math::min);
                }
                try (KafkaConsumer<String,String> consumer=new KafkaConsumer<>(props)) {
                    consumer.assign(starts.keySet());starts.forEach(consumer::seek);
                    long deadline=System.nanoTime()+Duration.ofSeconds(20).toNanos();
                    while (actual.size()<wanted.size() && System.nanoTime()<deadline)
                        for (var r:consumer.poll(Duration.ofMillis(250))) {
                            String k=r.partition()+":"+r.offset();
                            if (wanted.containsKey(k)) actual.put(k,r.key()+"="+r.value());
                        }
                }
                if (!wanted.equals(actual)) throw new IllegalStateException("Record content/offset mismatch");
                Files.writeString(output,JSON.writeValueAsString(Map.of("topic",name,"topicId",initial.topicId().toString(),"matched",actual.size(),"recordsByOffset",actual)));
            } else throw new IllegalArgumentException("Unknown probe mode");
        }
    }
}
