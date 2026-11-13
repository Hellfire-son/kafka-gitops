package com.devshawn.kafka.gitops;

import ch.qos.logback.classic.Level;
import com.devshawn.kafka.gitops.config.KafkaGitopsConfigLoader;
import com.devshawn.kafka.gitops.service.KafkaService;
import org.apache.kafka.clients.admin.*;
import org.apache.kafka.common.Node;
import org.apache.kafka.common.TopicPartition;
import org.slf4j.LoggerFactory;
import picocli.CommandLine;
import tools.jackson.databind.json.JsonMapper;
import java.io.File;
import java.lang.reflect.*;
import java.nio.file.*;
import java.util.*;

/** Transparent real-Admin trace. Only exact, pre-gated RF targets may be sent, once per partition. */
public final class Kafka3xTraceApply {
    public static void main(String[] args) throws Exception {
        ((ch.qos.logback.classic.Logger)LoggerFactory.getLogger("ROOT")).setLevel(Level.OFF);
        if (!args[0].equals("--allow-writes")) throw new IllegalArgumentException("Explicit opt-in required");
        Properties properties=new Properties();try(var in=Files.newInputStream(Path.of(args[1]))) { properties.load(in); }
        if (!"127.0.0.1:23792,127.0.0.1:23793,127.0.0.1:23794".equals(properties.getProperty("bootstrap.servers"))
                || !"PLAINTEXT".equals(properties.getProperty("security.protocol"))
                || properties.stringPropertyNames().stream().anyMatch(k -> k.startsWith("sasl.") || k.startsWith("ssl.")))
            throw new IllegalArgumentException("Only the fixed diagnostic Kafka 3.7.1 lab is allowed");
        var json=JsonMapper.builder().build();
        var plan=json.readTree(Files.readString(Path.of(args[2])));
        var topic=plan.get("topicPlans").get(0);
        if (plan.get("topicPlans").size()!=1 || !plan.get("aclPlans").isEmpty()
                || !topic.get("name").asText().matches("gitops-it-[0-9]{8}-[0-9]{6}-[a-f0-9]{6}-rf")
                || !topic.get("action").asText().equals("UPDATE") || !topic.get("topicConfigPlans").isEmpty()
                || topic.has("partitionPlan")) throw new IllegalArgumentException("RF-only lab plan required");
        String name=topic.get("name").asText();
        if (!"GV57l31pRMa-yIjZQBWyww".equals(topic.get("replicationPlan").get("clusterId").asText()))
            throw new IllegalArgumentException("Wrong diagnostic cluster");
        Map<TopicPartition,List<Integer>> wanted=new HashMap<>();
        for (var a:topic.get("replicationPlan").get("assignments")) {
            List<Integer> ids=new ArrayList<>();a.get("targetReplicas").forEach(n -> ids.add(n.asInt()));
            wanted.put(new TopicPartition(name,a.get("partition").asInt()),ids);
        }
        Set<TopicPartition> sent=Collections.synchronizedSet(new HashSet<>());
        List<Object> events=Collections.synchronizedList(new ArrayList<>());
        Set<String> reads=Set.of("describeCluster","describeTopics","listPartitionReassignments","describeFeatures","describeConfigs","describeLogDirs","close","toString","equals","hashCode");
        MainCommand.StateManagerFactory factory=(cfg,parser) -> new StateManager(cfg,parser,
            new KafkaService(KafkaGitopsConfigLoader.load(new File(args[1]))) {
                @Override protected Admin buildAdminClient() {
                    Admin real=super.buildAdminClient();
                    return (Admin)Proxy.newProxyInstance(Admin.class.getClassLoader(),new Class<?>[]{Admin.class},(p,m,a) -> {
                        if (m.getName().equals("alterPartitionReassignments")) {
                            var batch=(Map<TopicPartition,Optional<NewPartitionReassignment>>)a[0];
                            for (var entry:batch.entrySet())
                                if (!entry.getValue().isPresent() || !Objects.equals(wanted.get(entry.getKey()),entry.getValue().get().targetReplicas()) || !sent.add(entry.getKey()))
                                    throw new AssertionError("Unexpected/cancel/repeated RF submission");
                        } else if (!reads.contains(m.getName())) throw new AssertionError("Unexpected Admin method");
                        try {
                            Object value=m.invoke(real,a);
                            events.add(Map.of("method",m.getName(),"time",java.time.Instant.now().toString()));
                            if (value instanceof DescribeTopicsResult r) r.allTopicNames().thenApply(topics -> {
                                Map<String,Object> observed=new TreeMap<>();
                                topics.forEach((n,t) -> observed.put(n,Map.of("uuid",t.topicId().toString(),"partitions",t.partitions().stream().map(part ->
                                    Map.of("id",part.partition(),"replicas",part.replicas().stream().map(Node::id).toList(),"isr",part.isr().stream().map(Node::id).toList(),
                                           "leader",part.leader()==null?-1:part.leader().id())).toList())));
                                events.add(Map.of("topics",observed,"time",java.time.Instant.now().toString()));return topics;
                            });
                            if (value instanceof ListPartitionReassignmentsResult r) r.reassignments().thenApply(active -> {
                                events.add(Map.of("active",active.keySet().stream().map(Object::toString).toList(),"time",java.time.Instant.now().toString()));return active;
                            });
                            return value;
                        } catch (InvocationTargetException e) { throw e.getCause(); }
                    });
                }
            },null,true);
        int code;
        try { code=new CommandLine(new MainCommand(factory)).execute(Arrays.copyOfRange(args,4,args.length)); }
        finally { synchronized(events) { Files.writeString(Path.of(args[3]),json.writeValueAsString(List.copyOf(events))); } }
        System.exit(code);
    }
}
