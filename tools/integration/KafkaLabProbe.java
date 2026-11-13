package com.devshawn.kafka.gitops;

import ch.qos.logback.classic.Level;
import com.devshawn.kafka.gitops.config.KafkaGitopsConfigLoader;
import com.devshawn.kafka.gitops.service.KafkaService;
import org.apache.kafka.clients.admin.*;
import org.apache.kafka.common.KafkaFuture;
import org.apache.kafka.common.Node;
import org.apache.kafka.common.config.ConfigResource;
import org.slf4j.LoggerFactory;
import picocli.CommandLine;
import tools.jackson.databind.json.JsonMapper;

import java.io.*;
import java.lang.reflect.*;
import java.nio.file.*;
import java.time.Duration;
import java.util.*;
import java.util.concurrent.TimeUnit;

/** Explicit lab harness, outside Gradle source sets. All direct Admin operations here are read-only. */
public final class KafkaLabProbe {
    private static final String BOOTSTRAP = "127.0.0.1:19092,127.0.0.1:19093,127.0.0.1:19094";
    private static final String CLUSTER_ID = "4L6g3nShT-eMCtK--X86sw";
    private static final JsonMapper JSON = JsonMapper.builder().build();
    private static <T> T read(KafkaFuture<T> f) throws Exception { return f.get(30, TimeUnit.SECONDS); }

    static Properties properties(String filename) throws IOException {
        Properties p = new Properties();
        try (var in = Files.newInputStream(Path.of(filename))) { p.load(in); }
        if (!BOOTSTRAP.equals(p.getProperty("bootstrap.servers")) || !"PLAINTEXT".equals(p.getProperty("security.protocol")))
            throw new IllegalArgumentException("Refusing properties outside the fixed loopback lab.");
        if (p.stringPropertyNames().stream().anyMatch(k -> k.startsWith("sasl.") || k.startsWith("ssl.") || k.equals("bootstrap.controllers")))
            throw new IllegalArgumentException("Unexpected security/controller properties.");
        return p;
    }

    private static Map<String, Object> snapshot(Properties props, String prefix) throws Exception {
        Map<String, Object> result = new TreeMap<>();
        Admin admin = Admin.create(props);
        try {
            var cluster = admin.describeCluster(new DescribeClusterOptions().timeoutMs(30_000));
            String id = read(cluster.clusterId());
            if (!CLUSTER_ID.equals(id)) throw new IllegalStateException("Wrong lab cluster ID.");
            List<Node> nodes = read(cluster.nodes()).stream().sorted(Comparator.comparingInt(Node::id)).toList();
            if (!nodes.stream().map(Node::id).toList().equals(List.of(1,2,3))) throw new IllegalStateException("Unexpected broker IDs.");
            for (Node n : nodes) if (!(n.host().equals("localhost") || n.host().equals("127.0.0.1"))
                    || n.port() != 19091 + n.id() || !Objects.equals(n.rack(),"rack-" + n.id()) || n.isFenced())
                throw new IllegalStateException("Unexpected broker endpoint/rack/fencing.");
            result.put("clusterId", id);
            result.put("nodes", nodes.stream().map(n -> Map.of("id",n.id(),"host",n.host(),"port",n.port(),"rack",n.rack(),"fenced",n.isFenced())).toList());
            result.put("controller", read(cluster.controller()).id());
            var quorum = read(admin.describeMetadataQuorum(new DescribeMetadataQuorumOptions().timeoutMs(30_000)).quorumInfo());
            result.put("quorum",Map.of("leaderId",quorum.leaderId(),"leaderEpoch",quorum.leaderEpoch(),"highWatermark",quorum.highWatermark(),
                    "voters",quorum.voters().stream().map(v -> Map.of("id",v.replicaId(),"logEndOffset",v.logEndOffset())).toList()));
            if (!quorum.voters().stream().map(v -> v.replicaId()).sorted().toList().equals(List.of(1,2,3)) || quorum.leaderId()<1)
                throw new IllegalStateException("Quorum is not ready.");
            FeatureMetadata features = read(admin.describeFeatures(new DescribeFeaturesOptions().timeoutMs(30_000)).featureMetadata());
            Map<String,Object> finalized = new TreeMap<>();
            features.finalizedFeatures().forEach((k,v) -> finalized.put(k,Map.of("min",v.minVersionLevel(),"max",v.maxVersionLevel())));
            result.put("finalizedFeatures",finalized);
            result.put("featureEpoch",features.finalizedFeaturesEpoch().orElse(-1L));
            List<ConfigResource> brokers = nodes.stream().map(n -> new ConfigResource(ConfigResource.Type.BROKER,n.idString())).toList();
            Map<ConfigResource,Config> configs = read(admin.describeConfigs(brokers,new DescribeConfigsOptions().timeoutMs(30_000)).all());
            Map<String,Object> safeConfigs = new TreeMap<>();
            configs.forEach((resource,config) -> {
                Map<String,String> safe = new TreeMap<>();
                for (String key : List.of("process.roles","cordoned.log.dirs","min.insync.replicas")) {
                    ConfigEntry e = config.get(key);
                    safe.put(key,e==null || e.isSensitive() || e.value()==null ? "UNKNOWN" : e.value());
                }
                safeConfigs.put(resource.name(),safe);
            });
            result.put("brokerConfigs",safeConfigs);
            Map<String,Object> dirs = new TreeMap<>();
            read(admin.describeLogDirs(List.of(1,2,3),new DescribeLogDirsOptions().timeoutMs(30_000)).allDescriptions()).forEach((broker,descriptions) ->
                    dirs.put(broker.toString(),descriptions.values().stream().map(d -> Map.of("healthy",d.error()==null,
                            "usableBytes",d.usableBytes().orElse(-1L),"replicaCount",d.replicaInfos().size())).toList()));
            result.put("logDirectories",dirs);
            List<String> all = read(admin.listTopics(new ListTopicsOptions().listInternal(true).timeoutMs(30_000)).names()).stream().sorted().toList();
            result.put("allTopics",all);
            List<String> selected = all.stream().filter(n -> n.startsWith(prefix)).toList();
            Map<String,Object> topics = new TreeMap<>();
            if (!selected.isEmpty()) {
                Map<String,TopicDescription> descriptions = read(admin.describeTopics(selected,new DescribeTopicsOptions().timeoutMs(30_000)).allTopicNames());
                List<ConfigResource> resources = selected.stream().map(n -> new ConfigResource(ConfigResource.Type.TOPIC,n)).toList();
                Map<ConfigResource,Config> tc = read(admin.describeConfigs(resources,new DescribeConfigsOptions().timeoutMs(30_000)).all());
                descriptions.forEach((name,t) -> {
                    Map<String,Object> safe = new TreeMap<>();
                    tc.get(new ConfigResource(ConfigResource.Type.TOPIC,name)).entries().forEach(c -> {
                        if (List.of("max.message.bytes","retention.ms","cleanup.policy","min.insync.replicas").contains(c.name()) && !c.isSensitive())
                            safe.put(c.name(),Map.of("value",String.valueOf(c.value()),"source",c.source().name()));
                    });
                    topics.put(name,Map.of("topicId",t.topicId().toString(),"configs",safe,"partitions",t.partitions().stream()
                            .sorted(Comparator.comparingInt(p -> p.partition())).map(p -> Map.of("id",p.partition(),"leader",p.leader()==null?-1:p.leader().id(),
                                    "replicas",p.replicas().stream().map(Node::id).toList(),"isr",p.isr().stream().map(Node::id).sorted().toList(),
                                    "elr",p.elr()==null?"UNKNOWN":p.elr().stream().map(Node::id).sorted().toList(),
                                    "lastKnownElr",p.lastKnownElr()==null?"UNKNOWN":p.lastKnownElr().stream().map(Node::id).sorted().toList())).toList()));
                });
            }
            result.put("topics",topics);
            Map<String,Object> reassignments = new TreeMap<>();
            read(admin.listPartitionReassignments(new ListPartitionReassignmentsOptions().timeoutMs(30_000)).reassignments()).forEach((tp,r) ->
                    reassignments.put(tp.topic()+":"+tp.partition(),Map.of("replicas",r.replicas(),"adding",r.addingReplicas(),"removing",r.removingReplicas())));
            result.put("reassignments",reassignments);
            return result;
        } finally { admin.close(Duration.ZERO); }
    }

    public static void main(String[] args) throws Exception {
        ((ch.qos.logback.classic.Logger)LoggerFactory.getLogger(org.slf4j.Logger.ROOT_LOGGER_NAME)).setLevel(Level.OFF);
        Properties props = properties(args[1]);
        if (args[0].equals("snapshot")) {
            Files.writeString(Path.of(args[3]),JSON.writeValueAsString(snapshot(props,args[2])));
        } else if (args[0].equals("plan")) {
            List<String> calls = Collections.synchronizedList(new ArrayList<>());
            Set<String> allowed = Set.of("listTopics","describeTopics","describeCluster","describeConfigs","describeFeatures","describeLogDirs","listPartitionReassignments","describeAcls","close","toString","hashCode","equals");
            MainCommand.StateManagerFactory factory = (cfg,parser) -> new StateManager(cfg,parser,
                    new KafkaService(KafkaGitopsConfigLoader.load(new File(args[1]))) {
                        @Override protected Admin buildAdminClient() {
                            Admin real = super.buildAdminClient();
                            return (Admin)Proxy.newProxyInstance(Admin.class.getClassLoader(),new Class<?>[]{Admin.class},(p,m,a) -> {
                                calls.add(m.getName());
                                if (!allowed.contains(m.getName())) throw new AssertionError("WRITE_OR_UNKNOWN_ADMIN_CALL:"+m.getName());
                                try { return m.invoke(real,a); } catch (InvocationTargetException e) { throw e.getCause(); }
                            });
                        }
                    }, null, true);
            int code;
            try { code = new CommandLine(new MainCommand(factory)).execute(Arrays.copyOfRange(args,3,args.length)); }
            finally { Files.writeString(Path.of(args[2]),JSON.writeValueAsString(Map.of("adminCalls",calls,"writeAttempts",calls.stream().filter(c -> !allowed.contains(c)).count()))); }
            System.exit(code);
        } else throw new IllegalArgumentException("Unknown read-only mode");
    }
}
