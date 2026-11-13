// SPDX-License-Identifier: Apache-2.0
package com.devshawn.kafka.gitops

import com.devshawn.kafka.gitops.domain.metadata.ReplicationClusterSnapshot
import com.devshawn.kafka.gitops.domain.metadata.ReplicationClusterSnapshot.*
import org.apache.kafka.clients.admin.TopicDescription
import org.apache.kafka.common.Node
import org.apache.kafka.common.TopicPartitionInfo
import org.apache.kafka.common.Uuid

import static com.devshawn.kafka.gitops.domain.metadata.ReplicationClusterSnapshot.Observation.VERIFIED

class ReplicationPlanningSupport {
    static ReplicationClusterSnapshot snapshot(Map<String, List<List<Integer>>> assignments = [orders:[[0], [1]]],
                                                Map<Integer, String> racks = [0:'a', 1:'b', 2:'c', 3:'d']) {
        def brokers = racks.collectEntries { id, rack ->
            [(id):new Broker(id, Optional.ofNullable(rack), VERIFIED, VERIFIED, VERIFIED, VERIFIED, OptionalLong.of(1000000))]
        }
        def topics = assignments.collectEntries { name, replicas ->
            [(name):new Topic(new Uuid(10L, name.hashCode().longValue()).toString(), replicas.withIndex().collect { r, index ->
                new Partition(index, r, OptionalInt.of(r.first()), new LinkedHashSet<>(r))
            }, OptionalInt.of(1))]
        }
        Map<Integer, Long> load = racks.collectEntries { id, rack -> [(id):0L] }
        assignments.values().flatten().each { id -> load[id] = load.getOrDefault(id, 0L) + 1L }
        new ReplicationClusterSnapshot('AQIDBAUGBwgJCgsMDQ4PEA', brokers, topics, [] as Set, VERIFIED, load, VERIFIED)
    }

    static ReplicationClusterSnapshot changed(ReplicationClusterSnapshot s, Map values) {
        def get = { String key, original -> values.containsKey(key) ? values[key] : original }
        new ReplicationClusterSnapshot(get('clusterId', s.clusterId()), get('brokers', s.brokers()),
                get('topics', s.topics()), get('reassignments', s.reassignments()), get('reassignmentState', s.reassignmentState()),
                get('replicaCounts', s.replicaCounts()), get('loadState', s.loadState()))
    }

    static ReplicationClusterSnapshot broker(ReplicationClusterSnapshot s, int id, Map values) {
        Broker b = s.brokers()[id]
        def get = { String key, original -> values.containsKey(key) ? values[key] : original }
        def brokers = new LinkedHashMap<>(s.brokers())
        brokers[id] = new Broker(id, get('rack', b.rack()), get('reachable', b.reachable()), get('unfenced', b.unfenced()), get('storage', b.storage()),
                get('uncordoned', b.uncordoned()), get('minimumUsableBytes', b.minimumUsableBytes()))
        changed(s, [brokers:brokers])
    }

    static ReplicationClusterSnapshot topic(ReplicationClusterSnapshot s, String name, Map values) {
        Topic t = s.topics()[name]
        def topics = new LinkedHashMap<>(s.topics())
        topics[name] = new Topic(values.getOrDefault('topicId', t.topicId()), values.getOrDefault('partitions', t.partitions()),
                values.getOrDefault('minInSyncReplicas', t.minInSyncReplicas()))
        changed(s, [topics:topics])
    }

    static TopicDescription description(String name, Topic topic, Map<Integer, Broker> brokers) {
        def node = { int id -> new Node(id, 'unused.invalid', 1, brokers[id]?.rack()?.orElse(null)) }
        new TopicDescription(name, false, topic.partitions().collect { p ->
            new TopicPartitionInfo(p.id(), p.leader().present ? node(p.leader().asInt) : null,
                    p.replicas().collect(node), p.isr().collect(node))
        }, [] as Set, Uuid.fromString(topic.topicId()))
    }
}
