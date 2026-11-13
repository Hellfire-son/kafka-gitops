package com.devshawn.kafka.gitops.service

import com.devshawn.kafka.gitops.domain.metadata.ReplicationClusterSnapshot.Partition
import org.apache.kafka.common.Node
import org.apache.kafka.common.TopicPartitionInfo
import spock.lang.Specification

import static com.devshawn.kafka.gitops.domain.metadata.ReplicationSafetyEvidence.ElrMode.*

class ElrSafetyPolicySpec extends Specification {
    void 'ELR assessment distinguishes mode, missing and confirmed empty data: #label'() {
        given:
        def p=new Partition(0,[1,2],leader, isr as Set,
                Optional.ofNullable(elr),Optional.ofNullable(last))

        expect:
        ElrSafetyPolicy.assess(mode,p).state().name()==state
        ElrSafetyPolicy.assess(mode,p).reason().name()==reason

        where:
        label | mode | elr | last | isr | leader | state | reason
        'old broker' | NOT_APPLICABLE | null | null | [1,2] | OptionalInt.of(1) | 'NOT_APPLICABLE' | 'ELR_NOT_APPLICABLE'
        'known disabled' | DISABLED | null | null | [1,2] | OptionalInt.of(1) | 'SUPPORTED' | 'ELR_DISABLED'
        'enabled healthy' | ENABLED | [] | [] | [1,2] | OptionalInt.of(1) | 'SUPPORTED' | 'ELR_VERIFIED_EMPTY'
        'mode unknown despite arrays' | UNKNOWN | [] | [] | [1,2] | OptionalInt.of(1) | 'UNKNOWN' | 'ELR_MODE_UNESTABLISHED'
        'null mode' | null | [] | [] | [1,2] | OptionalInt.of(1) | 'UNKNOWN' | 'ELR_MODE_UNESTABLISHED'
        'missing both arrays' | ENABLED | null | null | [1,2] | OptionalInt.of(1) | 'UNKNOWN' | 'ELR_METADATA_UNAVAILABLE'
        'missing last known' | ENABLED | [] | null | [1,2] | OptionalInt.of(1) | 'UNKNOWN' | 'ELR_METADATA_UNAVAILABLE'
        'missing ELR' | ENABLED | null | [] | [1,2] | OptionalInt.of(1) | 'UNKNOWN' | 'ELR_METADATA_UNAVAILABLE'
        'ELR is not ISR' | ENABLED | [2] | [] | [1] | OptionalInt.of(1) | 'UNSAFE' | 'ELR_NONEMPTY'
        'last known is not synchronization' | ENABLED | [] | [2] | [1,2] | OptionalInt.of(1) | 'UNSAFE' | 'LAST_KNOWN_ELR_NONEMPTY'
        'unknown broker ID' | ENABLED | [9] | [] | [1,2] | OptionalInt.of(1) | 'UNSAFE' | 'ELR_METADATA_INVALID'
        'duplicate ID' | ENABLED | [2,2] | [] | [1,2] | OptionalInt.of(1) | 'UNSAFE' | 'ELR_METADATA_INVALID'
        'negative ID' | ENABLED | [] | [-1] | [1,2] | OptionalInt.of(1) | 'UNSAFE' | 'ELR_METADATA_INVALID'
        'incomplete ISR despite empty ELR' | ENABLED | [] | [] | [1] | OptionalInt.of(1) | 'UNSAFE' | 'ELR_ISR_UNHEALTHY'
        'missing leader' | ENABLED | [] | [] | [1,2] | OptionalInt.empty() | 'UNSAFE' | 'ELR_ISR_UNHEALTHY'
        'outside leader' | ENABLED | [] | [] | [1,2] | OptionalInt.of(9) | 'UNSAFE' | 'ELR_ISR_UNHEALTHY'
        'disabled with contradictory evidence' | DISABLED | [2] | [] | [1,2] | OptionalInt.of(1) | 'UNSAFE' | 'ELR_NONEMPTY'
    }

    void 'actual Kafka 4_4 constructors preserve Metadata fallback UNKNOWN and DescribeTopicPartitions empty fields'() {
        given:
        def n=new Node(1,'unused.invalid',1)

        expect:
        ElrSafetyPolicy.partition(new TopicPartitionInfo(0,n,[n],[n])).elr().empty
        ElrSafetyPolicy.partition(new TopicPartitionInfo(0,n,[n],[n],[],[])).elr().get().empty
        !ElrSafetyPolicy.partition(new TopicPartitionInfo(0,n,[n],[n],[],[])).elr().empty
    }
}
