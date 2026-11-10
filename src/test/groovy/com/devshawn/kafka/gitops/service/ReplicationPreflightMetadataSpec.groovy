package com.devshawn.kafka.gitops.service

import com.devshawn.kafka.gitops.config.KafkaGitopsConfig
import com.devshawn.kafka.gitops.domain.metadata.ReplicationSafetyEvidence
import org.apache.kafka.clients.admin.*
import org.apache.kafka.common.KafkaFuture
import org.apache.kafka.common.errors.UnsupportedVersionException
import spock.lang.Specification
import spock.lang.Unroll

import java.lang.reflect.Method

import static com.devshawn.kafka.gitops.domain.metadata.ReplicationClusterSnapshot.Observation.*
import static com.devshawn.kafka.gitops.domain.metadata.ReplicationSafetyEvidence.ElrMode.*

class ReplicationPreflightMetadataSpec extends Specification {
    PreflightReadOnlyAdmin fake = new PreflightReadOnlyAdmin()

    private KafkaService service() {
        new KafkaService(new KafkaGitopsConfig.Builder().putConfig('default.api.timeout.ms',2000).build()) {
            @Override protected Admin buildAdminClient() { fake.admin }
        }
    }

    void 'fresh preflight reads features twice and rechecks configs without manufacturing safety'() {
        when:
        long started = System.nanoTime()
        def result = service().readReplicationPreflightSnapshot(['orders'] as Set)

        then:
        result.safety().completedAtNanos().asLong >= started
        result.safety().elrMode() == DISABLED
        result.safety().featuresStable() == VERIFIED
        result.safety().configsStable().orders == VERIFIED
        result.safety().cordoningStable().values().every { it == VERIFIED }
        result.safety().admissions().isEmpty()
        result.brokers().values().every { it.unfenced() == com.devshawn.kafka.gitops.domain.metadata.ReplicationClusterSnapshot.Observation.UNKNOWN }
        fake.calls.count('describeFeatures') == 2
        fake.calls.count('describeConfigs') == 2
        fake.calls.count('close') == 1
        !result.toString().contains('private-')
    }

    @Unroll
    void 'ELR feature metadata #description gives #mode'() {
        given:
        fake.feature = feature

        when:
        def result = service().readReplicationPreflightSnapshot(['orders'] as Set)

        then:
        result.safety().elrMode() == mode
        fake.calls.count('close') == 1

        where:
        description | feature | mode
        'enabled' | [finalized:['eligible.leader.replicas.version':new FinalizedVersionRange((short)1,(short)1)]] | ENABLED
        'unsupported future level' | [finalized:['eligible.leader.replicas.version':new FinalizedVersionRange((short)2,(short)2)]] | com.devshawn.kafka.gitops.domain.metadata.ReplicationSafetyEvidence.ElrMode.UNKNOWN
        'unknown epoch' | [epoch:Optional.empty()] | com.devshawn.kafka.gitops.domain.metadata.ReplicationSafetyEvidence.ElrMode.UNKNOWN
        'missing support metadata' | [supported:[:]] | com.devshawn.kafka.gitops.domain.metadata.ReplicationSafetyEvidence.ElrMode.UNKNOWN
        'denied or unsupported RPC' | [failure:true] | com.devshawn.kafka.gitops.domain.metadata.ReplicationSafetyEvidence.ElrMode.UNKNOWN
    }

    void 'changed min ISR during preflight is recorded rather than hidden by the first read'() {
        given:
        fake.configDrift = true

        when:
        def result = service().readReplicationPreflightSnapshot(['orders'] as Set)

        then:
        result.topics().orders.minInSyncReplicas().asInt == 2
        result.safety().configsStable().orders == UNAVAILABLE
    }

    void 'cordoning change during preflight blocks placement and is retained as a typed observation'() {
        given:
        fake.cordonDrift = true

        when:
        def result = service().readReplicationPreflightSnapshot(['orders'] as Set)

        then:
        result.brokers()[2].uncordoned() == UNAVAILABLE
        result.safety().cordoningStable()[2] == UNAVAILABLE
        !result.toString().contains('private-')
    }

    void 'feature epoch drift is never mistaken for a stable disabled ELR mode'() {
        given:
        fake.featureDrift = true

        expect:
        service().readReplicationPreflightSnapshot(['orders'] as Set).safety().featuresStable() == UNAVAILABLE
    }

    void 'unknown config state remains unknown during both reads'() {
        given:
        fake.configFailure = new org.apache.kafka.common.errors.ClusterAuthorizationException('private-config')

        when:
        def result = service().readReplicationPreflightSnapshot(['orders'] as Set)

        then:
        result.safety().configsStable().orders == com.devshawn.kafka.gitops.domain.metadata.ReplicationClusterSnapshot.Observation.UNKNOWN
        result.safety().cordoningStable().values().every { it == com.devshawn.kafka.gitops.domain.metadata.ReplicationClusterSnapshot.Observation.UNKNOWN }
        !result.toString().contains('private-')
    }
}

/** Inherits the strict read-only Admin allowlist; only describeFeatures is added. */
class PreflightReadOnlyAdmin extends ReadOnlyAdmin {
    Map feature = [:]
    boolean configDrift
    boolean cordonDrift
    boolean featureDrift

    @Override Object invoke(Object proxy, Method method, Object[] args) {
        if (method.name == 'describeFeatures') {
            calls.add(method.name)
            assert args[0].timeoutMs() > 0 && args[0].timeoutMs() <= 2000
            if (feature.failure) throw new UnsupportedVersionException('private-feature-response')
            def epoch = feature.getOrDefault('epoch',Optional.of(featureDrift && calls.count(method.name) > 1 ? 8L : 7L))
            def metadata = new FeatureMetadata(feature.getOrDefault('finalized',[:]),epoch,
                    feature.getOrDefault('supported',['eligible.leader.replicas.version':new SupportedVersionRange((short)0,(short)1)]))
            return new DescribeFeaturesResult(KafkaFuture.completedFuture(metadata))
        }
        if (method.name == 'describeConfigs' && calls.count(method.name) == 1) {
            if (configDrift) topicEntries = [ReplicationMetadataSpec.entry('min.insync.replicas','2')]
            if (cordonDrift) brokerEntries[2] = [ReplicationMetadataSpec.entry('cordoned.log.dirs','/private-cordoned-path')]
        }
        return super.invoke(proxy,method,args)
    }
}
