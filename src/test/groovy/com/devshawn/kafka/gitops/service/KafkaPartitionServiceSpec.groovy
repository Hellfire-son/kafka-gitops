// SPDX-License-Identifier: Apache-2.0
package com.devshawn.kafka.gitops.service

import com.devshawn.kafka.gitops.config.KafkaGitopsConfig
import com.devshawn.kafka.gitops.exception.KafkaExecutionException
import com.devshawn.kafka.gitops.util.LogUtil
import org.apache.kafka.clients.admin.Admin
import org.apache.kafka.clients.admin.CreatePartitionsResult
import org.apache.kafka.clients.admin.DescribeTopicsResult
import org.apache.kafka.clients.admin.TopicDescription
import org.apache.kafka.common.KafkaFuture
import org.apache.kafka.common.Node
import org.apache.kafka.common.TopicPartitionInfo
import org.apache.kafka.common.errors.InvalidPartitionsException
import org.apache.kafka.common.errors.TopicAuthorizationException
import org.apache.kafka.common.internals.KafkaFutureImpl
import spock.lang.Specification
import spock.lang.Unroll

import java.time.Duration

import static com.devshawn.kafka.gitops.manager.PartitionPlanSpec.capture

class KafkaPartitionServiceSpec extends Specification {
    Admin admin = Mock(Admin)

    private KafkaService service(int timeout = 2000) {
        new SuppliedAdminService(new KafkaGitopsConfig.Builder().putConfig('default.api.timeout.ms', timeout)
                .putConfig('sasl.jaas.config', 'private-stage5-jaas').build(), admin)
    }

    void 'increaseTo sends final count and verifies metadata before returning'() {
        given:
        def kafka = service()

        when:
        boolean changed = kafka.increaseTopicPartitions('orders', 3, 6)

        then:
        1 * admin.describeTopics(['orders'], _) >> described(3)
        then:
        1 * admin.createPartitions(_, _) >> { Map request, options ->
            assert request.keySet() == ['orders'] as Set
            assert request.orders.totalCount() == 6
            assert request.orders.assignments() == null
            assert options.timeoutMs() > 0 && options.timeoutMs() <= 2000
            new CreatePartitionsResult(['orders': KafkaFuture.completedFuture(null)])
        }
        then:
        1 * admin.describeTopics(['orders'], _) >> described(6)
        1 * admin.close(Duration.ZERO)
        changed
        0 * admin._
    }

    void 'controller acknowledgement waits for metadata propagation'() {
        when:
        boolean changed = service().increaseTopicPartitions('orders', 3, 6)

        then:
        3 * admin.describeTopics(['orders'], _) >>> [described(3), described(3), described(6)]
        1 * admin.createPartitions(_, _) >> new CreatePartitionsResult(['orders': KafkaFuture.completedFuture(null)])
        1 * admin.close(Duration.ZERO)
        changed
    }

    void 'already completed target is a read-only no-op'() {
        when:
        boolean changed = service().increaseTopicPartitions('orders', 3, 6)

        then:
        !changed
        1 * admin.describeTopics(['orders'], _) >> described(6)
        0 * admin.createPartitions(_, _)
        1 * admin.close(Duration.ZERO)
    }

    @Unroll
    void 'drift to #actual partitions never issues a write'() {
        when:
        service().increaseTopicPartitions('orders', 3, 6)

        then:
        def error = thrown(KafkaExecutionException)
        error.exceptionMessage.contains('has ' + actual)
        1 * admin.describeTopics(['orders'], _) >> described(actual)
        0 * admin.createPartitions(_, _)
        1 * admin.close(Duration.ZERO)

        where:
        actual << [1, 4, 7]
    }

    void 'acknowledged but different result is not reported as success'() {
        when:
        service().increaseTopicPartitions('orders', 3, 6)

        then:
        def error = thrown(KafkaExecutionException)
        error.message.contains('differs')
        error.exceptionMessage.contains('has 5 partitions')
        2 * admin.describeTopics(['orders'], _) >>> [described(3), described(5)]
        1 * admin.createPartitions(_, _) >> new CreatePartitionsResult(['orders': KafkaFuture.completedFuture(null)])
        1 * admin.close(Duration.ZERO)
    }

    void 'metadata that never reaches target causes a bounded verification timeout'() {
        when:
        service(200).increaseTopicPartitions('orders', 3, 6)

        then:
        def error = thrown(KafkaExecutionException)
        error.exceptionMessage.contains('TimeoutException')
        error.exceptionMessage.contains('partially applied')
        (2.._) * admin.describeTopics(['orders'], _) >> described(3)
        1 * admin.createPartitions(_, _) >> new CreatePartitionsResult(['orders': KafkaFuture.completedFuture(null)])
        1 * admin.close(Duration.ZERO)
    }

    @Unroll
    void 'API error #failure never exposes exception text or modifies input configuration'() {
        given:
        def config = new KafkaGitopsConfig.Builder().putConfig('sasl.jaas.config', 'private-stage5-jaas').build()
        def kafka = new SuppliedAdminService(config, admin)

        when:
        kafka.increaseTopicPartitions('orders', 3, 6)

        then:
        def error = thrown(KafkaExecutionException)
        String text = capture { LogUtil.printKafkaExecutionError(error, true) }
        !text.contains('private-stage5')
        text.contains(failure.class.simpleName)
        text.contains('partially applied')
        config.config['sasl.jaas.config'] == 'private-stage5-jaas'
        1 * admin.describeTopics(['orders'], _) >> described(3)
        1 * admin.createPartitions(_, _) >> new CreatePartitionsResult(['orders': failed(failure)])
        1 * admin.close(Duration.ZERO)

        where:
        failure << [new TopicAuthorizationException('private-stage5-auth'),
                    new org.apache.kafka.common.errors.TimeoutException('private-stage5-timeout'),
                    new InvalidPartitionsException('private-stage5-race')]
    }

    void 'uncompleted mutation future times out and never confirms success'() {
        when:
        service(200).increaseTopicPartitions('orders', 3, 6)

        then:
        def error = thrown(KafkaExecutionException)
        error.exceptionMessage.contains('TimeoutException')
        1 * admin.describeTopics(['orders'], _) >> described(3)
        1 * admin.createPartitions(_, _) >> new CreatePartitionsResult(['orders': new KafkaFutureImpl<Void>()])
        1 * admin.close(Duration.ZERO)
    }

    void 'interrupted metadata wait restores interrupt and closes Admin'() {
        given:
        Thread.currentThread().interrupt()

        when:
        service().describeTopics(['orders'])

        then:
        def error = thrown(KafkaExecutionException)
        error.exceptionMessage.contains('InterruptedException')
        Thread.currentThread().isInterrupted()
        1 * admin.describeTopics(['orders'], _) >> new DescribeTopicsResult(null, ['orders': new KafkaFutureImpl<TopicDescription>()])
        1 * admin.close(Duration.ZERO)

        cleanup:
        Thread.interrupted()
    }

    void 'uncompleted metadata query times out safely'() {
        when:
        service(200).describeTopics(['orders'])

        then:
        def error = thrown(KafkaExecutionException)
        error.exceptionMessage.contains('TimeoutException')
        1 * admin.describeTopics(['orders'], _) >> new DescribeTopicsResult(null, ['orders': new KafkaFutureImpl<TopicDescription>()])
        1 * admin.close(Duration.ZERO)
    }

    void 'authorization failure during preflight exposes only its category'() {
        when:
        service().describeTopics(['orders'])

        then:
        def error = thrown(KafkaExecutionException)
        !error.exceptionMessage.contains('private-stage5')
        error.exceptionMessage.contains('TopicAuthorizationException')
        1 * admin.describeTopics(['orders'], _) >> new DescribeTopicsResult(null, ['orders': failed(new TopicAuthorizationException('private-stage5-secret'))])
        1 * admin.close(Duration.ZERO)
    }

    void 'empty description set does not construct an Admin client'() {
        when:
        def descriptions = service().describeTopics([])

        then:
        descriptions.isEmpty()
        0 * admin._
    }

    @Unroll
    void 'invalid increase #previous -> #target is rejected without an Admin client'() {
        when:
        service().increaseTopicPartitions('orders', previous, target)

        then:
        thrown(IllegalArgumentException)
        0 * admin._

        where:
        previous | target
        0        | 6
        3        | 0
        6        | 3
        3        | 3
    }

    void 'Admin initialization error cannot expose old wrapper or JAAS details'() {
        given:
        KafkaService kafka = new KafkaService(new KafkaGitopsConfig.Builder().build()) {
            @Override
            protected Admin buildAdminClient() {
                throw new KafkaExecutionException('private-stage5-wrapper', 'private-stage5-jaas')
            }
        }

        when:
        kafka.describeTopics(['orders'])

        then:
        def error = thrown(KafkaExecutionException)
        !error.message.contains('private-stage5')
        !error.exceptionMessage.contains('private-stage5')
    }

    void 'failed post-write description leaves the outcome unconfirmed and closes Admin'() {
        when:
        service().increaseTopicPartitions('orders', 3, 6)

        then:
        def error = thrown(KafkaExecutionException)
        !error.exceptionMessage.contains('private-stage5')
        error.exceptionMessage.contains('partially applied')
        2 * admin.describeTopics(['orders'], _) >>> [described(3),
                new DescribeTopicsResult(null, ['orders': failed(new TopicAuthorizationException('private-stage5-secret'))])]
        1 * admin.createPartitions(_, _) >> new CreatePartitionsResult(['orders': KafkaFuture.completedFuture(null)])
        1 * admin.close(Duration.ZERO)
    }

    @Unroll
    void 'rejects invalid default API timeout #iterationIndex without exposing its value'() {
        given:
        def config = new KafkaGitopsConfig.Builder().putConfig('default.api.timeout.ms', value).build()

        when:
        new SuppliedAdminService(config, admin).describeTopics(['orders'])

        then:
        def error = thrown(KafkaExecutionException)
        error.message == 'Invalid Kafka operation timeout.'
        !error.exceptionMessage.contains('private-stage5')
        0 * admin._

        where:
        value << [0, -1, 'private-stage5-config']
    }

    private static DescribeTopicsResult described(int count) {
        Node node = new Node(0, 'unused-host', 1)
        def topic = new TopicDescription('orders', false, (0..<count).collect { new TopicPartitionInfo(it, node, [node], [node]) })
        new DescribeTopicsResult(null, ['orders': KafkaFuture.completedFuture(topic)])
    }

    private static KafkaFuture failed(Throwable error) {
        def future = new KafkaFutureImpl()
        future.completeExceptionally(error)
        future
    }
}

class SuppliedAdminService extends KafkaService {
    private final Admin supplied
    SuppliedAdminService(KafkaGitopsConfig config, Admin supplied) {
        super(config)
        this.supplied = supplied
    }
    @Override
    protected Admin buildAdminClient() { supplied }
}
