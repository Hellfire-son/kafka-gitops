// SPDX-License-Identifier: Apache-2.0
package com.devshawn.kafka.gitops

import com.devshawn.kafka.gitops.config.KafkaGitopsConfig
import com.devshawn.kafka.gitops.domain.plan.AclPlan
import com.devshawn.kafka.gitops.domain.plan.DesiredPlan
import com.devshawn.kafka.gitops.domain.state.AclDetails
import com.devshawn.kafka.gitops.service.KafkaService
import org.apache.kafka.clients.admin.Admin
import org.apache.kafka.clients.admin.CreatePartitionsResult
import org.apache.kafka.clients.admin.DeleteAclsResult
import org.apache.kafka.clients.admin.DeleteAclsResult.FilterResult
import org.apache.kafka.clients.admin.DeleteAclsResult.FilterResults
import org.apache.kafka.clients.admin.DescribeAclsResult
import org.apache.kafka.clients.admin.DescribeTopicsResult
import org.apache.kafka.clients.admin.TopicDescription
import org.apache.kafka.common.KafkaFuture
import org.apache.kafka.common.Node
import org.apache.kafka.common.TopicPartitionInfo
import org.apache.kafka.common.acl.AclBinding
import org.apache.kafka.common.errors.ClusterAuthorizationException
import org.apache.kafka.common.errors.InvalidPartitionsException
import org.apache.kafka.common.errors.TimeoutException
import org.apache.kafka.common.internals.KafkaFutureImpl
import spock.lang.Specification
import spock.lang.TempDir

import java.time.Duration

import static com.devshawn.kafka.gitops.enums.PlanAction.REMOVE

class ApplyRemediationWorkflowSpec extends Specification {
    private static final String PARTITION_UPDATE = '{"name":"orders","action":"UPDATE",' +
            '"partitionPlan":{"action":"UPDATE","previousCount":1,"targetCount":3}}'
    @TempDir File directory
    Admin admin = Mock(Admin)

    void 'the same saved ACL plan reports one deletion and then a successful no-op'() {
        given:
        def acl = binding('orders')
        File saved = new File(directory, 'acl-plan.json')
        File state = writeState()

        when:
        def proposal = run(state, ['--skip-topics', 'plan', '-o', saved.path])

        then:
        1 * admin.describeAcls(_) >> new DescribeAclsResult(KafkaFuture.completedFuture([acl]))
        1 * admin.close(Duration.ZERO)
        0 * admin._
        proposal.code == 0

        when:
        byte[] original = saved.bytes
        def first = run(state, ['--skip-topics', 'apply', '-p', saved.path])
        def repeat = run(state, ['--skip-topics', 'apply', '-p', saved.path])

        then:
        2 * admin.deleteAcls([acl.toFilter()]) >>> [deletion(acl, [new FilterResult(acl, null)]), deletion(acl, [])]
        2 * admin.close(Duration.ZERO)
        0 * admin._
        first.code == 0
        first.text.contains('  Result: SUCCESS\n')
        first.text.contains('ACLs: 0 created, 1 deleted\n')
        first.text.contains('Successful resource changes: 1\n')
        repeat.code == 0
        repeat.text.contains('  Result: NO_OP\n')
        repeat.text.contains('ACLs: 0 created, 0 deleted\n')
        repeat.text.contains('Successful resource changes: 0\n')
        repeat.text.contains('Resources already satisfied: 1\n')
        repeat.text.contains('Status: APPLY_COMPLETED\n')
        saved.bytes == original
    }

    void 'the summary counts confirmed deletions across a mix of existing and missing ACLs'() {
        given:
        def first = binding('first')
        def missing = binding('missing')
        def last = binding('last')
        File saved = aclPlan(first, missing, last)

        when:
        def result = run(writeState(), ['--skip-topics', 'apply', '-p', saved.path])

        then:
        1 * admin.deleteAcls([first.toFilter()]) >> deletion(first, [new FilterResult(first, null)])
        1 * admin.deleteAcls([missing.toFilter()]) >> deletion(missing, [])
        1 * admin.deleteAcls([last.toFilter()]) >> deletion(last, [new FilterResult(last, null)])
        3 * admin.close(Duration.ZERO)
        0 * admin._
        result.code == 0
        result.text.count('  Result: SUCCESS\n') == 2
        result.text.count('  Result: NO_OP\n') == 1
        result.text.contains('ACLs: 0 created, 2 deleted\n')
        result.text.contains('Successful resource changes: 2\n')
        result.text.contains('Resources already satisfied: 1\n')
    }

    void 'ACL deletion reports #kind with accurate counters and stops later operations'() {
        given:
        def acl = binding('orders')
        def pending = binding('pending')
        File saved = aclPlan(acl, pending)
        def denied = new ClusterAuthorizationException('private-diagnostic')
        def response = [
                'filter refusal': new DeleteAclsResult([(acl.toFilter()): failed(denied)]),
                'binding refusal': deletion(acl, [new FilterResult(null, denied)]),
                'binding timeout': deletion(acl, [new FilterResult(null, new TimeoutException('private-diagnostic'))]),
                'pending future': new DeleteAclsResult([(acl.toFilter()): new KafkaFutureImpl<FilterResults>()]),
                'missing filter': new DeleteAclsResult([:]),
                'missing result': new DeleteAclsResult([(acl.toFilter()): KafkaFuture.completedFuture(null)]),
                'unexpected binding': deletion(acl, [new FilterResult(pending, null)]),
                'mixed binding results': deletion(acl, [new FilterResult(null, denied), new FilterResult(acl, null)]),
                'close after empty response': deletion(acl, [])
        ][kind]

        when:
        def result = run(writeState(), ['--skip-topics', 'apply', '-p', saved.path])

        then:
        1 * admin.deleteAcls([acl.toFilter()]) >> response
        1 * admin.close(Duration.ZERO) >> {
            if (kind in ['close after empty response', 'binding refusal']) throw new IllegalStateException('private-close')
        }
        0 * admin._
        result.code == 2
        result.text.contains("  Result: ${operation}\n")
        result.text.contains('  Result: NOT_RUN\n')
        result.text.contains('Successful resource changes: 0\n')
        result.text.contains("ACLs: 0 created, ${deleted} deleted\n")
        result.text.contains("Status: APPLY_${operation}\n")
        result.text.contains(reason)
        !result.text.contains('  Result: NO_OP\n')
        !result.text.contains('private-')

        where:
        kind                        | operation | deleted | reason
        'filter refusal'            | 'FAILED'  | 0       | 'ClusterAuthorizationException'
        'binding refusal'           | 'FAILED'  | 0       | 'ClusterAuthorizationException'
        'binding timeout'           | 'UNKNOWN' | 0       | 'TimeoutException'
        'pending future'            | 'UNKNOWN' | 0       | 'TimeoutException'
        'missing filter'            | 'UNKNOWN' | 0       | 'NullPointerException'
        'missing result'            | 'UNKNOWN' | 0       | 'METADATA_INVALID'
        'unexpected binding'        | 'UNKNOWN' | 0       | 'IllegalStateException'
        'mixed binding results'     | 'PARTIAL' | 1       | 'ClusterAuthorizationException'
        'close after empty response'| 'UNKNOWN' | 0       | 'IllegalStateException'
    }

    void 'a confirmed ACL deletion stays counted when a later distinct binding is refused'() {
        given:
        def first = binding('first')
        def refused = binding('refused')
        def pending = binding('pending')
        File saved = aclPlan(first, refused, pending)

        when:
        def result = run(writeState(), ['--skip-topics', 'apply', '-p', saved.path])

        then:
        1 * admin.deleteAcls([first.toFilter()]) >> deletion(first, [new FilterResult(first, null)])
        1 * admin.deleteAcls([refused.toFilter()]) >> deletion(refused,
                [new FilterResult(null, new ClusterAuthorizationException('private-refused'))])
        2 * admin.close(Duration.ZERO)
        0 * admin._
        result.code == 2
        result.text.contains('ACLs: 0 created, 1 deleted\n')
        result.text.contains('Successful resource changes: 1\n')
        result.text.contains('Failed resource changes: 1\n')
        result.text.contains('Resources not run: 1\n')
        result.text.contains('Status: APPLY_PARTIAL\n')
    }

    void 'drift between partition preflight and the immediate safety read is BLOCKED with previous writes=#previousWrite'() {
        given:
        File saved = partitionPlan(previousWrite)

        when:
        def result = run(writeState(), ['apply', '-p', saved.path])

        then:
        1 * admin.describeTopics(['orders'], _) >> described(1)
        1 * admin.close(Duration.ZERO)
        then:
        if (previousWrite) {
            1 * admin.createTopics({ it*.name() == ['earlier'] }) >>
                    new ApplyCreateTopicsResponse(KafkaFuture.completedFuture(null))
            1 * admin.close(Duration.ZERO)
        }
        then:
        1 * admin.describeTopics(['orders'], _) >> described(2)
        1 * admin.close(Duration.ZERO) >> { throw new IllegalStateException('private-close') }
        0 * admin._
        result.code == 2
        result.text.contains('PARTITION_PLAN_STALE: Partition plan is stale.')
        result.text.contains('Topic orders has 2 partitions, expected 1.')
        result.text.contains('  Result: BLOCKED\n')
        result.text.contains('  Result: NOT_RUN\n')
        result.text.contains('Blocked resources: 1\n')
        result.text.contains('Resources not run: 1\n')
        result.text.contains("Successful resource changes: ${previousWrite ? 1 : 0}\n")
        result.text.contains("Status: APPLY_${previousWrite ? 'PARTIAL' : 'BLOCKED'}\n")
        !result.text.contains('private-close')

        where:
        previousWrite << [false, true]
    }

    void 'partition writes distinguish #caseName from stale pre-write drift'() {
        given:
        File saved = partitionPlan(false)
        KafkaFuture<Void> future = caseName == 'timeout' ? new KafkaFutureImpl<Void>() :
                failed(new InvalidPartitionsException('private-refused'))

        when:
        def result = run(writeState(), ['apply', '-p', saved.path])

        then:
        1 * admin.describeTopics(['orders'], _) >> described(1)
        1 * admin.close(Duration.ZERO)
        then:
        1 * admin.describeTopics(['orders'], _) >> described(1)
        1 * admin.createPartitions({ it.orders.totalCount() == 3 }, _) >> new CreatePartitionsResult(['orders': future])
        1 * admin.close(Duration.ZERO)
        0 * admin._
        result.code == 2
        result.text.contains("  Result: ${operation}\n")
        result.text.contains('  Result: NOT_RUN\n')
        result.text.contains("Status: APPLY_${operation}\n")
        result.text.contains(reason)
        !result.text.contains('private-refused')

        where:
        caseName  | operation | reason
        'timeout' | 'UNKNOWN' | 'TimeoutException'
        'refusal' | 'FAILED'  | 'InvalidPartitionsException'
    }

    private File writeState() {
        File state = new File(directory, 'state.yaml')
        state.setText('topics: {}\n', 'UTF-8')
        state
    }

    private Map run(File state, List<String> arguments) {
        Admin supplied = admin
        def config = new KafkaGitopsConfig.Builder().putConfig('default.api.timeout.ms', 500).build()
        def kafka = new KafkaService(config) {
            @Override protected Admin buildAdminClient() { supplied }
        }
        def result = PlanReportKafka.cli(state, kafka, arguments)
        result.text = result.text.replace('\r\n', '\n')
        result
    }

    private File aclPlan(AclBinding... acls) {
        def plan = new DesiredPlan.Builder()
        acls.each { acl ->
            plan.addAclPlans(new AclPlan.Builder().setName(acl.pattern().name()).setAction(REMOVE)
                    .setAclDetails(AclDetails.fromAclBinding(acl)).build())
        }
        File file = new File(directory, 'acl-plan.json')
        PlanTestSupport.mapper().writeValue(file, plan.build())
        file
    }

    private File partitionPlan(boolean previousWrite) {
        def create = { name -> "{\"name\":\"${name}\",\"action\":\"ADD\",\"topicDetails\":{\"partitions\":1,\"replication\":1}}" }
        def topics = (previousWrite ? [create('earlier')] : []) + [PARTITION_UPDATE, create('pending')]
        File file = new File(directory, 'partition-plan.json')
        file.setText('{"topicPlans":[' + topics.join(',') + ']}', 'UTF-8')
        file
    }

    private static AclBinding binding(String name) {
        new AclDetails.Builder().setName(name).setType('TOPIC').setPattern('LITERAL')
                .setPrincipal('User:read-only-fixture').setOperation('READ').build().toAclBinding()
    }

    private static DeleteAclsResult deletion(AclBinding binding, List<FilterResult> results) {
        new DeleteAclsResult([(binding.toFilter()): KafkaFuture.completedFuture(new FilterResults(results))])
    }

    private static KafkaFuture failed(Throwable failure) {
        def future = new KafkaFutureImpl()
        future.completeExceptionally(failure)
        future
    }

    private static DescribeTopicsResult described(int count) {
        def broker = new Node(1, 'localhost', 1)
        def partitions = (0..<count).collect { new TopicPartitionInfo(it, broker, [broker], [broker]) }
        new DescribeTopicsResult(null, ['orders': KafkaFuture.completedFuture(new TopicDescription('orders', false, partitions))])
    }
}
