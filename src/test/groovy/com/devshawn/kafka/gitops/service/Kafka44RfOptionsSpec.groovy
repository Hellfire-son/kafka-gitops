package com.devshawn.kafka.gitops.service

import org.apache.kafka.clients.admin.Admin
import org.apache.kafka.clients.admin.AlterPartitionReassignmentsOptions
import org.apache.kafka.common.errors.UnsupportedVersionException
import org.apache.kafka.common.message.AlterPartitionReassignmentsRequestData
import org.apache.kafka.common.protocol.ByteBufferAccessor
import org.apache.kafka.common.protocol.ObjectSerializationCache
import org.apache.kafka.common.requests.AlterPartitionReassignmentsRequest
import spock.lang.Specification

import java.lang.reflect.InvocationTargetException
import java.lang.reflect.Proxy
import java.time.Duration

import static com.devshawn.kafka.gitops.ReplicationSafetyTestSupport.observed

/** The actual Client 4.4 defaults and protocol builders, with no broker or network. */
class Kafka44RfOptionsSpec extends Specification {
    void 'Kafka 4_4 permits RF change by default; omission is not a false default defect'() {
        expect:
        new AlterPartitionReassignmentsOptions().allowReplicationFactorChange()
    }

    void 'true RF option serializes on legacy v0 without requiring a newer broker API'() {
        given:
        def data=new AlterPartitionReassignmentsRequestData().setAllowReplicationFactorChange(true).setTimeoutMs(1000)

        when:
        def request=new AlterPartitionReassignmentsRequest.Builder(data).build((short)0)
        def bytes=java.nio.ByteBuffer.allocate(1024)
        request.data().write(new ByteBufferAccessor(bytes),new ObjectSerializationCache(),(short)0)

        then:
        request.version()==0
        bytes.position()>0
    }

    void 'false RF option on legacy v0 yields UnsupportedVersion rather than silently losing the guard'() {
        when:
        new AlterPartitionReassignmentsRequest.Builder(new AlterPartitionReassignmentsRequestData()
                .setAllowReplicationFactorChange(false)).build((short)0)

        then:
        thrown(UnsupportedVersionException)
    }

    void 'false RF option is representable in the new request version'() {
        expect:
        !new AlterPartitionReassignmentsRequest.Builder(new AlterPartitionReassignmentsRequestData()
                .setAllowReplicationFactorChange(false)).build((short)1).data().allowReplicationFactorChange()
    }

    void 'production executor submits an allowed RF change only after policy: #previous -> #target'() {
        given:
        def h=new ExecutorHarness([orders:previous],[orders:target],100)
        List<Boolean> flags=[]
        Admin proxy=Proxy.newProxyInstance(Admin.classLoader,[Admin] as Class[],{ p,m,args ->
            if(m.name=='alterPartitionReassignments') flags.add(args[1].allowReplicationFactorChange())
            try { m.invoke(h.fake.admin,args) } catch(InvocationTargetException e) { throw e.cause }
        } as java.lang.reflect.InvocationHandler) as Admin
        def reader={ session,names,timeout ->
            assert session.is(proxy)
            observed(h.fake.current,[completedAtNanos:OptionalLong.of(h.clock.now)])
        } as AdminReassignmentExecutor.SnapshotReader
        def executor=new AdminReassignmentExecutor(proxy,
                new AdminReassignmentExecutor.Settings(100,Duration.ofMillis(10),Duration.ofMillis(5)),h.clock,reader)

        when:
        def result=executor.execute(h.request)

        then:
        result.state()==ReassignmentExecutor.State.COMPLETED
        flags==[true]
        h.fake.writes.first()==h.targets

        where:
        previous | target
        [[0],[1]] | 2
        [[0,1],[1,0]] | 3
        [[0,1,2],[1,2,0]] | 2
    }
}
