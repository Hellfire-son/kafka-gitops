package com.devshawn.kafka.gitops.service

import org.apache.kafka.clients.admin.Admin
import org.apache.kafka.common.TopicPartition

import java.lang.reflect.Method
import java.time.Duration
import java.util.function.BiFunction
import java.util.function.Supplier

import static com.devshawn.kafka.gitops.ReplicationSafetyTestSupport.*

/** Test-only trust: every VERIFIED admission below is synthetic and never installed by production CLI. */
class ReplicationApplicationTestHarness extends ExecutorHarness {
    List<String> events = []
    int opens
    int executionCalls
    long timeoutMs = 50L
    int batchSize = 1
    Closure executeOverride
    ReassignmentExecutor.ExecutionResult lastResult
    RuntimeException failure
    PreflightReadOnlyAdmin realReaderAdmin

    ReplicationApplicationTestHarness(Map assignments = [orders:[[0,1],[1,0]]], Map targets = [orders:3]) {
        super(assignments,targets,1)
        fake = new OwnedIntegrationAdmin(current:fake.current,events:events)
    }

    // Groovy constructor closures dispatch on the derived fixture; keep this test-only preparation visible.
    ReassignmentExecutor.PreparedOperation prepared(String name,plan) {
        def ready = new ReplicationSafetyPolicy().evaluate(name,plan,observed(fake.current),permissions(),constraints())
        assert ready.status() == com.devshawn.kafka.gitops.domain.plan.ReplicationSafetyResult.Status.READY
        new ReassignmentExecutor.PreparedOperation(name,plan,ready)
    }

    ReplicationApplication application() {
        Supplier<Admin> supplier = { opens++; events.add('open'); fake.admin } as Supplier<Admin>
        AdminReassignmentExecutor.SnapshotReader reader = { Admin supplied,Set names,int timeout ->
            assert supplied.is(fake.admin)
            snapshotReads++
            observed(snapshotMutation(fake.current),[completedAtNanos:OptionalLong.of(clock.now)] + evidence)
        }
        BiFunction<AdminReassignmentExecutor,ReassignmentExecutor.Request,ReassignmentExecutor.ExecutionResult> execute = { engine,request ->
            executionCalls++
            lastResult = executeOverride ? executeOverride.call(engine,request) : engine.execute(request)
            lastResult
        } as BiFunction
        def application = new AdminReplicationApplication(supplier,
                new AdminReassignmentExecutor.Settings(batchSize,Duration.ofMillis(10),Duration.ofMillis(5)),clock,reader,Duration.ofMillis(timeoutMs),execute)
        return ({ topics,permissions ->
            try { application.apply(topics,permissions) }
            catch (RuntimeException ex) { failure = ex; throw ex }
        } as ReplicationApplication)
    }

    /** Uses the unmodified real reader and its System.nanoTime source; all Admin RPCs remain strict proxy fakes. */
    ReplicationApplication applicationWithRealReader() {
        realReaderAdmin = new PreflightReadOnlyAdmin(state:fake.current)
        def time = new AdminReassignmentExecutor.TimeSource() {
            @Override long nanoTime() { System.nanoTime() }
            @Override void pause(long nanos) { throw new AssertionError('No execution or waiting is allowed') }
        }
        new AdminReplicationApplication({ opens++; realReaderAdmin.admin } as Supplier<Admin>,
                new AdminReassignmentExecutor.Settings(1,Duration.ofSeconds(1),Duration.ofMillis(5)),
                time,{ admin,names,timeout -> ReplicationMetadataReader.read(admin,names,timeout,true) },
                Duration.ofSeconds(2),{ engine,request -> executionCalls++; throw new AssertionError('UNKNOWN must block execution') } as BiFunction)
    }
}

class OwnedIntegrationAdmin extends ExecutorAdmin {
    List<String> events
    int closes
    boolean failClose

    @Override Object invoke(Object proxy,Method method,Object[] args) {
        if (method.name == 'close') {
            assert args[0] == Duration.ZERO
            closes++; events.add('close')
            if (failClose) throw new IllegalStateException('private-close-response')
            return null
        }
        if (method.name == 'alterPartitionReassignments') events.add('rf-write')
        super.invoke(proxy,method,args)
    }

    @Override void finish(TopicPartition key,List<Integer> target) {
        super.finish(key,target)
        events.add('rf-target')
    }
}
