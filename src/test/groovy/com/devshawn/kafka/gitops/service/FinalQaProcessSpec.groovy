// SPDX-License-Identifier: Apache-2.0
package com.devshawn.kafka.gitops.service

import spock.lang.Specification
import java.time.Duration

class FinalQaProcessSpec extends Specification {
    String[] command(String mode) {
        [new File(System.getProperty('java.home'),'bin/java.exe').absolutePath,'-cp',
         new File(QaProcessProbe.protectionDomain.codeSource.location.toURI()).absolutePath,
         QaProcessProbe.name,mode] as String[]
    }
    void cleanup() { Thread.interrupted() }

    void 'local command output is returned only after successful exit'() {
        expect:
        ConfluentCloudService.execCmd(command('ok'))=='{"id":"synthetic"}'
    }

    void 'valid JSON stdout from a failed command cannot become a successful account result'() {
        when:
        ConfluentCloudService.execCmd(command('failed'))

        then:
        def error=thrown(IOException)
        !error.message.contains('private-')
        error.cause==null
    }

    void 'local command deadline terminates a pending child within a bounded time'() {
        given:
        long start=System.nanoTime()

        when:
        ConfluentCloudService.execCmd(command('pending'),Duration.ofMillis(250))

        then:
        thrown(IOException)
        System.nanoTime()-start < Duration.ofSeconds(5).toNanos()
    }

    void 'oversized local command output is rejected rather than retained without a bound'() {
        when:
        ConfluentCloudService.execCmd(command('oversize'))

        then:
        thrown(IOException)
    }

    void 'an interrupted caller cannot start an external command and retains its interrupt flag'() {
        given:
        Thread.currentThread().interrupt()

        when:
        ConfluentCloudService.execCmd(command('ok'))

        then:
        thrown(IOException)
        Thread.currentThread().isInterrupted()
    }
}
