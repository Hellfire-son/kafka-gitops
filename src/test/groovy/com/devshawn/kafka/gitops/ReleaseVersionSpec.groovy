// SPDX-License-Identifier: Apache-2.0
package com.devshawn.kafka.gitops

import picocli.CommandLine
import spock.lang.Specification

class ReleaseVersionSpec extends Specification {
    void 'version help is 0.4.1 without reading configuration or creating a manager: #args'() {
        given:
        def factory = Mock(MainCommand.StateManagerFactory)
        def output = new StringWriter()
        def cli = new CommandLine(new MainCommand(factory))
        cli.setOut(new PrintWriter(output))
        cli.setErr(new PrintWriter(output))

        when:
        int exit = cli.execute(*args)

        then:
        exit == 0
        output.toString().trim() == '0.4.1'
        0 * factory._

        where:
        args << [['--version'], ['-V'], ['-c', 'missing-release.properties', '--version'],
                 ['-f', 'missing-release.yaml', '--version']]
    }
}
