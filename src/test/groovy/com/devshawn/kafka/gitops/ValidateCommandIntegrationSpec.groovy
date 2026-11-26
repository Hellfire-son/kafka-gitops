package com.devshawn.kafka.gitops


import spock.lang.Specification
import spock.lang.Unroll

@Unroll
class ValidateCommandIntegrationSpec extends Specification {

    void 'test validate state files - #planName'() {
        setup:
        String file = TestUtils.getResourceFilePath("plans/${planName}.yaml")

        when:
        def result = TestUtils.runCli("-f", file, "validate")

        then:
        result.code == 0
        result.out == "[VALID] Successfully validated the desired state file.\n"
        result.err.empty

        where:
        planName << [
                "simple",
                "application-service",
                "kafka-connect-service",
                "kafka-streams-service",
                "topics-and-services",
                "multi-file"
        ]
    }

    void 'test invalid state files - #planName'() {
        setup:
        String file = TestUtils.getResourceFilePath("plans/${planName}.yaml")
        String output = TestUtils.getResourceFileContent("plans/${planName}-validate-output.txt")

        when:
        def result = TestUtils.runCli("-f", file, "validate")

        then:
        result.code == 2
        result.out == output
        result.err.empty

        where:
        planName << [
                "invalid-custom-service-acls-1",
                "invalid-custom-service-acls-2",
                "invalid-custom-user-acls-1",
                "invalid-custom-user-acls-2"
        ]
    }
}
