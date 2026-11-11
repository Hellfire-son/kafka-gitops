package com.devshawn.kafka.gitops.service

import com.devshawn.kafka.gitops.exception.ConfluentCloudException
import spock.lang.Specification
import tools.jackson.databind.json.JsonMapper

class ConfluentCloudServiceSpec extends Specification {
    void 'Jackson 3 parse failures remain safe for #operation'() {
        given:
        def service = new ConfluentCloudService(JsonMapper.builderWithJackson2Defaults().build(),
                { String[] ignored -> 'private-ccloud-response-token' } as ConfluentCloudService.CommandRunner)

        when:
        if (operation == 'list') service.getServiceAccounts()
        else service.createServiceAccount('example', false)

        then:
        def error = thrown(ConfluentCloudException)
        !error.message.contains('private-ccloud-response-token')
        error.cause == null

        where:
        operation << ['list', 'create']
    }

    void 'command IO failures remain safe for #operation'() {
        given:
        def service = new ConfluentCloudService(JsonMapper.builderWithJackson2Defaults().build(),
                { String[] ignored -> throw new IOException('private-ccloud-io-token') } as ConfluentCloudService.CommandRunner)

        when:
        if (operation == 'list') service.getServiceAccounts()
        else service.createServiceAccount('example', true)

        then:
        def error = thrown(ConfluentCloudException)
        !error.message.contains('private-ccloud-io-token')
        error.cause == null

        where:
        operation << ['list', 'create']
    }

    void 'Jackson 3 retains the generated service account builder contract'() {
        given:
        List<List<String>> commands = []
        def service = new ConfluentCloudService(JsonMapper.builderWithJackson2Defaults().build(),
                { String[] command ->
                    commands.add(command.toList())
                    command.contains('list') ? '[{"id":42,"name":"example","description":"description"}]' :
                            '{"id":43,"name":"user-example","description":"User: example"}'
                } as ConfluentCloudService.CommandRunner)

        expect:
        service.getServiceAccounts().first().name == 'example'
        service.createServiceAccount('example', true).name == 'user-example'
        commands[1].contains('user-example')
        commands[1].contains('User: example')
    }
}
