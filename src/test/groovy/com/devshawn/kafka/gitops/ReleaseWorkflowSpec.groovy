// SPDX-License-Identifier: Apache-2.0
package com.devshawn.kafka.gitops

import spock.lang.Specification
import tools.jackson.dataformat.yaml.YAMLMapper

class ReleaseWorkflowSpec extends Specification {
    void 'automatic CI has no publishing trigger or write permission'() {
        given:
        def files = new File('.github/workflows').listFiles().findAll { it.name.endsWith('.yml') || it.name.endsWith('.yaml') }
        def flow = new YAMLMapper().readTree(new File('.github/workflows/gradle.yml'))

        expect:
        files*.name == ['gradle.yml']
        flow.path('on').properties()*.key.toSet() == ['push', 'pull_request', 'workflow_dispatch'].toSet()
        flow.path('on').path('push').path('tags-ignore').get(0).asText() == '**'
        flow.path('permissions').size() == 1
        flow.path('permissions').path('contents').asText() == 'read'
        flow.path('jobs').size() == 1
        !flow.path('jobs').path('build').has('permissions')
        !flow.path('jobs').path('build').has('environment')
    }

    void 'CI only checks out code installs Java and runs local verification'() {
        given:
        def file = new File('.github/workflows/gradle.yml')
        def flow = new YAMLMapper().readTree(file)
        def steps = flow.path('jobs').path('build').path('steps').toList()
        def allowed = ['actions/checkout@3d3c42e5aac5ba805825da76410c181273ba90b1',
                       'actions/setup-java@77dd5b2ce225ea11469928ac317850245a74ae78']
        def forbidden = ~/(?i)(docker|sonar|gh\s+release|curl|upload|publish|dependency.submission|(?<!no-)--scan\b)/

        expect:
        steps.findAll { it.has('uses') }.collect { it.path('uses').asText() } == allowed
        steps[0].path('with').has('persist-credentials')
        steps[0].path('with').path('persist-credentials').isBoolean()
        steps[0].path('with').path('persist-credentials').asBoolean() == false
        steps.findAll { it.has('run') }.every { !(it.path('run').asText() =~ forbidden).find() }
        !file.text.contains('secrets.')
        steps.find { it.has('run') && it.path('run').asText().contains('gradlew') }.path('run').asText().contains('--no-scan')
    }
}
