# SPDX-License-Identifier: Apache-2.0
"""Offline final-JAR/CLI/JNI checks. Never connects to Kafka or starts Docker."""
import collections
import hashlib
import json
import os
import pathlib
import re
import shutil
import struct
import subprocess
import tempfile
import zipfile

REPO = pathlib.Path(__file__).resolve().parents[2]
VERSION = '0.4.0'
JAR = REPO / 'build/libs/kafka-gitops-all.jar'
ENV = {k: v for k, v in os.environ.items() if not k.startswith(('KAFKA_', 'CCLOUD_'))}


def executable(name):
    suffix = '.exe' if os.name == 'nt' else ''
    home = os.environ.get('JAVA_HOME')
    candidate = pathlib.Path(home) / 'bin' / (name + suffix) if home else None
    if candidate and candidate.is_file():
        return str(candidate)
    found = shutil.which(name)
    if not found:
        raise RuntimeError('JDK 21 is required: missing ' + name)
    return found


def sha(data):
    return hashlib.sha256(data).hexdigest()


def artifact_checks():
    assert sorted(p.name for p in (REPO / 'build/libs').glob('*.jar')) == ['kafka-gitops-all.jar']
    assert sorted(p.name for p in (REPO / 'build/distributions').iterdir() if p.is_file()) == ['kafka-gitops-' + VERSION + '.zip']
    legal = json.loads((REPO / 'legal/runtime-components.json').read_text(encoding='utf-8'))
    with zipfile.ZipFile(JAR) as jar:
        names = jar.namelist()
        assert not [n for n, count in collections.Counter(names).items() if count > 1]
        manifest = jar.read('META-INF/MANIFEST.MF').decode()
        assert 'Main-Class: com.devshawn.kafka.gitops.MainCommand' in manifest
        assert 'Implementation-Version: ' + VERSION in manifest
        assert 'Multi-Release: true' in manifest and 'Class-Path:' not in manifest
        assert 'version=4.4.0' in jar.read('kafka/kafka-version.properties').decode()
        project_license = (REPO / 'LICENSE').read_bytes().replace(b'\r\n', b'\n')
        assert jar.read('META-INF/licenses/kafka-gitops/LICENSE') == project_license
        assert jar.read('META-INF/licenses/kafka-gitops/CHANGES-FROM-UPSTREAM.md') == (REPO / 'CHANGES-FROM-UPSTREAM.md').read_bytes().replace(b'\r\n', b'\n')
        assert jar.read('META-INF/NOTICE') == (REPO / 'legal/THIRD-PARTY-NOTICES').read_bytes()
        assert jar.read('META-INF/THIRD-PARTY-NOTICES') == (REPO / 'legal/THIRD-PARTY-NOTICES').read_bytes()
        legal_count = 0
        for component in legal['components']:
            for name, expected in component['files'].items():
                assert sha(jar.read('META-INF/licenses/' + component['directory'] + '/' + name)) == expected
                legal_count += 1
        services = [n for n in names if n.startswith('META-INF/services/') and not n.endswith('/')]
        providers = 0
        for name in services:
            for line in jar.read(name).decode().splitlines():
                line = line.split('#', 1)[0].strip()
                if line:
                    assert line.replace('.', '/') + '.class' in names
                    providers += 1
        assert len(services) == 5 and providers == 9
        natives = [n for n in names if re.search(r'\.(so|dll|dylib|jnilib)$', n)]
        assert len(natives) == 53
        classes = [n for n in names if n.endswith('.class')]
        majors = []
        for name in classes:
            mr = re.match(r'META-INF/versions/(\d+)/', name)
            if not mr or int(mr[1]) <= 21:
                majors.append(struct.unpack('>H', jar.read(name)[6:8])[0])
        assert max(majors) == 65
        assert not any(n.endswith('module-info.class') for n in names)
        assert not any(re.search(r'META-INF/[^/]+\.(SF|RSA|DSA)$', n) for n in names)
        assert not any('QaProcessProbe' in n or 'KafkaLabProbe' in n or 'Kafka3xLabProbe' in n for n in names)
    launcher = (REPO / 'build/output/kafka-gitops').read_bytes()
    assert launcher.endswith(JAR.read_bytes())
    release = REPO / ('build/distributions/kafka-gitops-' + VERSION + '.zip')
    with zipfile.ZipFile(release) as archive:
        assert archive.read('kafka-gitops') == launcher
        assert (archive.getinfo('kafka-gitops').external_attr >> 16) & 0o777 == 0o755
        assert archive.read('LICENSE') == project_license
        for c in legal['components']:
            for name, expected in c['files'].items():
                assert sha(archive.read('legal/META-INF/licenses/' + c['directory'] + '/' + name)) == expected
    return dict(entries=len(names), classes=len(classes), effectiveJavaMajor=max(majors),
                runtimeComponents=len(legal['components']), legalFiles=legal_count,
                serviceDescriptors=len(services), serviceProviders=providers, nativeResources=len(natives),
                jarSha256=sha(JAR.read_bytes()), launcherSha256=sha(launcher), releaseZipSha256=sha(release.read_bytes()))


def main():
    artifact = artifact_checks()
    checks = []
    java = executable('java')
    with tempfile.TemporaryDirectory(prefix='kafka-gitops-release-check-') as temp:
        root = pathlib.Path(temp)

        def cli(name, args, expected, contains=None):
            result = subprocess.run([java, '-jar', str(JAR), *map(str, args)], env=ENV,
                                    cwd=REPO, capture_output=True, text=True, encoding='utf-8',
                                    errors='replace', timeout=15)
            output = result.stdout + result.stderr
            assert result.returncode == expected, (name, result.returncode)
            assert contains is None or contains in output, name
            if expected:
                assert 'Apply complete!' not in output and '[SUCCESS]' not in output, name
            checks.append(name)

        cli('help', ['--help'], 0, 'Usage:')
        cli('version', ['--version'], 0, VERSION)
        for cmd in ['plan', 'apply', 'validate', 'account']:
            cli('help-' + cmd, [cmd, '--help'], 0, 'Usage:')
        cli('invalid-option', ['--release-unknown-option'], 2)
        cli('missing-command', [], 2)
        state = root / 'state.yaml'
        state.write_text('topics:\n  synthetic:\n    partitions: 1\n    replication: 1\n', encoding='utf-8')
        cli('validate', ['-f', state, 'validate'], 0, '[VALID]')
        for rf in [0, -1, 32768]:
            bad = root / ('rf-' + str(rf) + '.yaml')
            bad.write_text('topics:\n  synthetic:\n    partitions: 1\n    replication: ' + str(rf) + '\n', encoding='utf-8')
            cli('invalid-rf-' + str(rf), ['-f', bad, 'validate'], 2)
        for value, name in [('null\n', 'null-root'), ('~\n', 'tilde-root')]:
            bad = root / (name + '.yaml'); bad.write_text(value, encoding='utf-8')
            cli(name, ['-f', bad, 'validate'], 2)
        cli('no-bootstrap', ['--skip-acls', '-f', state, 'plan'], 2, 'BOOTSTRAP_MISSING')
        cli('no-changes', ['--skip-acls', '--skip-topics', '-f', state, 'plan'], 0, 'NO_CHANGES')
        cli('missing-config', ['-c', root / 'missing.properties', '-f', state, 'validate'], 2, 'COMMAND_CONFIG_UNREADABLE')
        bad = root / 'bad.properties'; bad.write_text('sasl.jaas.config=fixture-value\\uZZZZ\n', encoding='utf-8')
        cli('malformed-config', ['-c', bad, '-f', state, 'validate'], 2, 'COMMAND_CONFIG_UNREADABLE')
        for cmd in ['plan', 'apply']:
            cli('rf-conflict-' + cmd, [cmd, '--manage-replication', '--no-manage-replication'], 2, 'RF_OPTION_CONFLICT')
        for plan in sorted((REPO / 'src/test/resources/plans').glob('*-plan.json')):
            if plan.name != 'invalid-plan.json':
                cli('legacy-' + plan.stem, ['--skip-acls', '--skip-topics', 'apply', '-p', plan], 0)
        cli('invalid-plan', ['--skip-acls', '--skip-topics', 'apply', '-p', REPO / 'src/test/resources/plans/invalid-plan.json'], 2)
        rf_plan = {'topicPlans': [{'name': 'synthetic-topic', 'action': 'UPDATE', 'replicationPlan': {
            'schemaVersion': 1, 'action': 'UPDATE', 'clusterId': 'AQIDBAUGBwgJCgsMDQ4PEA',
            'topicId': 'AgICAgICAgICAgICAgICAg', 'partitionCount': 1, 'targetReplicationFactor': 2,
            'algorithm': 'preserve-rack-load-v1', 'rackPolicy': 'RACK_AWARE', 'expectedMinInSyncReplicas': 1,
            'brokerSnapshot': [{'id': 0, 'rack': 'a'}, {'id': 1, 'rack': 'b'}],
            'assignments': [{'partition': 0, 'previousReplicas': [0], 'targetReplicas': [0, 1]}]}}], 'aclPlans': []}
        rf = root / 'rf.json'; rf.write_text(json.dumps(rf_plan), encoding='utf-8')
        cli('rf-disabled-json', ['--skip-acls', '--skip-topics', 'apply', '-p', rf, '--no-manage-replication'], 2, 'MANAGE_REPLICATION_REQUIRED')
        compile_result = subprocess.run([executable('javac'), '-cp', str(JAR), '-d', str(root),
                                         str(REPO / 'tools/release/CodecSmoke.java')], env=ENV, capture_output=True, timeout=40)
        assert compile_result.returncode == 0, 'Codec smoke compilation failed'
        codec = subprocess.run([java, '-cp', str(root) + os.pathsep + str(JAR), 'CodecSmoke', 'all'],
                               env=ENV, capture_output=True, text=True, timeout=40)
        assert codec.returncode == 0 and 'array=60 direct=60 KafkaRecordBatches=36' in codec.stdout
    print(json.dumps(dict(artifact=artifact, cliChecks=len(checks), codecRoundTrips=156,
                         kafkaConnections=0, dockerOperations=0), indent=2))


if __name__ == '__main__':
    main()
