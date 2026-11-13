<!-- SPDX-License-Identifier: Apache-2.0 -->
> Historical stage evidence: the dates, versions, hashes and test outcomes below describe that stage and artifact. Personal machine paths were anonymized for publication. See [0.4.0 preparation](../release/v0.4.0-release-preparation.md) for the current release status.

# Kafka 4.4.0: испытания на локальном Docker-стенде

Дата: **12.11.2026** (Europe/Moscow). Ветка `feature/ads-gitops`.
Исходный и итоговый HEAD: `e82b20f46491e72b47416dcc9c9e0d9ecab57392`.
Run ID: `20261112-180451-668a0e`, prefix: `gitops-it-20261112-180451-668a0e-`.

**Базовые операции на реальном Kafka 4.4.0 подтверждены. Реальное изменение RF
не подтверждено: включённый ELR блокирует текущий алгоритм.** Проверки не отключались,
ELR/features/Compose не менялись. Все три контейнера оставлены работающими,
четыре созданных test topics и все логи сохранены. Коммит не создавался.

## Итог и границы подсчёта

Протокол содержит **38 cases: 30 PASS, 0 FAIL, 4 BLOCKED, 4 NOT_TESTED**.
Фактически выполнены **32 cases**: 24 с реальными Kafka Admin/CLI обращениями,
7 локальных CLI/config/validation cases и 1 проверка недоступного loopback port.
RF02/RF03 не выполнялись из-за недостигнутых prerequisites; ещё четыре RF/fault
проверки отмечены NOT_TESTED. Будущие защищённые/аварийные испытания вне этих 38.
Отрицательный case считается PASS только по проверенному ожидаемому отказу;
если нужную проверку опередил другой blocker, это BLOCKED, как N03.

| Направление | Результат |
|---|---|
| Подключение, 3 brokers/racks, configs/log dirs, quorum | Подтверждены настоящими read APIs и независимыми Kafka CLI |
| Создание topics | PASS, только уникальный test namespace |
| Config ADD / UPDATE / REMOVE / previousValue | PASS, effective values/source перечитаны |
| Partitions 2 → 4 / запрет 4 → 2 | PASS / PASS ожидаемого отказа |
| Повторный plan | NO_CHANGES после создания |
| RF 1 → 2 | BLOCKED: ELR_UNSUPPORTED; до executor не дошло |
| RF 2 → 3 / 3 → 2 | BLOCKED: необходимые исходные RF не достигнуты |
| No-manage RF opt-out | PASS: drift warning, exit 0, RF operation отсутствует |
| Invalid config, stale partition JSON, invalid state/config, local timeout | Проверенные отказы, без нежелательных изменений |
| Regression | **972 Java tests** (965 + 7), errors/failures/skipped = 0; **6 offline Python guard tests** |
| CLI/JNI после испытаний | **56 local JAR smoke checks**, 156 codec/JNI round trips — PASS |
| Production Java changes | **Нет**; dependency/Java/ACL/RF policy changes отсутствуют |

## Подтверждение изоляции и готовности

| Проверка | Фактическое значение |
|---|---|
| Docker context / endpoint | `desktop-linux`, Docker Desktop Linux named pipe |
| Desktop / Engine / Compose | **4.99.0 (245822) / 29.9.3 / 5.7.0** |
| Compose project / source | `kafka-gitops-lab`, `../kafka-lab\compose.yaml` |
| Image | `apache/kafka:4.4.0`, одинаковый image ID у всех трёх контейнеров |
| Container `Image` field | `sha256:833b867a9585a141da455dc2b57431c65cbccb27f11c91f694444cca9f777e0f` у всех трёх |
| Compose image label | `sha256:76af3d0116abbb33b4cd964af410d4921c33d1f663e7a95d5453a9c35a84869b`; это отдельное поле, не container Image ID |
| Broker binaries | Все три `kafka-topics.sh --version` вернули **4.4.0** |
| Cluster ID из Compose и реального Admin | **`4L6g3nShT-eMCtK--X86sw`** |
| Nodes/racks | 1/rack-1, 2/rack-2, 3/rack-3; unfenced |
| Windows bindings | Только `127.0.0.1:19092`, `:19093`, `:19094` → соответствующий HOST listener 9092 |
| Advertised HOST addresses | `localhost:19092/19093/19094`; localhost разрешается только в loopback |
| Docker network | Только `kafka-gitops-lab_kafka-lab` у test containers |
| Mode/security | KRaft, combined broker/controller; PLAINTEXT; без SASL/TLS |
| Quorum | Voters 1/2/3, leader 3, все три log end offsets совпали с high watermark на checkpoint |
| Initial topics / active reassignments | Пусто / пусто |
| Health | DescribeConfigs/DescribeLogDirs действительно ответили для каждого broker, log directories healthy |
| Features | metadata.version=33; **eligible.leader.replicas.version=1** |
| Final topics / active reassignments | Четыре явно перечисленных test topics / пусто |

Проверены не только cluster ID и Docker Up: context, project/service labels,
Compose working/config path, actual container IDs, image, node IDs, racks,
listeners/security map/quorum voters, network и реальные port bindings.
Gate повторяется **перед каждым apply**, включая ожидаемо неуспешные write-тесты.
Он затем получает свежий cluster ID через Admin и проверяет brokers/racks/quorum/
storage. План отдельно проверяется на scope, пустые ACL, отсутствие topic REMOVE
и недопустимых config keys. `--skip-acls` использовался всегда; `--no-delete`
не снимался ни в одном фактически выполненном apply.

Runtime features и UUID не подменялись fake READY. Рабочий ADS/внешние endpoints
не использовались. Kafka environment очищался только в child process; явный
properties-файл отдельно проверен на приоритет над конфликтующим безопасным env.
Compose/volumes не редактировались. Docker down/stop/restart и broker faults не выполнялись.

## Каталог evidence и конфигурация

Корень: `../kafka-lab\gitops-integration`.
Run: `../kafka-lab\gitops-integration\run-20261112-180451-668a0e`.

В каталоге сохранены YAML, JSON plans, `<id>.log`, `<id>-command.json` с точным argv,
exit code и duration, `*-metadata.json`, `*-docker-proof.json`, Admin audit JSON,
`manifest.json`, `summary.json`, container version/ps и неисполненные cleanup-команды.
[Manifest](../kafka-lab/gitops-integration/run-20261112-180451-668a0e/manifest.json)
содержит полный protocol, expected/actual и вложенные before/after snapshots.
Краткие состояния ниже дополняются этими полными metadata, а не заменяют их.

`client.properties`:

```properties
bootstrap.servers=127.0.0.1:19092,127.0.0.1:19093,127.0.0.1:19094
client.id=kafka-gitops-integration
security.protocol=PLAINTEXT
request.timeout.ms=10000
default.api.timeout.ms=30000
```

Ни пароли, ни реальные credentials не использовались. Логи с verbose config
содержат только разрешённые diagnostic fields; sensitive payloads не создавались.
Негативный localhost timeout использует отдельный файл с `127.0.0.1:19999`, API
timeout 1000 ms / request timeout 500 ms и **только plan**; этот файл не допускается
никаким write gate.

## Воспроизводимые команды

В таблицах `$repo`, `$root`, `$run`, `$cfg`, `$jar` обозначают эти точные paths:

```powershell
$repo = '.'
$root = '../kafka-lab\gitops-integration'
$run = "$root\run-20261112-180451-668a0e"
$cfg = "$root\client.properties"
$jar = "$repo\build\libs\kafka-gitops-all.jar"
```

Сокращения команд в protocol:

```powershell
# P(id): реальный JAR plan, без Kafka writes
java -jar $jar --command-config $cfg --file "$run\<id>.yaml" --skip-acls --no-delete plan --output "$run\<id>-plan.json"
# A(id): перед запуском runner выполняет полный Docker/cluster/scope gate
java -jar $jar --command-config $cfg --file "$run\<id>.yaml" --skip-acls --no-delete apply --plan "$run\<id>-plan.json"
# D(topic): независимая проверка из официального контейнера
docker --context desktop-linux exec kafka-gitops-lab-kafka-1-1 /opt/kafka/bin/kafka-topics.sh --bootstrap-server kafka-1:19092 --describe --topic <полное-test-name>
```

Точные filenames/argv, включая отличающиеся negative/guarded команды, находятся
в `<case>-command.json` или `<case>-plan-command.json`. Сокращения не являются
дополнительными CLI-флагами приложения. Все before/after файлы используют полный
topic UUID, ordered replicas, leader/ISR и effective config values/source.

## Read-only protocol

| ID | Команда | Исходное → ожидаемое | Фактический результат / metadata | Status / exit |
|---|---|---|---|---|
| R01 | `docker context show`, `docker compose ... ps/config`, probe snapshot | Пустой test namespace → подтвердить весь safety gate | 3 nodes/racks, matched cluster ID, healthy log dirs/quorum; topics/reassignments пусты (`R01-metadata.json`) | PASS / 0 |
| R02 | `java -jar $jar plan --help`; validate `R02.yaml` с `$cfg` | Новый topic 2 partitions/RF1 → local help/valid state | Help и validate успешны; никаких Kafka write API | PASS / 0 |
| R03 | P(`R02.yaml`), output `R03.json` | Нет test topic → один ADD, без DELETE | Реальный JAR показывает ABSENT→2/1; `R03-after-metadata.json` остаётся пустым | PASS / 0 |
| R04 | Guarded plan для `R02.yaml`, output `R04.json` | Тот же ADD → только read APIs | Все reads делегированы настоящему Admin; `R04-api-audit.json`: writeAttempts=0, без изменений | PASS / 0 |
| R05 | `java -jar $jar -v -c $cfg -f R02.yaml --skip-acls --no-delete plan -o R05.json` с конфликтующим env | Env содержит один разрешённый loopback endpoint/другой client ID → файл должен выиграть | Log показывает все три file endpoints и `kafka-gitops-integration`; до W01 Kafka остаётся пустой | PASS / 0 |
| R06 | `kafka-topics.sh --version` в каждом container; `kafka-metadata-quorum.sh ... describe --status` | Docker Up → независимая проверка binaries/quorum | Три версии 4.4.0, healthy 3-voter quorum; metadata не менялась | PASS / 0 |
| R07 | Guarded `plan --no-manage-replication` для существующего RF-topic, desired RF2 | Actual RF1 → явный opt-out без RF operation | RF_MANAGEMENT_DISABLED, exit0, пустой changes JSON, audit writeAttempts=0; before/after topics совпали | PASS / 0 |
| R08 | Guarded plan `W06.yaml`, output `R08-plan.json` | Финальные config values → нет изменений | NO_CHANGES, zero write attempts; full metadata до/после совпала | PASS / 0 |
| R09 | Guarded plan `W07.yaml`, output `R09-plan.json` | Main уже Count4 → нет изменений | NO_CHANGES, zero write attempts; Count4/UUID/replicas/ISR не менялись | PASS / 0 |

`KafkaLabProbe` — отдельный lab harness **вне Gradle source sets/JAR**. Для guarded
plan и blocked apply он использует тот же CLI/StateManager/real KafkaService и
production application binding, но supplied Admin обёрнут proxy: разрешены только
описанные read methods/close; всякий mutating/unknown method фиксируется и запрещается
**до** вызова настоящего Admin. Это live-read аудит, не fake metadata и не подстановка
безопасности. Обычный JAR plan дополнительно проверен независимыми snapshots.

## Topic/config/partitions protocol

Короткие имена `main/configs/rf/stale` ниже всегда обозначают соответствующее полное
имя из таблицы сохранённых ресурсов; обычные/чужие topics не использовались.

| ID | Команда | Before → expected | After / комментарий | Status / exit |
|---|---|---|---|---|
| W01 | P/A(`W01`, main), D(main) | ABSENT → partitions2/RF1/max.message.bytes1048588 | Topic создан, UUID сохранён, 2 healthy singleton ISR; before/after `W01-*metadata.json` | PASS / 0 |
| W02 | Повторный plan `W01.yaml`, output `W02.json` | W01 state → нет изменений | NO_CHANGES; последующий W03-before snapshot подтверждает неизменный main | PASS / 0 |
| W03 | P/A(`W03`, configs), D(configs) | ABSENT → отдельный config topic 2/RF1/max1048588 | Создан только configs; main не меняется | PASS / 0 |
| W04 | P/A(`W04`, configs), D(configs) | retention.ms inherited604800000 → local3600000 | Config ADD, effective3600000/DYNAMIC_TOPIC_CONFIG | PASS / 0 |
| W05 | P/A(`W05`, configs), D(configs) | max1048588 → 2097152 | UPDATE JSON previousValue=1048588; после apply2097152, retention3600000 | PASS / 0 |
| W06 | P/A(`W06`, configs), D(configs) | local retention3600000 → снять override | Config REMOVE, effective604800000/DEFAULT_CONFIG; max2097152; topic не удаляется, no-delete оставлен | PASS / 0 |
| W07 | P/A(`W07`, main), D(main) | partitions2 → 4, RF1 | Count4, новые partitions перечитаны; ordered replicas/ISR/leader здоровы | PASS / 0 |
| W08 | P/A(`W08`, rf), D(rf) | ABSENT → отдельный RF fixture 2/RF1 | Создан RF-topic; это CreateTopics, **не** RF reassignment | PASS / 0 |
| W09 | P/A(`W09`, stale), D(stale) | ABSENT → stale fixture 2/RF1 | Создан отдельный fixture, без изменений прочих topics | PASS / 0 |
| W10 | Guarded `apply` из `W07.yaml` без saved JSON | Main уже соответствует partitions4/RF1/configs → no-op | Exit0, строка `There are no necessary changes`, writeAttempts=0; before/after full metadata совпала | PASS / 0 |
| N04-fresh-increase | P/A(`N04-fresh-increase`, stale), D(stale) | stale2 → 4 при сохранённом старом плане2→3 | Разрешённый контрольный write, Count4; готовит настоящий stale-plan отказ | PASS / 0 |

Для каждого A вызваны fresh Docker/cluster checks, проверен **полный** JSON scope,
отсутствие topic REMOVE/ACL operations, unique prefix и известные test names.
Последующие reads и Kafka CLI подтверждали фактическое состояние. ACK не был
единственным доказательством; counts/config sources/ISR перечитывались.

## RF: реальные результаты и blockers

| ID | Команда | Before → expected | Фактический результат / metadata | Status / exit |
|---|---|---|---|---|
| RF01 | Guarded plan `RF01.yaml` (RF2), затем guarded `apply` **из YAML без JSON** | rf2 partitions, actual RF1 (`[2]`, `[3]`) → RF2 при READY | ELR_UNSUPPORTED: actual ENABLED, required DISABLED или положительно NOT_APPLICABLE. Нет executable JSON; guarded apply заново прочитал состояние и остановился. UUID/assignments/ISR/leader до/после совпали; writeAttempts=0 | BLOCKED / 2 |
| RF02 | Не выполнялась | Нужен достигнутый RF2 → 3 | RF остался1 после RF01. Проверка2→3 не имитировалась другой операцией | BLOCKED / N/A |
| RF03 | Не выполнялась | Нужен достигнутый RF3 → 2 с fresh allow | RF3 не достигнут; decrease с разрешением не испытан | BLOCKED / N/A |

Точная первичная блокировка — `PlanManager.planReportedReplications`, основанная
на реальном `eligible.leader.replicas.version=1`. Apply-from-YAML остановился на
свежем planning report **до RF application preflight/executor**. Нельзя утверждать,
что live executor/preflight успешно отработал, что RPC был принят либо что transfer
дошёл до COMPLETED. Исполняемый RF JSON не создан, mutating RF RPC не отправлялся.

Все три brokers имеют rack, verified fencing/storage, явный пустой
`cordoned.log.dirs` и положительный usableBytes. Placement admission остаётся
обоснованным warning, а не причиной данного отказа. Блокирует именно ELR.
Режим не отключался, policy не менялась, endpoints/identity не подменялись.

### Проверка allowReplicationFactorChange

Дефект «не выставлен обязательный true» **не подтвердился**. В фактическом Kafka
Client 4.4.0 constructor `AlterPartitionReassignmentsOptions` задаёт default **true**;
executor создаёт эти options и меняет только timeout. AdminClient переносит это
значение в request. Это подтверждено bytecode установленного artifact и семью
новыми offline tests, без добавления зависимостей:

1. Default options=true.
2. True option строится и сериализуется для legacy request v0.
3. False на v0 даёт UnsupportedVersionException и не теряется молча.
4. False представим на v1.
5. Реальный executor над fake Admin передаёт true после safety checks для 1→2,
   2→3 и 3→2; permissions/assignments не подменяются production-кодом.

Первичные API references: [Kafka API option](https://kafka.apache.org/43/javadoc/org/apache/kafka/clients/admin/AlterPartitionReassignmentsOptions.html),
[KafkaAdminClient 4.4.0 source](https://raw.githubusercontent.com/apache/kafka/4.4.0/clients/src/main/java/org/apache/kafka/clients/admin/KafkaAdminClient.java).
Совместимая сериализация не равна проверке на настоящей Kafka 3.2/3.7.1.
Локальное CLI decrease permission остаётся независимым от transport option;
его проверки не ослаблялись. Live InvalidReplicationFactorException здесь не
возникал, поскольку RF RPC не был достигнут. Принудительно добавлять option/bypass
или менять policy ради прохождения теста оснований нет.

## Негативные проверки

| ID | Команда / вход | Before → expected | Actual / metadata / ограничение | Status / exit |
|---|---|---|---|---|
| N01 | P/A(`N01`, configs), max.message.bytes=-1 | max2097152 → Kafka должен отказать | Exit2/KAFKA_ERROR, safe exception category; `N01-before/after-metadata.json` совпали, max остался2097152 | PASS / 2 |
| N02 | P(`N02`, main), partitions4→2 | Count4 → отказ decrease | TOPIC_CHANGE_UNSAFE с 4→2 в reason; JSON не создан, N02-after Count4 | PASS / 2 |
| N03 | P(`N03`, rf), RF4 при трёх brokers | Нужен изолированный недостаток brokers (3<4) | ELR_UNSUPPORTED опережает placement calculation; JSON/writes отсутствуют, но INSUFFICIENT_BROKERS не достигнут | BLOCKED / 2 |
| N04 | Сохранить plan stale2→3; после N04-fresh-increase выполнить старый `apply -p N04-stale-plan.json` | Actual4 при previous2/target3 → отказ stale | Exit2/KAFKA_ERROR, stale count actual4/expected2 или3; before/after совпали, Count4 | PASS / 2 |
| N05 | `plan --manage-replication --no-manage-replication` | Противоположные flags → local refusal | RF_OPTION_CONFLICT, без конструирования Kafka session | PASS / 2 |
| N06 | `-c <missing.properties> validate` | Отсутствующий файл → отказ | COMMAND_CONFIG_UNREADABLE, без fallback/подключения | PASS / 2 |
| N07 | Validate невалидного YAML `topics: [ invalid` | Syntax error → безопасный отказ | Exit2, без stack trace/содержимого secret configs; Kafka не вызывается | PASS / 2 |
| N08 | Validate generated test-prefixed name, partitions0 | Count0 → отказ validation | Exit2, Kafka не вызывается | PASS / 2 |
| N09 | Validate generated test-prefixed name, RF-1 | RF-1 → отказ validation | Exit2, Kafka не вызывается | PASS / 2 |
| N10 | Plan с properties без bootstrap | Нет bootstrap → ранний отказ | BOOTSTRAP_MISSING; no implicit localhost; ни одного подключения к внешнему host | PASS / 2 |
| N11 | Plan с `127.0.0.1:19999`, API timeout1000/request500 | Локальный недоступный port → bounded failure | TIMEOUT, exit2; **1.39 s** вместе со стартом JVM, Kafka metadata не менялась | PASS / 2 |
| N12 | Не выполнялась | Decrease без разрешения | Невозможно изолировать при текущем RF1 и ELR blocker; unit coverage остаётся | NOT_TESTED / N/A |
| N13 | Не выполнялась | Decrease с no-delete | Live decrease не достигнут; ограничение не снималось | NOT_TESTED / N/A |
| N14 | Не выполнялась | Unknown eligibility/ISR loss | Искусственные broker/storage/quorum faults запрещены в этом задании | NOT_TESTED / N/A |
| N15 | Не выполнялась | ACK/RUNNING/COMPLETED настоящего transfer | Transfer не начался; нельзя перенести успешные fake executor tests на live поведение | NOT_TESTED / N/A |

У N01 разрешён scoped config RPC с невалидным значением; это не утверждение
«запроса записи вообще не было»: брокер отверг его, нежелательного изменения нет.
При N04 barrier срабатывает до write. N05–N10 — локальные или ранние input/network
checks; before/after весь local-negative блок также ограничен metadata checkpoints.
No false SUCCESS проверен для сохранённых nonzero CLI logs. N03 намеренно
не посчитан PASS по одной лишь невозможности применить RF4.

## Сохранённые test topics

Полный prefix: `gitops-it-20261112-180451-668a0e-`. Все topics оставлены.

| Полное имя | UUID | Partitions / RF | Ordered replicas по partition; leader и ISR |
|---|---|---|---|
| gitops-it-20261112-180451-668a0e-main | NIaUAm4LRqGtWW2uCJ9xKg | 4 / 1 | p0[1], p1[2], p2[1], p3[2]; каждый leader/ISR равен singleton replica |
| gitops-it-20261112-180451-668a0e-configs | 2rTZhoL7QiOiPcM_XBTX1g | 2 / 1 | p0[2], p1[3]; leader/ISR2 и3 |
| gitops-it-20261112-180451-668a0e-rf | ALM3ArjgSq6Yvz_JeZBNtA | 2 / 1 | p0[2], p1[3]; RF не увеличивался |
| gitops-it-20261112-180451-668a0e-stale | SQPVv4w8TsyvSQJgK2wSew | 4 / 1 | p0[1], p1[2], p2[3], p3[1]; полные singleton ISR |

У configs: max.message.bytes=2097152, retention.ms=604800000/DEFAULT_CONFIG.
У остальных max.message.bytes=1048588. Effective min.insync.replicas=1.
Active reassignments отсутствуют. Полные final snapshots: `FINAL-metadata.json`,
последующие `RF01-after-metadata.json`, `R07-after-metadata.json`, `VERIFY-metadata.json`
и `W10-after-metadata.json` после повторного apply.
`verify` дополнительно сопоставляет UUID с сохранённым reference.

Ни чужих topics, ни ACL не создавали/изменяли/удаляли. Topics глобально не очищались.
Два разных prefix-run набора при будущих запусках сохраняются отдельно.

## Runner, исправления harness и регрессия

Добавлены только:

- `tools/integration/kafka44_lab.py`: fixed Docker/project/cluster/endpoint gate,
  отдельный read-only режим, явный `--allow-writes` для write phases, scoped JSON
  checks и logs/results. Запуск с Python `-O` запрещён, чтобы не удалить assertions.
- `tools/integration/KafkaLabProbe.java`: только read APIs и guarded CLI harness;
  отдельная компиляция в lab, не входит в application JAR/Gradle test sources.
- `tools/integration/test_kafka44_lab.py`: 6 offline отрицательных guard tests
  (foreign topic, DELETE, ACL, unknown config, wrong cluster и допустимый scope).
- `Kafka44RfOptionsSpec.groovy`: 7 безопасных regression tests реального client API.
- Этот отчёт.

Production Java, ReplicationSafetyPolicy, executor, schema, ACL semantics,
dependencies, Java 21 и Compose **не изменялись**. Подтверждённых новых дефектов
приложения в выполненном scope не обнаружено; ELR отказ — действующая политика.
В harness исправлены компиляционная скобка и ожидания формата bootstrap/no-op log;
read-only фаза повторена до любых writes. История tooling assertion сохранена
отдельно в `runnerEvents` с resolved=true и не выдана за application test failure.
Первый вариант protocol test затрагивал optional Jackson 2 signature через Groovy
reflection; он заменён непосредственным protocol writer, без добавления runtime libs.

Выполнено после live испытаний:

```powershell
.\gradlew.bat --gradle-user-home "$env:USERPROFILE\.gradle" --offline --no-daemon --no-scan --console plain --warning-mode all --rerun-tasks classes testClasses test shadowJar buildExecutableJar buildRelease
python tools/integration/test_kafka44_lab.py
python .gradle/kafka44-integration/verify.py
```

**Финальный BUILD SUCCESSFUL (29 s), 8 executed tasks, 972 tests / 42 suites,
zero failures/errors/skips.** После всех live cases принудительно повторены
компиляция и упаковка; предыдущий incremental прогон также прошёл за 14 s.
Все прежние 965 tests сохранены. Legacy IntegrationSpec не запускались и по-прежнему
исключены Gradle. Дополнительно 56 CLI smoke checks, старые JSON fixtures, manifests/
services/native inventory и 156 Windows JNI/codec round trips прошли.

Итоговый один `build/libs/kafka-gitops-all.jar`: **25 834 708 bytes (24.64 MiB)**.
SHA-256: `bf30233eee3aa5cae5c0f7ce8a449630530a8ab445632fb2762743881ae933d5`.
Hash совпадает с испытанным JAR и артефактом предыдущей технической приёмки.
Runtime: 13 dependency JARs, Kafka Client4.4.0, Jackson3.3.0; 7286 classes,
53 native resources, 5 merged service descriptors, duplicate entries/classes=0.
Main-Class=`com.devshawn.kafka.gitops.MainCommand`, Multi-Release=true,
application bytecode65 / Java21. Kafka3.x, test harness/fixtures и внешних
client.properties в JAR нет. Native/MR/manifest выводы ограничены этой Windows/JDK средой.

## Повторный запуск и cleanup — только вручную

Для нового run при уже работающем том же кластере сначала подготовить фиксированный
client.properties и compile probe; Docker runner сам **не** запускает:

```powershell
javac -cp $jar -d "$root\probe-classes" "$repo\tools\integration\KafkaLabProbe.java"
python tools/integration/kafka44_lab.py --phase readonly
# взять новый RUN_DIR; prefix генерируется заново
python tools/integration/kafka44_lab.py --phase basic --run-dir <NEW_RUN_DIR> --allow-writes
python tools/integration/kafka44_lab.py --phase rf --run-dir <NEW_RUN_DIR> --allow-writes
python tools/integration/kafka44_lab.py --phase negative --run-dir <NEW_RUN_DIR> --allow-writes
python tools/integration/kafka44_lab.py --phase recheck --run-dir <NEW_RUN_DIR> --allow-writes
# Только read-only повторная проверка текущего сохранённого run:
python tools/integration/kafka44_lab.py --phase verify --run-dir $run
```

Режим rf в этом runner фиксирует ожидаемую блокировку **данного ELR-enabled
стенда**. Он не является универсальным positive-RF runner для другого feature mode.
Нет автоматического cleanup. Отдельные scoped команды для четырёх точных имён
сохранены в [cleanup-NOT-EXECUTED.txt](../kafka-lab/gitops-integration/run-20261112-180451-668a0e/cleanup-NOT-EXECUTED.txt).
Они **не выполнялись**. Перед каждой отдельной разрешённой delete-командой надо
выполнить `--phase verify`, проверить UUID/name/identity и получить разрешение
владельца на удаление именно этого test topic. Никаких wildcard/global delete,
volume removal или Docker down/stop в программе нет.

## Ограничения и следующий шаг

1. Главный RF acceptance остаётся открытым. Нужна отдельная задача: либо спроектировать
   корректную поддержку ELR в algorithm/preflight/executor с adversarial tests,
   либо отдельно разрешённый стенд/feature mode без ELR для испытания существующего
   алгоритма. Отключать ELR/изменять политику текущего стенда без нового решения нельзя.
2. После этого выполнить настоящие 1→2, 2→3, 3→2, permission/no-delete negatives,
   observe ACK→RUNNING→COMPLETED, exact ordered target, UUID/cluster identity,
   full ISR/leader/no active; существующие unit tests этого live evidence не заменяют.
3. Kafka3.2/3.7.1 не запускались. Нужны отдельно разрешённые isolated runs того же JAR:
   protocol negotiation/v0, mode/feature applicability proofs, старый DescribeLogDirs,
   identity/racks/min ISR и refusal по неполным данным. Wire compatibility tests
   не означают подтверждённую поддержку реальных старых brokers.
4. SASL/PLAIN, SASL/GSSAPI/Kerberos, SSL/SASL_SSL, authorization denials и production
   ACL enforcement **NOT_TESTED**: текущий PLAINTEXT cluster таких условий не имеет.
   ACL profiles/READ/WRITE/CREATE/DESCRIBE не менялись; нужны отдельные read/write
   principals и защищённый стенд с тестовыми credentials.
5. Остановки broker, потери quorum, fault injection, частичный transfer и recovery
   не выполнялись. Это отдельная последующая программа с явными разрешениями,
   synthetic data, контролируемым timing и обязательным сохранением состояния.
6. Snapshot не атомарный, placement admission/transfer reserve не доказаны общим
   read-only API. Production-ready и нагрузочная/аварийная готовность не заявляются.
   Остаточные build/security risks из [Task3 QA](development-task3-final-qa.md)
   этими PLAINTEXT испытаниями не устраняются.

Финальный `docker compose ps`: **все три контейнера running**, endpoints/проект
не изменились. Ветка/HEAD сохранены; `git status --short`, `git diff --check`,
`git diff --stat` выполнены. Рабочие конфигурации/ADS не использовались,
Git-коммит не создавался, дополнительные версии Kafka/новый стенд не запускались.
