# Kafka 3.x: совместимость одного Kafka GitOps JAR

Дата: 13.11.2026, Europe/Moscow. Ветка `feature/ads-gitops`, HEAD
`91a7c974beb098dbe49d339ed1bfddb282aad399`. Коммит и staging не выполнялись.

Один окончательный JAR с Kafka Client 4.4.0 реально прошёл **RF 1→2, 2→3, 3→2**
на Apache Kafka **3.2.3, 3.7.1 и 4.4.0**. JAR запускался на Windows/Java 21;
отдельных legacy-сборок нет. ELR на существующем Kafka 4.4 оставлен на level1.

SHA-256 реально испытанного и финального `build/libs/kafka-gitops-all.jar` одинаков:
`04c7fb173b15e87355d94f2f4373bf8d2fef5ee79574342a27f9af1a35c29c4e`.
Размер: **25 844 467 bytes**. Изменены два production-компонента после обнаружения
двух несовместимостей наблюдения metadata; затем все три версии проверены заново этим артефактом.

## Основная матрица приёмки

| Брокеры | Режим | Сценариев | PASS | FAIL | BLOCKED | NOT_TESTED |
|---|---|---:|---:|---:|---:|---:|
| 3.2.3 | ZooKeeper 3.6.3 | 43 | 42 | 0 | 0 | 1 |
| 3.7.1 | KRaft, static quorum | 43 | 42 | 0 | 0 | 1 |
| 4.4.0 | KRaft, ELR level1 | 37 | 35 | 0 | 0 | 2 |
| **Итого на финальном JAR** | | **123** | **119** | **0** | **0** | **4** |

NOT_TESTED: авторизационная ошибка в каждом PLAINTEXT-стенде без authorizer — три случая;
промежуточный RUNNING при быстром Kafka 4.4 decrease — один. Сам decrease на 4.4 завершён и проверен.
Ожидаемый отказ отрицательного сценария считается PASS; блокировка положительного RF так не считается.
Числа относятся к именованным сценариям manifest, а не к числу отдельных RPC или CLI-команд.

| Функция | 3.2.3 | 3.7.1 | 4.4.0 |
|---|---|---|---|
| Connect, command-config precedence, identity, brokers/racks | PASS | PASS | PASS |
| Read-only plan / guarded Admin audit | PASS | PASS | PASS |
| Создание topic | PASS | PASS | PASS |
| Config ADD / UPDATE / REMOVE | PASS | PASS | Ранее проверены; в этом RF-прогоне — invalid-config refusal |
| Partitions 2→4 | PASS | PASS | Ранее проверены; в этом RF-прогоне — decrease refusal |
| RF 1→2 / 2→3 / 3→2 | PASS / PASS / PASS | PASS / PASS / PASS | PASS / PASS / PASS |
| Повторные plan / saved-plan apply | NO_CHANGES / zero writes | NO_CHANGES / zero writes | NO_CHANGES / zero writes |
| Сообщения, keys, partitions, offsets | 12/12 | 12/12 | 12/12 |
| RF>brokers, decrease permissions/no-delete, stale JSON | PASS: refusal | PASS: refusal | PASS: refusal |
| Partition decrease / invalid config / unavailable bootstrap | PASS: refusal | PASS: refusal | PASS: refusal |

Прежние результаты 4.4 относятся к [Docker integration](kafka-4.4-docker-integration-results.md)
и [ELR/RF](kafka-4.4-elr-rf-results.md). Эти отчёты и evidence не перезаписывались.

## Изолированные стенды и identity

| Параметр | Kafka 3.2.3 | Kafka 3.7.1 | Kafka 4.4.0 |
|---|---|---|---|
| Каталог | `F:\DEV\GitOps\kafka-lab-323` | `F:\DEV\GitOps\kafka-lab-371` | `F:\DEV\GitOps\kafka-lab` |
| Compose project | `kafka-gitops-323-lab` | `kafka-gitops-371-lab` | `kafka-gitops-lab` |
| Cluster ID | `vqvmhB4iTgaEqrNq-LdfrQ` | `GV57l31pRMa-yIjZQBWyww` | `4L6g3nShT-eMCtK--X86sw` |
| Host ports, только 127.0.0.1 | 23292/23293/23294 | 23792/23793/23794 | 19092/19093/19094 |
| IDs / racks | 1/2/3; rack-1/2/3 | 1/2/3; rack-1/2/3 | 1/2/3; rack-1/2/3 |
| Server Java | Temurin 17.0.21+9 | Temurin 21.0.3+9 из официального image | Существующий image не менялся |
| Features | finalized={}, supported={}, epoch0 | metadata.version=19, supported1..19 | metadata.version=33, ELR=1 |
| Effective min ISR | 1 | 1 | 1 |
| Log directories | healthy; capacity отсутствует | healthy; capacity сообщается | healthy; capacity сообщается |
| ELR assessment | NOT_APPLICABLE: подтверждён ZooKeeper | NOT_APPLICABLE: положительный metadata bound | ENABLED + VERIFIED_EMPTY |

Контекст Docker: `desktop-linux`; Engine29.9.3, Compose5.7.0. Старые стенды имеют разные
bridge-сети `kafka-gitops-323-lab_legacy-host` / `kafka-gitops-371-lab_legacy-host`, masquerading
отключён. ZooKeeper не имеет опубликованного порта. Data volumes имеют собственные Compose labels;
чужие volumes не использованы и другой версией не переинициализировались. Официальный 3.7.1 image
дополнительно создаёт отдельные volumes config/secrets: их точные ID зафиксированы и проверены
на отсутствие совместного использования с другими контейнерами.

Перед каждым apply и seed-batch повторяется gate: context, project/config path, compose hash,
runtime image identity, network, mounts/volumes, bindings, фиксированные properties, cluster ID,
IDs/racks, здоровье directories и отсутствие competing reassignment. Проверяется весь JSON scope.
Для decrease разрешён только один RF UPDATE конкретного нового topic без ACL/config/partition/delete operations.
`topicDetails` проверяется как неизменённый описательный контекст, а не дополнительная write-операция.

Все команды используют `--skip-acls`, отдельные properties/YAML, очищенное окружение KAFKA_*/CCLOUD_*.
Write-фазы требуют `--allow-writes`, decrease — `--allow-replication-decrease` без `--no-delete`.
Production/ADS endpoints не использовались. Проверки Kafka 4.4 в существующих runners не ослаблялись.
Compose/features/ELR этого кластера не менялись.

### Происхождение образов и архива

Kafka 3.7.1 — официальный `apache/kafka:3.7.1`, наличие тега проверено Docker manifest,
версия прочитана Kafka CLI у каждого брокера. Image digest:
`sha256:ed74d7d115968d5e8b00ba6822ac6a384cbaaf54ca38991828647000d7089b68`.
Репозиторий указан в [официальной Docker-документации Kafka](https://kafka.apache.org/37/getting-started/docker/).

Kafka 3.2.3 упакован локально в `kafka-gitops-lab/kafka:3.2.3-verified` из
[официального архивного дистрибутива](https://archive.apache.org/dist/kafka/3.2.3/)
`kafka_2.13-3.2.3.tgz`, **104 083 532 bytes**. SHA-512 совпал с Apache sidecar:

```text
4a57d69f8d3b5158ddca9c64ac366c2975ca55f4c2ef02cd3b122e127016ad1a5aa1de92e7e2f392fdc3b57aabe75d4631373d77a3a5958c28d911f4034a136f
```

Detached PGP signature: GOODSIG/VALIDSIG, exit0, Tom Bentley, fingerprint
`6E85804BB51E348372973AE607D24FE8E0B51CAC`. Использован официальный Apache KEYS и отдельный
workspace keyring; пользовательские ключи не менялись. Проверена криптографическая подпись;
личная web-of-trust сертификация владельца ключа не устанавливалась.

База — [Docker Official Image Eclipse Temurin](https://hub.docker.com/_/eclipse-temurin),
`17-jre-jammy`, pinned digest
`sha256:aa6381384f8db99762fa7d0d3c2b5de58874e5f12c96df4c13a1455376ae0ed1`.
Java 17 входит в [поддерживаемые Kafka 3.2 JVM](https://kafka.apache.org/32/operations/java-version/).
SHA-512 повторно проверен внутри Docker build. Итоговый local image ID:
`sha256:3b9d4b19a761688f267f5694a6f4c28e32a87b4f10052bd68ce50f1e4ae45359`.
Cluster ID 3.2.3 независимо совпал в Admin response и meta.properties всех трёх brokers.
ZooKeeper API не добавлялся в GitOps; приложение управляет Kafka только через Admin API.

При подготовке обнаружены и исправлены только особенности нового lab packaging:
internal Docker network не публиковала host ports — создан отдельный bridge без masquerading;
архивные scripts пытались писать JVM logs в root-owned /opt/kafka — LOG_DIR перенесён в /tmp;
Docker Desktop возвращал Linux mirror Windows bind path — gate теперь проверяет точные разрешённые
представления пути и read-only destination. Первые отказы gates/startup сохранены; test writes
до подтверждения identity не выполнялись. Непривилегированный UID1000 сохранён.

## Реальные API и metadata fallback

Таблица показывает диапазоны, объявленные брокерами, по Kafka CLI API-versions evidence.
Для legacy DescribeTopics реально возвращены topic UUID, ordered replicas, leader/ISR и nullable ELR-поля.
Используется Metadata fallback Client 4.4.0; он исследован в [ELR design](../design/elr-replication-compatibility.md).

| Wire API | 3.2.3 | 3.7.1 | 4.4.0 |
|---|---|---|---|
| Metadata | 0–12 | 0–12 | 0–13 |
| ApiVersions | 0–3 | 0–3 | 0–5 |
| DescribeCluster | 0 | 0–1 | 0–2 |
| DescribeLogDirs | 0–3 | 0–4 | 1–5 |
| IncrementalAlterConfigs | 0–1 | 0–1 | 0–1 |
| AlterPartitionReassignments | 0 | 0 | 0–1 |
| ListPartitionReassignments | 0 | 0 | 0 |
| DescribeTopicPartitions | Нет; Metadata fallback | Нет; Metadata fallback | 0 |

Read-only проверки подтвердили describeCluster/Topics/Configs/Features/LogDirs, listTopics и
listPartitionReassignments. На старых brokers ELR/LastKnownELR в partition metadata остаются
**UNKNOWN (null)**, а assessment — NOT_APPLICABLE по отдельному положительному доказательству.
Пустые массивы вместо отсутствующих не создаются. Kafka 3.2.3 не сообщает usableBytes:
это UNKNOWN capacity и прежнее предупреждение disk reserve, а не выдуманное свободное место.
Rack, effective min ISR, log-dir health и topic UUID подтверждены реальными ответами.

## Реальные RF assignments

Во всех 18 строках apply exit0, ISR after равен множеству target replicas, лидер допустим,
UUID сохранён, active reassignment отсутствует. ELR/LastKnownELR на4.4 — подтверждённые `[]`;
на3.x поля отсутствуют, assessment N/A. Три RF12/RF23/RF32 операции — это 1→2/2→3/3→2.

| Kafka | Операция | Partition | Ordered before → after | ISR after | Leader before→after |
|---|---|---:|---|---|---|
| 4.4.0 | RF12 | 0 | `[1]` → `[1,3]` | `[1,3]` | 1→1 |
| 4.4.0 | RF12 | 1 | `[2]` → `[2,1]` | `[1,2]` | 2→2 |
| 4.4.0 | RF23 | 0 | `[1,3]` → `[1,3,2]` | `[1,2,3]` | 1→1 |
| 4.4.0 | RF23 | 1 | `[2,1]` → `[2,1,3]` | `[1,2,3]` | 2→2 |
| 4.4.0 | RF32 | 0 | `[1,3,2]` → `[1,2]` | `[1,2]` | 1→1 |
| 4.4.0 | RF32 | 1 | `[2,1,3]` → `[2,3]` | `[2,3]` | 2→2 |
| 3.7.1 | RF12 | 0 | `[3]` → `[3,2]` | `[2,3]` | 3→3 |
| 3.7.1 | RF12 | 1 | `[1]` → `[1,2]` | `[1,2]` | 1→1 |
| 3.7.1 | RF23 | 0 | `[3,2]` → `[3,2,1]` | `[1,2,3]` | 3→3 |
| 3.7.1 | RF23 | 1 | `[1,2]` → `[1,2,3]` | `[1,2,3]` | 1→1 |
| 3.7.1 | RF32 | 0 | `[3,2,1]` → `[3,2]` | `[2,3]` | 3→3 |
| 3.7.1 | RF32 | 1 | `[1,2,3]` → `[1,2]` | `[1,2]` | 1→1 |
| 3.2.3 | RF12 | 0 | `[2]` → `[2,1]` | `[1,2]` | 2→2 |
| 3.2.3 | RF12 | 1 | `[3]` → `[3,1]` | `[1,3]` | 3→3 |
| 3.2.3 | RF23 | 0 | `[2,1]` → `[2,1,3]` | `[1,2,3]` | 2→2 |
| 3.2.3 | RF23 | 1 | `[3,1]` → `[3,1,2]` | `[1,2,3]` | 3→3 |
| 3.2.3 | RF32 | 0 | `[2,1,3]` → `[2,3]` | `[2,3]` | 2→2 |
| 3.2.3 | RF32 | 1 | `[3,1,2]` → `[3,1]` | `[1,3]` | 3→3 |

UUID основных RF topics: 4.4 `lQh87mCVQLidVpsdruw6OQ`, 3.7.1 `uQEzPalSTcqwKypHXFlLpw`,
3.2.3 `fixko4-GQt2Pb-ZMxf8-PQ`. Полные topic names находятся в manifests и таблице evidence ниже.
Producer записал по6 уникальных сообщений в каждую из двух partitions: `acks=all`, idempotence.
Consumer после каждого RF проверил 12/12 keys/values/partitions/offsets0–5 через assign/seek без commits.
После каждого перехода guarded plan показал NO_CHANGES, saved-plan apply вернул0 с writeAttempts=0.
После конечной сборки каждый кластер ещё раз проверен guarded apply без writes.

На3.x дополнительно реально изменены max.message.bytes и retention.ms (ADD/UPDATE/REMOVE),
partitions main topic2→4 и stale topic2→4. Отрицательные checks: RF4/три брокера, decrease без allow,
decrease с no-delete, stale RF при actualRF3, stale partition JSON, partition decrease, invalid config,
unavailable localhost bootstrap. Отказы имеют exit2; guarded случаи — zero write attempts.
Invalid config является намеренно отправленным допустимым по scope запросом, отвергнутым брокером;
before/after configs неизменны. Нежелательных изменений при отказах не обнаружено.

## Два исправленных дефекта приложения

### Legacy KRaft epoch принимался за смену features

До исправления Kafka 3.7.1 блокировал RF1→2: FEATURES_CHANGED, без отправки RF.
В40 read-only диагностических preflight **21** получили разные epochs при одинаковых полных
finalized/supported maps; пример1355→1354. Это разные broker caches, а не изменение feature level.
`features()` возвращает metadata-image offset в [исходнике Kafka 3.7.1](https://raw.githubusercontent.com/apache/kafka/3.7.1/core/src/main/scala/kafka/server/metadata/KRaftMetadataCache.scala).

`ReplicationMetadataReader` теперь допускает такие offsets только при положительном N/A на обоих
чтениях, одинаковых полных картах и известных допустимых epochs. Changed maps/unknown mode
блокируются. Правило enabled ELR не менялось. Добавлен `LegacyFeatureEpochSpec`:8 tests, включая
реальный reader→policy→executor на fake Admin для всех трёх RF переходов и отрицательные карты/UNKNOWN.
Положительные regression cases воспроизвели дефект до исправления; полный suite прошёл после него.

### Однократная независимая verification отвергала lagging cache

После первого исправления preflight проходил и запросы принимались, но CLI возвращал
phase=VERIFICATION/UNKNOWN. Прозрачный real-Admin trace показал: в10:46:23.638 MSK описан полный
target ISR, затем в10:46:23.647 MSK другой ответ ещё не содержал добавленной ISR-реплики.
ListPartitionReassignments уже возвращал пустой результат. Никакого SUCCESS тогда не выводилось;
история двух submission attempts сохранялась предыдущим исправлением integration boundary.

`AdminReassignmentExecutor.verifyCompleted` теперь повторяет только чтения в пределах одного RPC
window (production10s) и первоначального deadline. Для ожидания нужны валидный лидер и сохранённый
retained ISR. Identity drift, foreign/conflicting assignments, опасный/неизвестный ELR прекращают
проверку сразу. Успех требует тех же exact target/full ISR/no active и ELR-specific proofs;
постоянный lag остаётся UNKNOWN. Новых RF отправок, rollback, cancellation или смены target нет.

`ReplicationFinalMetadataConvergenceSpec`:6 tests — old ISR/assignment converge, identity/retained ISR/
foreign target fail, постоянный lag не становится COMPLETED. Тесты воспроизвели отказ до изменения.
После исправления все три версии полностью проверены заново в новых namespaces одним JAR.
Эти дефекты относятся к политике наблюдений GitOps; downgrade клиента или подмена broker metadata не требовались.

### Сохранённая история попыток

| Run | JAR prefix | PASS / FAIL / BLOCKED / NOT_TESTED |
|---|---|---|
| 4.4: `20261113-101430-d31920` — повтор исходного финального JAR | a640d4d9 | 29 / 0 / 0 / 1 |
| 4.4: `20261113-103613-f9e720` — первый fix | c06cb0b1 | 34 / 0 / 0 / 2 |
| 3.7.1: `20261113-102529-918961` — gate aux volumes | a640d4d9 | 0 / 1 / 0 / 0 |
| 3.7.1: `20261113-102639-d76452` — epoch blocker | a640d4d9 | 18 / 0 / 3 / 0 |
| 3.7.1: `20261113-103647-ca03d0` — verification/trace | c06cb0b1 | 19 / 0 / 4 / 0 |
| 3.2.3: `20261113-110832-6d49a1` — bind path gate | 04c7fb17 | 0 / 1 / 0 / 0 |

Вместе с основными123 сценариями все сохранённые manifests этого задания содержат
**235 записей:219 PASS,2 FAIL,7 BLOCKED,7 NOT_TESTED**. Два FAIL — диагностические ошибки
подготовки gates до Kafka writes; BLOCKED включают зависимые RF-сценарии, не отправленные после
отказа предпосылки. Они не скрыты и не названы успешными RF tests. Старые namespaces сохранены:
первый3.7.1 RF topic остался RF1, второй диагностический — RF3, основной — RF2.

## Регрессия и окончательный артефакт

База 1019 Java tests сохранена без изменения assertions. Итог: **1033 tests, 46 suites,
failures/errors/skipped=0**; прирост 14. Legacy IntegrationSpec не запускались; write runners не
подключены к Gradle test. Успешны clean/classes/testClasses/test/shadowJar/buildExecutableJar/buildRelease,
56 CLI smoke checks (включая старые JSON), 21 offline Python guard test, codec/JNI tests.
Packaged-JAR codec smoke: array60, direct60, KafkaRecordBatches36; native LZ4/Snappy/Zstd загружены на Windows/JDK 21.

Финальная clean-сборка совпала с уже испытанными JAR/executable/ZIP по всем трём хешам:

| Артефакт | SHA-256 |
|---|---|
| `build/libs/kafka-gitops-all.jar` | `04c7fb173b15e87355d94f2f4373bf8d2fef5ee79574342a27f9af1a35c29c4e` |
| `build/output/kafka-gitops` | `edaeb405fcb8d3945365aeb5108c338a0d892c6047e3ea4f021d9805ab9119b6` |
| `build/distributions/kafka-gitops.zip` | `011ff62f0ad2bfec9f9ac536741b059a026daa7622b184ebf244274a18f44f7e` |

Размер JAR 25 844 467 bytes;8035 entries,7287 classes,13 runtime dependency JARs,53 native resources,
5 service descriptors, duplicate classes0. Main-Class=`com.devshawn.kafka.gitops.MainCommand`,
Multi-Release=true, external Class-Path отсутствует. Effective Java major65; MR22/23 не используются
JVM 21. Runtime classes/native/pom bytes и services сверены с dependency JARs. Build inventory
не изменился: Java 21/Kafka Client 4.4.0/Gradle9.9.1/Shadow9.6.1 и все dependency coordinates сохранены.
Lab probes, fixtures и credentials в JAR отсутствуют. Credential-pattern scan дал0 findings.

## Evidence, воспроизведение и конечное состояние

| Версия | Основной каталог evidence |
|---|---|
| 4.4.0 | `F:\DEV\GitOps\kafka-lab\gitops-integration\run-20261113-105238-8aa17e` |
| 3.7.1 | `F:\DEV\GitOps\kafka-lab-371\integration\run-20261113-105250-b34a5a` |
| 3.2.3 | `F:\DEV\GitOps\kafka-lab-323\integration\run-20261113-110938-8e3f8d` |

В каждом: manifest с command/exit code/status, YAML/JSON, metadata before/after, assignments,
leader/ISR/UUID, Docker proofs, API audits, seed-records и offset checks. Write/final hashes
сохранены отдельно как writeJarSha256/jarSha256 и совпадают во всех основных прогонах.
В roots новых labs: identity.json, Compose, startup/build commands/logs, image provenance;
в323/archive — binary/SHA512/ASC/KEYS, checksum/signature proofs, library inventory.
Локальные build/regression/CLI/JNI proofs: `.gradle/kafka3x/verification.json`,
`integration-summary.json`, baseline snapshots, red/green logs, epoch diagnostics и final build log.
Точный real-Admin trace — `DIAG23-api-trace.json` во втором диагностическом3.7.1 run.

После подготовки отдельных labs/probes и прохождения identity gate:

```powershell
python -B tools/integration/kafka3x_lab.py --version 371 --phase readonly
python -B tools/integration/kafka3x_lab.py --version 371 --phase basic --run-dir <NEW_RUN_DIR> --allow-writes
python -B tools/integration/kafka3x_lab.py --version 371 --phase rf --run-dir <NEW_RUN_DIR> --allow-writes
python -B tools/integration/kafka3x_lab.py --version 371 --phase negative --run-dir <NEW_RUN_DIR> --allow-writes
# Для3.2.3 — отдельный profile323 и его NEW_RUN_DIR.
python -B -m unittest discover -s tools/integration -p 'test_kafka*.py' -v
```

Runner не запускает/останавливает контейнеры и не удаляет ресурсы. Он намеренно привязан к этим
проверенным lab paths/ports/identities. Хеш JAR проверяется перед каждой дочерней командой.
Смена JAR в середине legacy run запрещена: после code fix
создавался новый run. Configs/Dockerfile находятся в `tools/integration/legacy/371` и `/323`;
точная build-команда323 с pinned base сохранена в lab/build-command.json.

Все **10 контейнеров** трёх стендов оставлены работающими:3+3+4. Volumes и evidence сохранены.
Active reassignment отсутствует; основные RF topics имеют RF2/full ISR; ELR4.4 остаётся1.
Kafka 4.4 содержит9 тестовых topics,3.7.1 —12,3.2.3 —4, включая диагностические namespaces.
Предыдущие UUID/configs/assignments сверены с baseline; удалений, prune, down-v и rollback не было.

## Ограничения и условия релиза0.4.0

Функциональная совместимость не является security-сертификацией старых brokers.
В архиве323 есть snappy-java 1.1.8.4, попадающий в [DoS advisory CVE-2023-34455](https://github.com/xerial/snappy-java/security/advisories/GHSA-qcwq-55hx-v3vh).
Для старых Kafka также опубликованы RCE/DoS advisories, включая Connect CVE-2023-25194 и broker
CVE-2025-27819; см. [официальный список Apache Kafka](https://kafka.apache.org/community/cve-list/).
Connect/JMX/REST не публиковались и Connect не запускался. Проверен исходный архив, без patching
его библиотек; полный CVE scan образов и exploit tests не выполнялись. Стенды остаются локальными
тестовыми окружениями с синтетическими данными.

Перед0.4.0 нужны отдельный non-production ADS4.3.1 acceptance (реальные vendor features,
cordoning/admission policies, auth), SSL/SASL/GSSAPI/authorizer проверки и Linux/ARM client-JNI smoke.
Следует отдельно проверить большие объёмы, длительные reassignment, recovery/partial failure и
согласованный fault testing. В этом задании брокеры ради fault injection не останавливались.
Непустой/неизвестный ELR, критический UNKNOWN, identity drift, unhealthy retained ISR, foreign
assignments и отсутствие разрешений продолжают блокировать операции. TOCTOU и отсутствие
leader pin в schema1 остаются эксплуатационными ограничениями.

## Изменения файлов и Git

Относительно начала этого задания изменены только два production-файла:
`service/ReplicationMetadataReader.java` и `service/AdminReassignmentExecutor.java`
под `src/main/java/com/devshawn/kafka/gitops/`. Обновлён текущий ELR design.
Добавлены два regression Specs, `Kafka3xLabProbe.java`, `Kafka3xDataProbe.java`,
`Kafka3xTraceApply.java`, `kafka3x_lab.py`, `kafka44_compat_checks.py`, `test_kafka3x_lab.py`,
семь файлов конфигурации/упаковки в `tools/integration/legacy/`, и этот отчёт.
Исходные4.4 runners не редактировались. ACL-семантика, JSON/YAML schema и зависимости сохранены.

`git diff --check`: exit0. Весь текущий `git diff --stat` от HEAD (включая ранее оставленные
ELR-изменения): **14 files changed,193 insertions(+),30 deletions(-)**. Новые untracked files
в этот stat не входят. Task delta:3 изменённых существовавших файла и16 новых файлов.
Прежние пользовательские изменения, gradle.properties и audit files сохранены; ветка/HEAD прежние.
