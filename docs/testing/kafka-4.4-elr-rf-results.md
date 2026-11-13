# Kafka 4.4: ELR-enabled RF, результаты

Дата: 12.11.2026, Europe/Moscow. Ветка `feature/ads-gitops`.
Исходный и конечный HEAD: `91a7c974beb098dbe49d339ed1bfddb282aad399`. Коммит не создавался.
Обоснование и API: [ELR compatibility](../design/elr-replication-compatibility.md).
Прежний [Docker-отчёт](kafka-4.4-docker-integration-results.md) и его evidence сохранены.

## Итоговая приёмка

На новом топике реально завершены **RF 1→2, 2→3 и 3→2 с ELR level 1**. Каждый apply вернул
exit 0 после fresh preflight, наблюдения terminal state и независимой проверки. После каждого
перехода совпали ordered target, полный ISR, допустимый лидер, UUID и пустые ELR/LastKnownELR;
активных reassignment не осталось. Проверены повторные plan/apply и сохранность данных/offsets.

Основной прогон `20261112-231114-925176`: **31 сценарий — 30 PASS, 0 FAIL, 0 BLOCKED,
1 NOT_TESTED**. Отрицательные проверки, успешно отвергнувшие опасный запрос, считаются PASS.
NOT_TESTED относится только к наблюдению промежуточного RUNNING при быстром decrease;
сам RF 3→2 и его завершение проверены и имеют PASS.

## Стенд и защита scope

| Параметр | Фактическое значение |
|---|---|
| Lab | `F:\DEV\GitOps\kafka-lab` |
| Docker context / Compose project | `desktop-linux` / `kafka-gitops-lab` |
| Docker Engine / Compose | 29.9.3 / 5.7.0 |
| Образ | `apache/kafka:4.4.0`, версия независимо прочитана Kafka CLI во всех трёх контейнерах |
| Runtime image digest | `sha256:833b867a9585a141da455dc2b57431c65cbccb27f11c91f694444cca9f777e0f` |
| Cluster ID | `4L6g3nShT-eMCtK--X86sw` |
| Brokers / racks | 1/rack-1, 2/rack-2, 3/rack-3 |
| Host endpoints | Только `127.0.0.1:19092`, `:19093`, `:19094` |
| Режим | KRaft, combined broker/controller, PLAINTEXT |
| ELR feature / metadata.version | 1 / 33 |
| Effective min ISR | 1, не изменялся |
| Cordoned dirs / log dirs | Явно пустые / healthy у всех брокеров |
| Compose SHA-256 | `de64c0eca27700c23cdd9c4618f2e057199f1d8196de51da4ea641737bde8b92` |

Перед каждым apply и единственным seed-batch повторён Docker/identity gate. Он проверяет
context, Compose services/project/working directory/config path, container labels и image digest,
runtime listeners, loopback DNS/port bindings, cluster ID, broker IDs/racks/fencing, quorum,
log directories, feature level, configs и отсутствие competing reassignment. Проверяется scope
JSON; для decrease — ровно один разрешённый RF UPDATE, без ACL/topic delete/config/partition operations.
`topicDetails` — описательный контекст существующей JSON schema; его partitions/configs дополнительно
сверяются с неизменённым actual state.

Все команды используют фиксированный `client.properties`, очищенное окружение `KAFKA_*`/
`CCLOUD_*`, `--skip-acls`. Writes runner требует явного `--allow-writes`; RF decrease дополнительно
использует `--allow-replication-decrease` без `--no-delete`. Ни features, ни Compose не менялись.

## Реальные назначения и ISR

Топик: `gitops-it-20261112-231114-925176-rf`, две partitions.
UUID до/после всех операций: `Doqj1dsVTpGlEyQuxCMzLg`.

| Сценарий | Partition | Ordered replicas before → after | ISR after | Leader before → after | Apply exit |
|---|---:|---|---|---|---:|
| RF12: 1→2 | 0 | `[2]` → `[2,1]` | `[1,2]` | 2→2 | 0 |
| RF12: 1→2 | 1 | `[3]` → `[3,1]` | `[1,3]` | 3→3 | 0 |
| RF23: 2→3 | 0 | `[2,1]` → `[2,1,3]` | `[1,2,3]` | 2→2 | 0 |
| RF23: 2→3 | 1 | `[3,1]` → `[3,1,2]` | `[1,2,3]` | 3→3 | 0 |
| RF32: 3→2 | 0 | `[2,1,3]` → `[2,3]` | `[2,3]` | 2→2 | 0 |
| RF32: 3→2 | 1 | `[3,1,2]` → `[3,1]` | `[1,3]` | 3→3 | 0 |

Во всех before/after ISR равен множеству actual replicas; **ELR=[] и LastKnownELR=[]**
получены реальным `describeTopics` Client 4.4.0, без подстановки вместо null. Feature level 1
подтверждён gate до каждого write и в final snapshot. Независимый Kafka CLI подтвердил assignments.

Read-only наблюдатель сохранил 27/24/23 snapshot в `RF12/RF23/RF32-trace.jsonl` с replicas,
ISR, leader, ELR, LastKnownELR, UUID и active reassignment. При increase зафиксирован RUNNING.
При decrease active sample отсутствует: короткая операция могла закончиться между чтениями.
Это не считается live-проверкой intermediate RUNNING для decrease. Различие ACK/COMPLETED
и остановка последующих пакетов проверены изолированными executor-тестами.

Producer записал 12 уникальных синтетических сообщений — по 6 на partition, `acks=all`,
idempotence включена. После seed и каждого RF перехода consumer проверил **12/12** значений,
ключей, partitions и исходных offsets (0–5 в каждой partition). Использовались explicit assign/seek,
без consumer group и offset commits. Topic UUID не менялся.

## Матрица основного прогона

| Сценарии | Количество | Результат / доказательство |
|---|---:|---|
| R01–R06 | 6 | PASS: gate, CLI/validate, read-only plan, proxy audit, properties precedence, broker CLI/quorum |
| E00 | 1 | PASS: отдельный namespace и baseline прежних тестовых топиков |
| E01 | 1 | PASS: создание нового RF1 topic, Admin + Kafka CLI |
| E02 / E02-DATA | 2 | PASS: seed и сверка исходных данных/offsets |
| RF12 / RF23 / RF32 | 3 | PASS: реальные RF operations, exact ordered targets и terminal proof |
| RF12/RF23-RUNNING | 2 | PASS: active reassignment наблюдался независимо |
| RF32-RUNNING | 1 | NOT_TESTED: промежуточный active sample не получен |
| RF*-DATA | 3 | PASS: сообщения и offsets сохранены |
| RF*-REPLAN | 3 | PASS: NO_CHANGES; zero guarded write attempts |
| RF*-REAPPLY | 3 | PASS: saved achieved plan, exit0; zero guarded write attempts |
| N01–N04 | 4 | PASS: безопасные отказы с exit2 и нулём write attempts |
| FINAL-JAR | 1 | PASS: финальный JAR повторно проверил достигнутый RF2 без writes |
| FINAL | 1 | PASS: identity/ELR/containers и прежние topics сохранены |

N01: RF4 при трёх брокерах отклонён `INSUFFICIENT_BROKERS`, JSON не экспортирован.
N02: decrease без разрешения; N03: decrease с `--no-delete`; N04: сохранённый previous RF1/
target RF2 при actual RF3. В каждой проверке real reads делегированы через Admin proxy,
который запрещает все write API и считает попытки. **writeAttempts=0**, metadata неизменны.

Конфликтующий reassignment, неполный ISR, опасный/неизвестный ELR, смена UUID/cluster,
min ISR/features и потеря лидера моделировались только fake Admin. Брокеры не останавливались,
quorum не менялся; live fault injection не проводился.

## Первый диагностический прогон

Сохранён отдельно: `run-20261112-230253-c14baf`, UUID `uCrmO5NFQUK9x6jfhzXWYg`.
RF1→2 прошёл. При RF2→3 actual target достигнут, но исходная независимая проверка вернула
exit2/UNKNOWN (`phase=VERIFICATION`, `STALE_PLAN`): ложного SUCCESS не было.
Топик оставлен с RF3; позднее read-only подтверждены полный ISR, пустые наборы и 12/12 сообщений.
Его manifest содержит **20 записей: 17 PASS, 1 FAIL события runner, 1 BLOCKED,
1 NOT_TESTED**. Эти записи не подменены результатами успешного прогона.

Исследование показало, что strict equality `FeatureMetadata`, включая metadata-image offset,
может отвергать неизменённые features во время metadata progress. В отдельной диагностике
после стабилизации 10/10 preflight-чтений были VERIFIED; исходный boolean terminal API
не сохранял точную внутреннюю причину отказа, поэтому утверждать её только по старому логу нельзя.
Семантика epoch проверена по исходнику Kafka. Добавлены сравнение полных feature maps с
неубывающим известным epoch для enabled mode, отрицательные тесты и вывод terminal enum reasons.

После успешной регрессии выполнен новый полный прогон в отдельном namespace. Затем исправлено
сохранение submission/lastKnownState при отказе независимой verification: прежняя обёртка
теряла эту историю. Это изменение проверено fake Admin, всеми 1019 тестами и final JAR no-write audit.

SHA-256 JAR реальных write-тестов основного прогона:
`f34575bba8757ff086a7530f8267e6d696b25a1fce54ca49326d2bbaff3890c9`.
Финальный JAR отличается сохранением истории при ошибке verification; write-policy не менялась.
Оба значения сохранены в manifest как `writeJarSha256` / `jarSha256`.

## Регрессия и сборка

До live-тестов прошли прежние 972 теста, затем 1014 с базовой ELR-поддержкой и 1019
после тестов feature epoch. Конечная чистая регрессия: **1019 tests, 44 suites,
failures/errors/skipped = 0**. Прирост 47: `ElrSafetyPolicySpec` — 18,
`ElrApplicationSpec` — 29. Прежние 972 теста и их исходные assertions сохранены.
Проверки включают missing/null vs empty, disabled/N/A, опасные metadata, отсутствие writes,
increase/decrease/permissions, post-send danger и независимую verification, epoch/maps и reapply.

Успешно выполнены `clean classes testClasses test shadowJar buildExecutableJar buildRelease`.
56 локальных CLI smoke checks включают старые JSON-планы и ошибочные параметры.
12 Python guard tests прошли. Java codec-тесты и отдельный packaged-JAR smoke подтвердили
LZ4/Snappy/Zstd: array=60, direct=60, KafkaRecordBatches=36; native backends загружены на Windows/JDK21.

| Артефакт | Размер / SHA-256 |
|---|---|
| `build/libs/kafka-gitops-all.jar` | **25 844 102 bytes**, `a640d4d9e991b6a9a6b3ff9fb628d966277a8f124897dae4d00e5f12850d3562` |
| `build/output/kafka-gitops` | `61676196565672a3bdaa5405d6eb2fcac7d7c28694b29aa4cfa7b97be3b3fe8a` |
| `build/distributions/kafka-gitops.zip` | `46e1c3b1593bfd5cdd235fb817af268caaeed87bb8460cb1d404f3b2245dc47b` |

Все три хеша совпали в двух финальных clean builds. Это один универсальный JAR;
executable и ZIP упаковывают его. Java major=65, Kafka Client=4.4.0, 13 runtime dependencies
побайтово совпадают с исходными dependency JARs, duplicate classes=0. 53 native resources и
5 services сохранены; MR entries 22/23 не используются JVM21. Lab probes/test helpers/credentials
в JAR отсутствуют. Gradle/dependency inventory не изменился. Проверка credential patterns дала 0 findings.

## Evidence и воспроизведение

Полный основной evidence:
`F:\DEV\GitOps\kafka-lab\gitops-integration\run-20261112-231114-925176`.
Содержит manifest со статусами/командами/exit codes, YAML/JSON, Docker proofs, before/after
metadata, API audits, наблюдения и data checks. Частичный прогон сохранён в соседнем каталоге.
Прежний `run-20261112-180451-668a0e` и `latest-run.json` старого runner сохранены;
новый runner использует `latest-elr-run.json`.

Локальные build/CLI/codec/inventory proofs: `.gradle/elr-rf/`, включая `verification.json`,
`baseline-tests.json`, `final-inventory.json`, clean build logs и `terminal-read-diagnostics.json`.
Обычный Gradle test не вызывает integration runner; существующие `*IntegrationSpec*` исключены.

```powershell
# Probes компилируются отдельно от приложения; запись в lab требует доступа к этому каталогу.
javac -cp build/libs/kafka-gitops-all.jar -d F:/DEV/GitOps/kafka-lab/gitops-integration/probe-classes tools/integration/KafkaLabProbe.java tools/integration/KafkaElrProbe.java
python -B tools/integration/kafka44_elr_rf.py --phase readonly
# Использовать НОВЫЙ RUN_DIR из вывода readonly:
python -B tools/integration/kafka44_elr_rf.py --phase rf --run-dir <NEW_RUN_DIR> --allow-writes
python -B tools/integration/kafka44_elr_rf.py --phase audit_final --run-dir <NEW_RUN_DIR>
python -B -m unittest discover -s tools/integration -p 'test_kafka44*.py' -v
```

`--resume` допускает только проверенную частичную подготовку RF1 с прежним UUID/seed и без
завершённых RF-сценариев. Он не повторяет неизвестную отправку. У этого стенда зафиксирован image
digest: изменение образа или identity намеренно требует отдельного пересмотра runner.

## Конечное состояние и ограничения

Все три контейнера работают, volumes сохранены; ELR=1, quorum здоров, active reassignment нет.
Исходные четыре топика `gitops-it-20261112-180451-668a0e-*` сохраняют UUID/configs/assignments.
Дополнительно оставлены диагностический RF3 topic и основной RF2 topic, каждый с 12 synthetic records.
Всего 6 тестовых топиков. Ничего не удалялось; автоматическая очистка не запускалась.

Непустые/неизвестные ELR/LastKnownELR, критические UNKNOWN, нездоровый ISR, stale identity/
assignments, недостаточный RF/min ISR и отсутствие разрешений остаются BLOCKED. Для разрешения
нужны реальные положительные доказательства, а не force-флаг или отключение features.
Kafka 3.2/3.7.1/3.9 и ADS 4.3.1 live не проверялись: сохранены capability/fallback контракты и unit coverage.
Большие объёмы, отказы брокеров, authorizer/ACL, vendor policy, нагрузка и disk reserve — за рамками
этого прогона. Неатомарность preflight/write и отсутствие leader pin в schema v1 остаются рисками.

## Изменённые файлы и Git

Tracked изменения:

- `domain/metadata/CapabilityAssessment.java`, `ReplicationClusterSnapshot.java`;
- `domain/plan/PlanningReport.java`, `ReplicationSafetyResult.java`;
- `manager/PlanManager.java`;
- `service/AdminReassignmentExecutor.java`, `AdminReplicationApplication.java`, `KafkaCapabilities.java`,
  `ReassignmentExecutor.java`, `ReplicaAssignmentPlanner.java`, `ReplicationMetadataReader.java`, `ReplicationSafetyPolicy.java`;
- `util/PlanReportRenderer.java`;
- `tools/integration/KafkaLabProbe.java`.

Java paths выше относительно `src/main/java/com/devshawn/kafka/gitops/`.
Новые файлы:

- `src/main/java/com/devshawn/kafka/gitops/service/ElrSafetyPolicy.java`;
- `src/test/groovy/com/devshawn/kafka/gitops/service/ElrSafetyPolicySpec.groovy`;
- `src/test/groovy/com/devshawn/kafka/gitops/service/ElrApplicationSpec.groovy`;
- `tools/integration/KafkaElrProbe.java`;
- `tools/integration/kafka44_elr_rf.py`;
- `tools/integration/test_kafka44_elr_rf.py`;
- `docs/design/elr-replication-compatibility.md`;
- `docs/testing/kafka-4.4-elr-rf-results.md`.

`git diff --check`: exit0. `git diff --stat`: **14 tracked files, 162 insertions(+), 29 deletions(-)**.
Обычный diff --stat не включает перечисленные 8 новых файлов. Уже существовавшие untracked
`gradle.properties` и `docs/audit/modernization-stage1-2026-11-11/` сохранены. Staging, смены ветки
и коммитов не выполнялось. Предупреждения Git LF→CRLF не являются ошибками whitespace check.
