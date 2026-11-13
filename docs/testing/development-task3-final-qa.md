<!-- SPDX-License-Identifier: Apache-2.0 -->
> Historical stage evidence: the dates, versions, hashes and test outcomes below describe that stage and artifact. Personal machine paths were anonymized for publication. See [0.4.0 preparation](../release/v0.4.0-release-preparation.md) for the current release status.

# Финальная техническая приёмка Kafka GitOps — Задание 3

Дата: **12.11.2026**, Europe/Moscow. Ветка `feature/ads-gitops`.
Исходный HEAD: `c0800cace018506652c7c81ec499e66727fc6410`.
Статус: **локальная техническая приёмка пройдена; проект готов к отдельно
авторизованному изолированному стенду. Production-ready не подтверждено.**

Выполнены аудит исходников, регрессия, минимальные исправления, повторный SCA,
проверка упаковки/воспроизводимости и программа будущих испытаний. Реальные
Kafka/ADS, Docker, настоящий `ccloud` и опасные `*IntegrationSpec*` не запускались.
Kafka writes в тестах — только обращения к fake Admin в памяти процесса.

## 1. Исходное состояние и итог

Перед работой проверены Git status/ветка/HEAD. Существовавшие untracked
`gradle.properties` и шесть файлов `docs/audit/modernization-stage1-2026-11-11/`
сохранены. Прежних tracked незакоммиченных изменений не было. Коммит/staging
не выполнялись, ветка не менялась.

| Показатель | Результат |
|---|---|
| Исходная регрессия | 929 tests, 38 suites, повторно выполнены успешно |
| Итоговая регрессия | **965 tests, 41 suites; failures/errors/skipped = 0** |
| Дополнительные сценарии | **36**, ни один прежний тест не отключён/не переписан |
| Найденные и исправленные дефекты | **9 групп**, перечислены ниже |
| Изменённые файлы | **19: 13 существующих + 6 новых** |
| JDK / bytecode | Temurin 21.0.13+9, application major 65; минимум Java 21 |
| CLI JAR smoke | **56** local-only checks, все прошли |
| JNI/codec | 59 существующих unit tests + 156 отдельных round trips |
| Сборка | clean/classes/testClasses/test/shadowJar/buildExecutableJar/buildRelease — SUCCESS |
| Воспроизводимость | SHA-256 JAR, shell executable и release ZIP совпали между двумя clean builds окончательного кода |

Сохранены Java 21, Gradle 9.9.1, Shadow 9.6.1, Kafka Client 4.4.0, Jackson 3.3.0
и весь dependency graph. Versions, constraints, wrapper и ACL-профили не менялись.

Изучены [Stage 1](../audit/modernization-stage1-2026-11-11.md),
[Task 2](../design/development-task2-compatibility-rf-ux.md),
[6B4](../design/stage6b4-regression.md), [6B3C](../design/stage6b3c-integration.md),
предыдущие security audits, README, quick start, installation, Gradle/CI и
реальный код указанных в задании слоёв. Исторические утверждения об opt-in RF
или закрытом production binding в старых документах не описывают текущий код.

## 2. Архитектурный аудит

| Граница | Проверенный контракт и ограничения |
|---|---|
| ParserService → StateManager | Strict unknown/duplicate fields, безопасные ошибки без исходного secret input, defaults/topic overrides; null root теперь отклоняется, RF range проверяется до IO |
| Config loader → KafkaService | Файл выигрывает по явно заданным свойствам, env заполняет отсутствующие; нет implicit bootstrap; клиент создаётся только при операции; malformed endpoint редактируется только в диагностике |
| PlanManager → PlanningReport → renderer | Диагностика вне executable JSON; известные facts/desired перед warnings/blockers/summary; critical blocker запрещает обычный writer; partial desired configs теперь не теряются |
| KafkaService | Каждая общая операция владеет своей короткой Admin session, bounded futures, Duration.ZERO close; planning разделяет один deadline |
| ReplicationMetadataReader / KafkaCapabilities | Только supplied Admin и общий snapshot deadline; broker/config/features/log dirs и повторные чтения; положительные proofs неприменимости, отсутствие поля не считается доказательством |
| ReplicaAssignmentPlanner | Чистый расчёт без IO/random; стабильные topic/partition/broker tie-break, ordered prefix/subsequence, projected load; apply не пересчитывает target |
| ReplicationSafetyPolicy | Fresh identity/ordered replicas/leader/ISR/min ISR/topology/permissions/conflicts/features; critical UNKNOWN остаётся blocker; только обоснованные typed operational risks становятся warning |
| ApplyManager | Permission/structure/RF и partition prerequisites до writes; RF failure запрещает follow-up topic/config/ACL operations; остальные операции только после RF COMPLETED/verification/close |
| AdminReplicationApplication | Единственный владелец одной RF Admin session для preflight/executor/independent verification; отсутствие/неполный result → UNKNOWN; close failure не превращается в успех |
| AdminReassignmentExecutor | Не создаёт и не закрывает supplied Admin; bounded sequential batches, per-future outcomes, read/observe/recheck; accepted RPC не completion; нет rollback/cancellation/resubmission принятой работы |
| ConfluentCloudService | CommandRunner injection для service tests; production child теперь имеет deadline/output bound/exit check/cleanup; в QA запускается только локальный Java probe |

Вложенных AdminClient внутри RF reader/planner/policy/executor нет. Единственная
реальная Kafka factory — `KafkaService.buildAdminClient`; RF application получает
её лениво. Plan не вызывает application/executor. Все futures в production Kafka
пути ограничены временем; циклы planner уменьшают конечную задачу, polling проверяет
operation/RPC deadline. Interrupt flag восстанавливается, в том числе после close.

Saved plans и record collections не мутируются исполнителем. Determinism проверен
существующими permutation tests по порядку Map/Set/partitions и сравнением JSON.
Snapshot не атомарный, а Kafka reassignment не CAS: гонки между последним read и
write полностью не устраняются. Повторные safety reads ограничивают риск; partial
или UNKNOWN результат не объявляется SUCCESS.

## 3. Подтверждённые дефекты и исправления

Каждая группа воспроизведена до исправления либо отдельным отрицательным JAR smoke.
Исходные 929 тестов проходили; новые проверки обнаружили отсутствовавшее покрытие.

| ID | Дефект | Исправление / regression evidence |
|---|---|---|
| D1 | YAML `null`/`~` возвращал null и приводил к NullPointerException вместо validation failure | Parser требует объект; CLI exit 2, без stack trace/ложного success |
| D2 | Явный RF 0/-1/32768 для нового topic и старого ADD JSON проходил локальную валидацию; short conversion мог исказить число, а ранее расположенный ADD мог выполниться | Range 1..32767 в YAML/defaults и общем PlanValidation; malformed JSON отклоняется целиком до любых writes |
| D3 | RF reader/application восстанавливали interrupt до finally; close, очищающий flag, терял состояние | Оба владельца сохраняют flag через close; fake close специально очищает его, execution/writes отсутствуют |
| D4 | При отказе DescribeConfigs partial plan терял уже известные desired config values | Renderer выводит UNKNOWN → desired с PREVIEW_ONLY; оба sensitive значения redacted; JSON остаётся blocked |
| D5 | `validate --help` и `account --help` были неизвестными options | Добавлены стандартные local-only help options; все четыре команды help exit 0 |
| D6 | Env JAAS helper экранировал quotes, но не backslashes: DOMAIN\\user/пароль менялись при разборе Kafka, некоторые строки становились невалидными | Корректное экранирование slash перед quote; round-trip через настоящий Kafka JaasConfig без login |
| D7 | Raw malformed bootstrap с user:password мог попасть в verbose config log; connection errors не показывали безопасный target | Общий bounded host:port/IPv6 diagnostic formatter; malformed/user-info/control text редактируются; phases/errors включают safe target, actual client properties неизменны |
| D8 | External ccloud wrapper не имел deadline/output bound, не проверял exit status и не закрывал ресурсы: valid JSON от failed process мог считаться успехом | 30 s shared budget, stdout ≤1 MiB, success только после exit 0, streams/direct child cleanup, safe IO errors; tests на Java child, не ccloud |
| D9 | Неизвестная root CLI option выводила ошибку, но возвращала exit 0 | `exitCodeOnInvalidInput=2`; воспроизведено JAR до/после, добавлен test |

D9 — **единственное изменение прежнего числового exit code**. Оно необходимо,
чтобы ошибка разбора команды не выглядела успешным CI run. Скрипты, рассчитывавшие
на exit 0 у неверных параметров/отсутствующей команды, должны использовать
`--help` для проверки запуска. Остальные command success/failure codes сохранены.
Изменён installation example и явно указан минимум Java 21.

Новые tests: `FinalQaRegressionSpec`, `FinalQaResourceSpec`, `FinalQaProcessSpec`,
локальный helper `QaProcessProbe`. Fixture strict Admin разрешает только нужные
reads, явно запрещает все create/delete/alter/incrementalAlter/update API и считает
session closes; полный CLI plan обязан успешно пройти эти reads, а не просто
остановиться раньше и показать нулевой write counter.

## 4. Регрессия plan, RF и ACL

| Сценарии | Основное покрытие / подтверждённый результат |
|---|---|
| Topic ADD/config ADD/UPDATE/REMOVE, no changes, previousValue | PlanManagerSpec, PlanFixturesSpec, PlanJsonSpec; прежние valid fixtures сохранены |
| Defaults и topic overrides | Fixture suite + новая проверка верхней RF-границы; допустимая числовая граница не обещает наличие 32767 brokers |
| Partitions increase/decrease, stale count, повторный apply | PartitionPlan/Json/Apply/Workflow/KafkaPartitionService specs; decrease отвергается, count наблюдается повторно |
| Several topics/partitions, mixed RF и порядок коллекций | ReplicaAssignmentPlannerSpec, ReplicationRegressionAuditSpec; deterministic ordered assignments и projected load |
| RF 1→2, 2→3, 3→2, 3→1, already target | Planner/Executor/Regression/Application workflow suites; no hidden rebalance, exact targets и no-op observation |
| Default RF / alias / opt-out / conflict / decrease/no-delete | DevelopmentTask2WorkflowSpec и ReplicationPlanning/ApplyGuard specs; permissions не сериализуются |
| Два brokers/racks и четыре `[0]` из исходного случая | KafkaCompatibilityTask2Spec и новый full CLI через real reader; при proofs — `[0,1]`, иначе честный critical UNKNOWN/BLOCKED |
| Insufficient brokers/нет eligible IDs/rack absence/storage/fenced/cordoned | Planner, Capability/Metadata/SafetyPolicy/Task2 specs; IDs не выдумываются, preserved unsafe target тоже blocked |
| Skip ACL/topics, no-delete, include/exclude | Существующие fixture/manager/workflow suites и new CLI tests; topic filters не меняют ACL semantics |
| Partial metadata, actual/desired, output order, blocked writer | Task2 + FinalQaRegressionSpec; changes перед warnings/blockers; existing output не перезаписывается |
| Все mutating API при plan | Strict proxy/fake Admin в metadata tests и новом полном CLI path; attempted writes = 0, каждый owned session закрыт |
| ACL profiles/bindings и legacy behavior Client 4.x | AclDetails/ServiceDetails/ApplicationService/CustomAcl/Role/manager fixtures; READ/WRITE/CREATE не менялись, auto-DESCRIBE не добавлен |

RF включён по умолчанию. `--manage-replication` сохранён deprecated alias,
`--no-manage-replication` opt-out. Decrease показывается plan, но требует свежего
`apply --allow-replication-decrease`; no-delete сильнее allow. Вызов plan не
приводит к автоматическому apply. Текст preview явно требует fresh preflight.

## 5. Apply, executor и частичные результаты

Проверен путь saved JSON → CLI → strict validation/permissions → ApplyManager →
fresh preflight → executor → independent verification → итог/close/follow-up.
Существующие regression suites покрывают cluster/topic identity, ordered replicas,
leader/ISR/min ISR, rack/broker eligibility, активный reassignment, drift после plan,
drift после preflight и между batch. Ни cached READY, ни старый JSON не являются
разрешением. При critical UNKNOWN/BLOCKED и permission failure mutating calls = 0;
другие topic/config/ACL изменения не начинают применяться после отказа RF.

Executor tests проверяют sequential bounded packets, individual future timeout,
partial acceptance, late-completed sibling futures, RPC/operation deadline,
interrupt, принятый чужой compatible reassignment, конфликт, restart, потерю связи,
full target ISR/leader и отсутствие active reassignment. Успех alter RPC остаётся
ACCEPTED; RUNNING/UNKNOWN/FAILED не становятся общим успехом. При rerun выполненная
работа наблюдается, не отправляется повторно. Сохранённый target не меняется.
Rollback и отмена чужого reassignment отсутствуют.

Обычные non-RF creation/config/ACL пути не имеют универсальной cluster-ID привязки
в legacy JSON. Их защита — валидный scope/permissions и правильный endpoint;
для будущих стендовых writes обязательно внешнее независимое identity gate.
Schema 1 не фиксирует исходного leader, поэтому он перечитывается как live invariant.

## 6. YAML/JSON и секреты

Сохранены previousValue, partitionPlan, replicationPlan/schemaVersion 1,
previousReplicas/targetReplicas и существующие actions. Saved JSON читает отдельный
strict mapper: unknown fields/operations, duplicate keys, coercion, null primitives,
trailing input, malformed identities/replicas/schema отклоняются. Generic model
validation повторяется до apply и serialization. Новый диапазон RF отвергает
невалидный input; действующие valid старые планы не переписываются.

YAML duplicate/unknown/malformed fields, multi-file definitions, defaults/overrides
и ACL formats покрыты fixture/diagnostic tests. Null root исправлен; ошибки не
повторяют пользовательский secret input. Историческое чтение одного YAML document
не заменено новым multi-document форматом; trailing-document policy не расширялась.
Экспорт sensitive values запрещён прежними serializers; text renderer скрывает
оба значения, включая неполный preview. Все 34 valid старых JSON fixtures прочитаны
в JAR smoke с обеими skip-категориями без Kafka IO.

Проверены high-confidence private-key/token patterns в `src`, `docs`, `.github`
и распакованном JAR: подтверждённых credentials **не найдено**. Два delimiter hits
разобраны отдельно: синтетический PEM-header fixture redaction test и public Kafka
PEM-parser constant `AssertionUtils.class`; ключевого материала в них нет. Не
выводились matched values. Это ограниченная проверка, не математическое доказательство
отсутствия всех секретов. External properties/credential stores не копировались
и не сканировались без необходимости; test fixtures содержат только synthetic secrets.

## 7. Connection config и CLI

Файл command-config выигрывает у environment для явно указанных свойств;
environment заполняет отсутствующие и работает без файла. PLAINTEXT/SSL не требуют
SASL, PLAIN/SCRAM helpers сохранены, GSSAPI допускает внешний JAAS/keytab. Разбор
credentials и properties tests не является реальным authentication handshake.

| Сценарий | Exit / наблюдение |
|---|---|
| Help/version/validate | 0; без сетевого IO |
| NO_CHANGES | 0, отдельный SUMMARY status |
| PLAN_BUILT / warnings без blockers | 0, preview явно требует fresh preflight |
| PLAN_BLOCKED / insufficient or critical metadata | 2; no export, известные facts остаются видимыми |
| Bootstrap missing / file absent or malformed | 2; BOOTSTRAP_MISSING / COMMAND_CONFIG_UNREADABLE, без localhost fallback |
| Fake DNS/refused/unavailable/SASL/SSL/authz | 2 на CLI workflow failure path; typed diagnostics tested через KafkaService futures; реальная сеть не проверялась |
| Timeout / interrupt | Ограниченное завершение; TIMEOUT/INTERRUPTED, flag сохраняется |
| RF permissions/no-delete/stale/preflight | 2, нет writes/follow-ups до разрешённого исполнения |
| Partial/unknown execution, verification/follow-up/close failure | 2, нет Apply complete; per-partition/phase evidence сохраняется |
| Root invalid option / missing command | **2, раньше 0**; это исправление D9 |

Общие normal command codes 0/2 сохранены. Точный низкоуровневый network cause не
выдумывается: Kafka может отдать только timeout. Этапы CONFIG/CONNECT/METADATA/PLAN
не называют сеть YAML-ошибкой. Safe target — diagnostic host:port summary, не
доказательство успешного подключения/DNS/identity. Text output не является stable
machine schema: автоматизация читает существующий JSON и проверяет exit/status;
старый output после failed plan нельзя использовать как новый.

## 8. Возможности и матрица совместимости

SUPPORTED означает полученное наблюдение; NOT_APPLICABLE требует положительного
mode/feature-bound proof. Отсутствие свойства не доказывает неприменимость.
UNKNOWN critical блокирует; UNSAFE блокирует соответствующее размещение. Missing
usableBytes не становится 0; generic admission и transfer reserve могут остаться
обоснованными warnings, но known storage/zero-space/placement failure — blockers.

Default DescribeCluster broker response и Node fencing учитываются без нового
includeFencedBrokers=true. ZooKeeper подтверждается явными process.roles/zk configs.
Metadata feature bounds 23/30 используются для ELR/cordoning applicability,
не для точного release. ELR enabled или unknown остаётся blocked для RF-алгоритма.
RACK_AWARE требует reliable racks; BROKER_ONLY выбирается явно с предупреждением.

| Целевая поставка | Локальное evidence | Live статус |
|---|---|---|
| Kafka 3.2.x | Базовые Admin API, old metadata/log dirs и положительные mode proofs в fakes | NOT_TESTED; старый KRaft без proofs может BLOCKED |
| Kafka 3.7.1 | Та же универсальная сборка/контракты | NOT_TESTED |
| Kafka 3.9.x | Planner/reader/executor regression contracts | NOT_TESTED |
| ADS Kafka 4.3.1 | Стандартные API без предположения о vendor backports | NOT_TESTED, vendor features/security отдельно |
| Kafka 4.4.x | Client 4.4.0, new fields/feature fault fakes | NOT_TESTED, ELR-enabled RF не поддержан |

Подробные positive proofs и первичные KIP/MetadataVersion ссылки — в
[Task 2](../design/development-task2-compatibility-rf-ux.md). Ни один mock результат
не назван успешным испытанием конкретного сервера.

## 9. Повторный security audit

Получен свежий resolved graph для runtime/test/compile/annotationProcessor/
JaCoCo, settings/build plugins и Groovy compiler. Побайтовое сравнение baseline/final
инвентаризаций совпало. Дополнительно проинвентаризированы Gradle bundled и embedded
Maven metadata: **429 физических JAR, включая 350 Gradle JAR; 442 идентифицированные
Maven coordinate/version records**. Число records не равно числу JAR: есть embedded
components и один artifact может присутствовать в нескольких scopes.

Публичным OSV querybatch передавались только Maven coordinates/version; исходники,
конфиги и credentials не отправлялись. На дату аудита активные OSV matches для
runtime/test/compile/build-plugin/settings/Groovy compiler graphs = **0**;
для Gradle bundle — Guava. Дополнительно обновлены advisories 12 repositories,
сопоставлены primary vendor records Jackson/Guava и официальный Kafka CVE list.
Один scanner не считается полной оценкой безопасности; vendor Jackson finding
сохраняется даже при отсутствии его в полученном OSV match list.

| Finding | Фактическая область | Оценка применимости |
|---|---|---|
| [CVE-2026-102554 / GHSA-xxph-c9ww-hj94](https://github.com/google/guava/security/advisories/GHSA-xxph-c9ww-hj94) | Gradle Guava **33.4.6-jre**; vendor range включает эту версию, исправленная линия 33.7.2 | **Остаётся version-affected**. Опасный путь — native Java deserialization недоверенных Guava collections. В приложении Guava/ObjectInputStream не используются, эксплуатации через Gradle здесь не доказано |
| [CVE-2026-110879 / GHSA-m8xf-2cr7-77ph](https://github.com/FasterXML/jackson-databind/security/advisories/GHSA-m8xf-2cr7-77ph) | Gradle Databind **2.22.3**, range 2.22.0–2.22.3, fix **2.22.4**; compiler marker 2.23 и app 3.3 вне полученных affected ranges | **Остаётся version-affected в Gradle**. Bypass type validation при class-name polymorphism; путь атаки внутри Gradle не установлен. App не включает class-name default typing |

Guava JAR SHA-256:
`958a035b74ff6c7d0cdff9c384524b645eb618f7117b60e1ee915f9cffd0e716`.
Gradle Databind SHA-256:
`556db5439e206114346043f68d200497dc96a0bca62a360a81784092ebd0e0a9`.
Оба находятся в официальном Gradle distribution, не в runtime fat JAR.
Не применялись принудительные overrides внутренних Gradle libraries. Требуется
поддерживаемое исправление build tool/vendor; до него сохраняется ограничение
доверия к build inputs/caches/plugins. NVD web views в этом прогоне не дали читаемого
подтверждения; identifiers/ranges проверены по доступным первичным vendor advisories
и GitHub records, а не выведены из названий прошлых отчётов.

Broker CVE — отдельный scope. [Официальный Kafka CVE list](https://kafka.apache.org/community/cve-list/)
по-прежнему включает upstream 4.3.1 в ряд server affected ranges, в частности
CVE-2026-114349/112375. Наличие ADS backports не известно. Для 114349 vendor требует
также конечный max.decompressed.message.bytes даже после обновления на исправленную
ветку. Клиентский JAR не исправляет broker. DoS/exploit проверки не запускались;
JDK/OS/container и неизвестные native/shaded versions не объявляются очищенными
одним Maven scan. Security findings учитываются при подготовке стенда отдельно.

## 10. Сборка и JAR

Окончательный код проверен командой:

```powershell
.\gradlew.bat --gradle-user-home "$env:USERPROFILE\.gradle" --offline --no-daemon --no-scan --console plain --warning-mode all clean classes testClasses test shadowJar buildExecutableJar buildRelease
```

Основной прогон **BUILD SUCCESSFUL, 33 s, 9 executed tasks**; контрольный clean
прогон **BUILD SUCCESSFUL, 29 s, 9 executed tasks**. Оба включают все 965 tests.
Граф ранее получен отдельной read-only init task. Прежний deprecation notice
TopicConfigSecurity остаётся; Sonar/build scans и GitHub Actions не запускались.

| Артефакт / проверка | Результат |
|---|---|
| Основной JAR | `build/libs/kafka-gitops-all.jar`, **25 834 708 bytes (24.64 MiB)** |
| SHA-256 | `bf30233eee3aa5cae5c0f7ce8a449630530a8ab445632fb2762743881ae933d5` |
| Main-Class / manifest | `com.devshawn.kafka.gitops.MainCommand`, Multi-Release=true, без внешнего Class-Path |
| Runtime JARs / ZIP entries / classes | 13 / 8034 / 7286 |
| Duplicate entries / runtime classes | 0 / 0 |
| Native resources / merged service descriptors | 53 / 5 |
| Java | Application/effective JDK21 classes ≤ major 65, app major 65; MR entries 22/23 не активны на Java 21 |
| Kafka | Единственный Kafka Client **4.4.0**, нет Kafka Client 3.x |
| Исключения | Нет Jackson 2 core/databind, FreeBuilder, test engines, QaProcessProbe, test fixtures или client credentials |
| Сверка dependency contents | Каждая runtime class/native/pom.properties побайтово совпадает с исходным dependency JAR; descriptors корректно объединены |
| Executable / ZIP | POSIX launcher + тот же JAR; ZIP содержит этот executable с mode 0755 |

Runtime: Kafka 4.4.0; Jackson core/databind/YAML 3.3.0 + annotations 2.23;
SnakeYAML engine 3.2; Picocli 4.7.7; SLF4J 2.0.20 + Logback classic/core 1.6.6;
lz4-java 1.14.0; snappy-java 1.1.10.12; zstd-jni 1.5.7-26.
Windows JNI LZ4/Snappy/Zstd действительно загрузились. Отдельно выполнены
60 array, 60 direct-buffer и 36 Kafka record-batch round trips.

Хеши двух clean builds совпали для всех трёх артефактов:

```text
JAR:        bf30233eee3aa5cae5c0f7ce8a449630530a8ab445632fb2762743881ae933d5
Executable: 5eb7717d6b39d30d3dece086b421eb193f694f60c3ea47651e8a67c8dc670050
ZIP:        c5b310922f827e4e4651ba342edfbfce628b380bc681b58b2219f338ffe3db0f
```

Это воспроизводимость в данном Windows/JDK/cache окружении; cross-OS/cross-JDK
битовая идентичность отдельно не заявляется. Минимальный runtime **Java 21**;
совместимость на более старой Java не предполагается.

## 11. Ограничения и готовность к стенду

1. Реальный protocol negotiation, SSL/SASL/GSSAPI, права, advertised listeners,
   vendor metadata и RF throughput не проверены; все серверные строки NOT_TESTED.
2. ELR-enabled RF остаётся unsupported; старый KRaft без положительных proofs может
   отказать. Disk headroom/transfer reserve/custom placement admission требуют
   эксплуатационной оценки и не гарантируются read-only metadata.
3. Snapshot не атомарен; accepted/partial work может продолжиться после timeout
   или interrupt. Нет автоматического rollback/cancellation. Проверять состояние
   перед повтором, не редактировать старый target для обхода validation.
4. Нужен независимый cluster-ID gate для всех будущих non-RF writes/cleanup.
   ACL regression не разрешает удалять неизвестные bindings на общем кластере.
5. Остались два Gradle version findings; native/OS/JDK/container assessment отдельно.
   Публичные Homebrew/Docker packages не сверялись с локальным JAR.
6. Legacy GitHub CI workflow всё ещё содержит отдельный Docker Compose start.
   Он здесь не исполнялся; Gradle test сам исключает IntegrationSpec. Перед будущим
   запуском CI следует отдельно пересмотреть ненужный Docker шаг и внешние
   Sonar/build-scan действия. Ни запуск, ни успешность hosted CI не заявляются.
7. Внешний ccloud проверен только service fakes и локальным child probe; реальная
   версия CLI, её output size/encoding/exit behavior остаются отдельным испытанием.

Критерии входа на стенд: отдельное разрешение, сетевой allowlist и независимый
cluster ID, disposable namespace/credentials, известные версии/features/server
patches, read-only API audit и cleanup manifest. Порядок и точные критерии каждого
R/W-case описаны в [программе испытаний](kafka-integration-test-plan.md).
Каждый write-case и cleanup требуют отдельного явного разрешения и свежего identity
check. Ни elapsed wait, ни успешный unit test такого разрешения не заменяют.

Технический вывод: исправленные локальные code paths и универсальная упаковка
достаточно проверены для следующих изолированных интеграционных испытаний.
Готовность к production требует их результатов и закрытия/принятия оставшихся рисков.

## 12. Изменённые файлы и evidence

```text
docs/installation.md
docs/testing/development-task3-final-qa.md (new)
docs/testing/kafka-integration-test-plan.md (new)
src/main/java/com/devshawn/kafka/gitops/MainCommand.java
src/main/java/com/devshawn/kafka/gitops/StateManager.java
src/main/java/com/devshawn/kafka/gitops/cli/AccountCommand.java
src/main/java/com/devshawn/kafka/gitops/cli/ValidateCommand.java
src/main/java/com/devshawn/kafka/gitops/config/KafkaGitopsConfigLoader.java
src/main/java/com/devshawn/kafka/gitops/service/AdminReplicationApplication.java
src/main/java/com/devshawn/kafka/gitops/service/ConfluentCloudService.java
src/main/java/com/devshawn/kafka/gitops/service/KafkaService.java
src/main/java/com/devshawn/kafka/gitops/service/ParserService.java
src/main/java/com/devshawn/kafka/gitops/util/KafkaDiagnostics.java
src/main/java/com/devshawn/kafka/gitops/util/PlanReportRenderer.java
src/main/java/com/devshawn/kafka/gitops/util/PlanValidation.java
src/test/groovy/com/devshawn/kafka/gitops/FinalQaRegressionSpec.groovy (new)
src/test/groovy/com/devshawn/kafka/gitops/service/FinalQaProcessSpec.groovy (new)
src/test/groovy/com/devshawn/kafka/gitops/service/FinalQaResourceSpec.groovy (new)
src/test/java/com/devshawn/kafka/gitops/service/QaProcessProbe.java (new)
```

Локальные evidence и scripts: `.gradle/development-task3/` (Git ignored):
baseline status/HEAD/tests/inventory/build log; defect reproduction logs;
final/reproducible build logs; current coordinates/JAR inventory; fresh
OSV/vendor/specific advisories и security summary; `verify.py`, `verification.json`,
56 CLI logs, codec log, artifact hashes и итоговые Git checks. Raw secret values
не публикуются. Постоянные результаты/ограничения отражены в этом отчёте;
локальные paths не являются дополнительными tracked source changes.

`git status --short`, `git diff --check`, `git diff --stat` выполнены.
Whitespace errors отсутствуют; возможные LF→CRLF notices относятся к Windows
настройке Git. Коммит не создавался. Реальные интеграционные испытания не начаты.
