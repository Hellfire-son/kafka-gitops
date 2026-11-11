# Stage 6B4 — финальная регрессия и аудит Replication Factor

Дата: **2026-11-11**, ветка `feature/ads-gitops`, исходный HEAD
`2d3afdd2a2cfff9187505a7ae3c4f0dd531a880f`. Рабочее дерево перед началом было чистым.
Стек сохранён: Java 21, Gradle 8.14.6, Shadow 8.3.11, Kafka Client 3.9.2.
Версия ADS Kafka 4.3.1 принята из задания; реальный кластер не опрашивался.

**Результат:** проверенный внутренний RF-путь сохраняет fail-closed; production RF
по-прежнему заблокирован. Пройдены **814 unit-тестов: 742 прежних + 72 новых**,
49 безопасных CLI smoke checks и локальные Windows JNI/codec checks.
Это основание для ограниченных испытаний Stage 7, а не утверждение production-ready.

Изучены исходники и документы [6A/6B1](stage6-replication.md), [6B2](stage6b2-planning.md),
[6B3A](stage6b3a-preflight.md), [6B3B](stage6b3b-executor.md), [6B3C](stage6b3c-integration.md).
Устаревшие формулировки предыдущих документов относятся к состоянию соответствующего этапа.
Здесь фиксируется фактическое состояние после 6B3C и минимальных исправлений аудита.

## 1. Архитектурные результаты

| Компонент / метод | Проверенный контракт |
| --- | --- |
| `PlanManager.planTopics()/planReplications()` | Opt-in RF, чтение метаданных, проверка дрейфа UUID/count/ordered assignments, вызов чистого planner. Нет reassignment writes. Новые топики сохраняют прежнее создание через TopicDetails |
| `TopicReplicationPlan`, `PartitionReplicaAssignment`, `ReplicationPlanValidation.validate()` | Schema 1, UPDATE, canonical non-reserved UUID, RF 1..32767, min ISR, полный набор партиций/брокеров, отсутствие дубликатов, ordered prefix/subsequence и запрет скрытого rebalance |
| `PlanManager.readPlanFromFile()` | Строгий отдельный ObjectMapper: неизвестные поля/операции/версии, null primitives, coercion, trailing tokens и duplicate JSON fields отклоняются; ошибка не повторяет входные секреты |
| `ReplicationMetadataReader.read()` | Только supplied Admin, общий deadline, повторные чтения identity/assignments/ISR/features/configs. UNKNOWN fencing/admission/capacity не превращается в VERIFIED. Config/hosts/log-dir paths не входят в RF snapshot |
| `ReplicaAssignmentPlanner.plan()/choose()` | Нет Admin/Node/Config IO; стабильные topic/partition order и tie-break по broker ID, projected load, rack policy, сохранение исходных реплик/лидера и порядка. Apply не пересчитывает размещение |
| `ReplicationSafetyPolicy.evaluateForExecution()` | Централизованные permissions, свежесть, identity/topology, min ISR, ISR/leader, fencing/storage/cordoning, admission/capacity, ELR и conflicts; одна ошибка блокирует весь RF-набор |
| `ApplyManager.applyPlan()` | До RF — permissions/structural validation и preflight иных partitionPlan; после полного RF COMPLETED/verification/close — остальные topic configs/partitions/ACL. Ошибка followup не заявляет отсутствие изменений |
| `AdminReplicationApplication.apply()` | Единственный владелец RF Admin от свежего preflight до независимой verification. Проверяет scope результата, превращает потерянные/неполные результаты в UNKNOWN; закрывает с Duration.ZERO ровно один раз |
| `AdminReassignmentExecutor.execute()/verifyCompleted()` | Последовательные ограниченные пакеты, свежая policy перед каждой отправкой, per-partition futures, observation до exact target/full target ISR/leader/no active. Не владеет Admin, не выполняет rollback/cancellation |

Структурная валидация повторяется при чтении, фильтрации, сериализации и исполнении через
общий ReplicationPlanValidation. Permissions повторяются на границах ApplyManager,
application и Request; живые проверки — перед каждым пакетом. Это защита от программного
вызова в обход CLI и устаревшего READY. Удалять повторные проверки ради сокращения кода нельзя.
Противоречивого разрешения записи при UNKNOWN в проверенных путях не обнаружено.

Есть консервативные различия между слоями: preview-проверка критических configs в
PlanManager уже, чем apply-policy; `Constraints.forTopic()` блокирует критические ключи
и в TopicDetails, даже если они не представлены изменяющей config operation. Поэтому
структурно допустимый preview не обещает READY. Это может отклонять план с неизменённым
min ISR в TopicDetails, но не даёт обхода безопасности. До включения production RF следует
согласовать явные assertions и действующие config changes; здесь правило не ослаблялось.

Прежний `PlanValidation.validateForApply()` содержит общий исторический запрет RF;
актуальный StateManager использует `ApplyManager.applyPlan()`. Старые applyTopics/applyAcls
также сохраняют барьер. Различие между default production construction и явной внутренней
инъекцией application намеренное, не управляется JSON/CLI/env.

RF metadata reader и executor не создают вложенных AdminClient. Другие legacy операции
KafkaService имеют отдельные try-with-resources сессии; вся команда apply не является одной
Kafka-транзакцией. Закрытие проверено при checked/runtime failures, interrupt, partial result,
verification/followup/close failures. JVM crash/forced termination не покрываются гарантиями finally.

Planner не импортирует Kafka API. Однако общие Snapshot/ExecutionResult ещё используют
Kafka TopicPartition как ключ, а UUID validation — Kafka Uuid. Это существующая связь модели
с библиотекой, которую следует учитывать при дальнейшем отделении адаптеров.

## 2. Подтверждённые дефекты и минимальные исправления

RF planner/policy/executor, модели и JSON-схема **не изменялись**. Исправлены соседние
ошибки диагностики и загрузки конфигурации, найденные при проверке всей цепочки Stage 1–6.

| ID / риск | До исправления | Исправление и проверка |
| --- | --- | --- |
| D1 / высокий при наличии секретов в ответе | Восемь старых методов KafkaService передавали `ex.getMessage()` в KafkaExecutionException и CLI; RuntimeException от API/factory/close выходил с raw текстом | Общий `operationFailure()` оставляет собственное описание операции и класс ошибки; raw message/cause/suppressed не сохраняются. 24 сценария: 8 операций × future/runtime/close |
| D2 / средний | Эти же catch теряли interrupt flag после InterruptedException | Helper восстанавливает флаг; проверены interrupt + close и безопасный CLI formatter. RF interruption checks сохранены |
| D3 / высокий для операций legacy | `--command-config` сохранялся в ManagerConfig, но StateManager всегда вызывал `load()` без файла. Ошибка чтения явно указанного файла в loader логировалась и допускала продолжение с defaults | StateManager передаёт Optional config file в loader. IOException/invalid properties останавливают загрузку безопасным MissingConfigurationException; defaults при ошибке явного файла больше не используются. ValidateCommand обрабатывает ошибку с exit 2 |
| D4 / высокий при конфиденциальном некорректном вводе | ParserService повторял value, dynamic field names, YAML path и сообщения/causes Jackson; они могли включать пароль/токен или исходный фрагмент | Фиксированные сообщения по категории parser error, без source/value/path/cause. Также убрано повторение внешнего имени included file |
| D5 / условный риск раскрытия пути/данных в IO error | PlanManager.writePlanToFile повторял IOException message, а INFO overwrite содержал полный destination | Фиксированная IO-диагностика и INFO без пути. Сериализация по-прежнему выполняется до открытия destination; JSON-контракт не менялся |
| D6 / средний | buildAdminClient обращался к `ex.getCause().getMessage()`: возможны raw secrets и NPE при null cause | Та же безопасная классификация RuntimeException без разыменования cause. Проверка factory failure выполняется fake, настоящий AdminClient не создаётся |

Новые тесты сначала выполнены против исходного кода: **34 из 37 новых диагностических
сценариев воспроизвели ошибки**, все 742 прежних прошли. После исправления диагностический
набор расширен до 41 сценария; он проходит полностью. Дополнительно проходят 31 RF audit case.

Изменены шесть Java-файлов относительно `src/main/java/com/devshawn/kafka/gitops/`:

- `service/KafkaService.java`: восемь legacy операций, buildAdminClient и новый operationFailure;
- `config/KafkaGitopsConfigLoader.java`: setConfigFromFile;
- `StateManager.java`: загрузка command-config в конструкторе;
- `cli/ValidateCommand.java`: обработка MissingConfigurationException в call;
- `service/ParserService.java`: безопасные ошибки parseStateFile/loadExternalFile;
- `manager/PlanManager.java`: ошибки/overwrite-лог writePlanToFile.

Новые unit-файлы: `service/KafkaDiagnosticsAuditSpec.groovy` и
`service/ReplicationRegressionAuditSpec.groovy` в `src/test/groovy/com/devshawn/kafka/gitops/`.
В четырёх text fixtures `invalid-format-output.txt`, `invalid-topic-output.txt`,
`unrecognized-property-output.txt`, `invalid-storage-topics-output.txt` обновлены только
ожидаемые строки безопасных ошибок. Они дополнительно проверяются unit-тестами;
ни одна прежняя спецификация/проверка не отключена и не изменена. Все JSON-эталоны сохранены.

Изменение текстов ошибок некорректного YAML и остановка при недоступном command-config —
намеренные исправления безопасности. Формат properties, приоритет environment над file
и передача настоящих значений Kafka сохранены: credentials не заменяются масками в конфигурации.

## 3. Матрица проверок безопасности RF

Основные доказательства: прежние ReplicationModel/Json/Planning/Preflight/Metadata/SafetyPolicy/
Executor/ApplicationWorkflow specs и новый ReplicationRegressionAuditSpec.
В таблице запись означает вызов mutating API **строгого fake**, не реальную операцию Kafka.

| Сценарий | Ожидаемый исход | Проверенное отсутствие опасных действий |
| --- | --- | --- |
| Неизвестный/изменённый cluster ID или topic UUID | Structural reject либо BLOCKED / STALE_PLAN | До первой отправки writes = 0; после отправки — не COMPLETED, без rollback |
| Другой partition count/IDs, дубликаты | Structural reject / PARTITION_COUNT_CHANGED | Полный RF scope проверяется до writes |
| Ordered previous replicas изменились после plan | PREVIOUS_REPLICAS_CHANGED / STALE_PLAN | Никакого нового алгоритма назначения при apply |
| ISR/leader изменились после preflight до отправки | STALE_PLAN | writes = 0; проверен controlled interleaving после fresh policy |
| Broker offline/UNKNOWN, fencing UNKNOWN | BROKER_OFFLINE / BROKER_HEALTH_UNKNOWN / BROKER_FENCING_UNKNOWN | Executor/остальные ресурсы не получают разрешения записи |
| Target RF ниже effective min ISR; значение отсутствует/изменилось | TARGET_BELOW_MIN_ISR / MIN_ISR_UNKNOWN / MIN_ISR_CHANGED | writes = 0 |
| Admission UNKNOWN; usable bytes/reserve UNKNOWN или недостаточны | PLACEMENT_UNKNOWN / DISK_CAPACITY_UNKNOWN / DISK_INSUFFICIENT | Положительное usableBytes само по себе не READY |
| ELR UNKNOWN/ENABLED, feature epoch/config drift | BLOCKED | ELR не подменяет ISR; реальный reader не расширялся |
| Active conflicting reassignment | ACTIVE_REASSIGNMENT / CONFLICT | Чужой перенос не отменяется; после обнаружения новые пакеты не отправляются |
| Нет --manage-replication; decrease без отдельного разрешения; --no-delete | BLOCKED до RF Admin | writes = 0, нет configs/partition/ACL followup |
| Одновременные RF + partitions одного топика | Structural reject | До metadata calls |
| Исторический READY, затем UNKNOWN в свежем executor snapshot | PREFLIGHT_BLOCKED | writes = 0; synthetic evidence не передаётся production reader |
| Частичная отправка/future error | FAILED/UNKNOWN с per-partition results | Последующие пакеты и legacy followup остановлены; нет заявления об отсутствии изменений |
| Target replicas есть, но ISR/leader неполны | RUNNING, затем UNKNOWN при deadline | Отсутствие active entry само по себе не COMPLETED |
| Полный target/ISR, затем drift перед независимой verification | UNKNOWN, phase VERIFICATION | Общего CLI success нет, Admin закрыт |

Production defaults всё ещё не устанавливают RF application. Даже искусственный READY
на старом default-пути приводит к EXECUTION_NOT_IMPLEMENTED, exit 2. Явная внутренняя
интеграция использует строгий reader с UNKNOWN и тоже не допускает реальную отправку.
Положительные результаты получаются исключительно на test-only attested snapshots.

## 4. Выполнение, конкурентность и восстановление

Проверены RF **1→2, 2→3, 3→2, 3→1**, неоднородный RF, партиции с уже целевым RF,
несколько топиков/пакетов, exact target без перестановки сохранённых реплик.
Дополнительная проверка 24 перестановок Map/порядка наблюдаемых partitions подтверждает
одинаковый JSON результата planner. Семантически неупорядоченные Set не выбирают брокеров.

ACCEPTED означает подтверждение RPC, RUNNING — наблюдаемый перенос/неполный target ISR.
COMPLETED требует точного ordered target, target в ISR, допустимого leader и отсутствия active.
FAILED описывает определённый отказ; UNKNOWN сохраняет неопределённость, submission и lastKnownState.
При смешанном результате общий FAILED не означает отсутствие UNKNOWN/COMPLETED у отдельных партиций.

Проверены изменения между plan и preflight, между policy и pre-submit read, во время
отправки/ответа и после executor completion перед verification. Обнаруженный drift до
отправки останавливает запись; после отправки не формируется общий успех. Network loss,
RPC timeout, общий deadline и interrupt не отменяют принятые брокерами запросы.
Ошибки/timeout отдельных futures не скрывают подтверждённых соседних результатов.

При повторе после частичного выполнения завершённая партиция распознаётся OBSERVED,
отправляется только оставшаяся исходная партиция, после нового preflight. Внутренний executor
умеет наблюдать совместимый активный перенос без присвоения авторства/повторной отправки;
начальный application-preflight при active reassignment консервативно блокирует запуск.
Это различие не превращает чужую операцию в разрешение записи.

**TOCTOU не устранён.** Между последним чтением и alterPartitionReassignments нет CAS
по cluster/topic epoch, assignments, ISR, fencing, config или disk admission. Последовательные
чтения не являются атомарным снимком; drift может остаться не замечен до следующего read
или начаться сразу после terminal verification. Терминальный статус описывает наблюдение
во времени, не постоянную гарантию здоровья Kafka.

До production нужны подтверждённые capabilities/evidence, управление конкурирующими
операторами и проверка реального ADS. Долговечный execution journal по plan hash из Stage 6A
пока отсутствует: результаты живут в памяти/безопасном CLI summary. Это обязательное
ограничение для эксплуатационного recovery, не закрытое данными unit-тестами.

## 5. Регрессия Stage 1–5 и аудит логирования

Все **254 сценария Stage 5**, включая Stage 1–2, сопоставлены с текущими XML по именам
классов и количеству cases; совпадение полное. Также полностью сохранены 742 cases 6B3C.

| Функция | Проверка / результат |
| --- | --- |
| JAAS PLAIN/SCRAM/GSSAPI/OAuth, SSL passwords, неизвестные credentials | Старый allowlist и verbose logger policy сохранены; прежние config logging tests и новые error-path tests проходят |
| Kafka API exceptions, close/factory failure | Новые тесты raw secrets не находят; cause не передаётся в exception для CLI |
| PreviousValue, ADD/UPDATE/REMOVE, null/sensitive | PlanManager/LogUtil/JSON tests проходят; семантика config SET/DELETE не менялась |
| Старый JSON, schema/unknown operations, round-trip, filter | Все прежние JSON fixtures и модели совпали с HEAD по SHA-256; строгая валидация не отключалась |
| Partition increase, shrink reject, stale/retry | Прежние Partition* и KafkaPartitionServiceSpec полностью проходят; код алгоритмов не изменён |
| --no-delete, --skip-acls, --skip-topics, include/exclude | Прежние fake workflow/fixtures проходят; RF в плане не позволяет обойти глобальный барьер через ACL-only apply |
| Новые топики, ACL и отсутствие RF flags | Прежняя apply-семантика сохранена; safety-исправления меняют только ошибки и ранее игнорировавшийся command-config |
| Невалидные JSON/YAML/config files, JSON export | Не повторяются credentials/source fragments; четыре text error fixtures обновлены явно, без изменения JSON-контракта |

Новые исключения RF по-прежнему содержат enum/счётчики, а не Kafka Config/JAAS/пароли/токены.
Полные per-partition outcomes доступны внутреннему вызывающему коду, не добавляются в JSON plan.
Прогресс application не печатает raw Kafka responses. Политика ERROR/WARN сторонних
login/callback modules, JVM dumps и произвольная внешняя перенастройка loggers не контролируются
данными тестами. Broker/rack/topic labels — диагностические метаданные; хранить в них секреты нельзя.

## 6. Сборка и артефакты

Чистая сборка: Windows 11, Launcher JVM Temurin/Adoptium 21.0.13. Существующая привязка
compile/test toolchain к AZUL не менялась. Git Bash добавлен только в PATH процесса для build.sh.
Запуск выполнен `--offline --no-scan`, CI/DEBUG очищены, build scan не публиковался.

```powershell
.\gradlew.bat --gradle-user-home C:\Users\hellf\.gradle --offline --no-daemon --no-scan --console plain --warning-mode all --init-script .gradle/ads-replication-stage6b4/safe-unit.init.gradle --init-script .gradle/ads-kafka-upgrade-4c/inventory.init.gradle -PadsInventoryOutput=.gradle/ads-replication-stage6b4/runtime-inventory.json clean classes testClasses test shadowJar buildExecutableJar buildRelease adsKafkaInventory
```

`test` запускался только со строгим allowlist **33 unit Spec**, исключением
`**/*IntegrationSpec*`, failOnNoMatchingTests и проверкой, что ignoreFailures не включён.
IntegrationSpec не запускались; настоящие Kafka AdminClient и сетевые write API не создавались.

| Проверка | Результат |
| --- | --- |
| clean classes testClasses | SUCCESS |
| Unit tests | 814 / 814, 0 failures/errors/skips: 742 прежних, 41 diagnostic audit, 31 RF regression audit |
| shadowJar | SUCCESS, `build/libs/kafka-gitops-all.jar`, **23 855 963 байта** |
| buildExecutableJar | SUCCESS, `build/output/kafka-gitops`, **23 856 165 байт**, JAR-секция совпадает побайтно |
| buildRelease | SUCCESS, `build/distributions/kafka-gitops.zip`, **22 584 183 байта**, внутри тот же executable |
| CLI | **49** безопасных проверок: help/version, локальные validation/skipped workflows, старые/новые планы, permission отказ до metadata, verbose secrets; дополнены command-config/invalid YAML cases |
| Main-Class | `com.devshawn.kafka.gitops.MainCommand` сохранён |
| JAR | 6 695 классов, 53 native resources, 6 service descriptors; ни один старый entry не потерян, байты библиотек совпадают с baseline |
| Конфликты | Нет duplicate ZIP entries, пересекающихся runtime .class, archive traversal paths, старого org.lz4, test-only libraries или старого SLF4J binder; единственный provider — Logback |
| Classpaths | compile/runtime/testRuntime, annotationProcessor, jacocoAgent совпали с 6B3C по координатам, версиям и SHA-256 |
| Windows codecs/JNI | Из итогового JAR: LZ4 Java/JNI, Snappy JNI, Zstd JNI; 12 synthetic payloads, 60 array + 60 direct-buffer round trips, 36 локальных Kafka MemoryRecords batch round trips |

Codec smoke не создаёт Admin/producer/consumer и не отправляет records брокерам.
**Linux/JNI проверки не выполнялись**: наличие неизменённых ELF/native resources не
заменяет запуск на Linux/glibc/musl/другой архитектуре. Поддержка Linux не объявляется подтверждённой
результатом Windows теста. Shell executable также не проверялся как Linux-процесс.

SHA-256 Shadow JAR: `f8ce0353a2d40a6b768feea693f77d501878bac7864cfe4514585bda5107ba0d`.
После clean восстановлены прежние build/audit artifacts. Старые audit/design документы не менялись.
Локальные logs, allowlist, classpath inventory и verification JSON находятся в
`.gradle/ads-replication-stage6b4/`; они не добавлялись в Git.

Runtime остаётся из 14 артефактов: kafka-clients 3.9.2; at.yawk LZ4 1.14.0;
Snappy 1.1.10.12; Zstd 1.5.7-26; Jackson core/databind/yaml/jdk8 2.21.8 и annotations 2.21;
SnakeYAML 2.5; Picocli 4.7.7; SLF4J 2.0.20; Logback core/classic 1.6.6.
Annotations 2.21 — версия из неизменённого Jackson BOM, не случайное рассогласование модулей.

Остались warnings: Convention/JavaPluginConvention/ApplicationPluginConvention,
IDEA testSourceDirs, mainClassName, Groovy DSL group assignment (удаление этой формы — Gradle 10),
implicit test framework implementation loading и замена Gradle Enterprise на Develocity.
Javac предупреждает об устаревших константах Kafka в TopicConfigSecurity.
Ничего не отключалось для подавления warnings.

## 7. Матрица готовности к Stage 7

Это план допуска к будущим испытаниям, не разрешение выполнять их в данном этапе.
Стенд должен быть отдельным, одноразовые ресурсы — выделены явно; production ADS исключён.

| Группа | Сценарии | Условие / ограничение |
| --- | --- | --- |
| READY FOR TESTING | CLI/JSON/config loading, secret masking, old plans, previousValue, config ADD/UPDATE/REMOVE | Синтетические credentials и контролируемый сбор логов; unit tests не заменяют стендовые auth checks |
| READY FOR TESTING | Создание тестовых топиков, partition increase/retry, configs, ACL, prefixes/no-delete/skip flags без RF | Только разрешённые стендовые ресурсы, явная изоляция тестов, внешний лимит времени; legacy операции ещё используют get() без собственного общего deadline |
| READY FOR TESTING | Отказы RF без manage/decrease permission, no-delete | Проверять exit 2 и отсутствие writes; не включать внутренние synthetic READY providers |
| READ-ONLY ONLY | TLS/GSSAPI/PLAIN/SCRAM/OAuth подключение и describe metadata, RF preview, policy diagnostics, UNKNOWN отказ | Требуются стендовые разрешения на чтение; реальные данные не эквивалентны synthetic evidence |
| READ-ONLY ONLY | Наблюдение assignments/ISR/ELR/features/config/cordoning и чужого reassignment | Не вызывать executor/write через внутреннюю Java инъекцию; не отменять чужую операцию |
| BLOCKED | Любое реальное RF increase/decrease/mixed execution | Нужно надёжно подтвердить fencing, admission, capacity, прочие live предусловия; стандартный CLI остаётся закрытым |
| BLOCKED | Уменьшение RF при unknown/incomplete ISR, target ниже min ISR, без разрешения или при no-delete | Исправить состояние/план и получить независимые разрешения; запрет no-delete не обходится |
| BLOCKED | RF при ELR ENABLED/UNKNOWN | Нужна проверенная ELR-aware модель/policy и достоверная полнота данных; не делать вывод по пустому списку |
| BLOCKED | Автоматическое production recovery с принятыми/неизвестными операциями | Нужны execution journal/идентификация плана, operational concurrency control и отдельные failure/restart испытания |

Stage 7 может проверять существующие функции и read-only RF на строго ограниченном стенде.
Production RF и реальные положительные RF write trials текущей реализации не разрешены
одними результатами этого аудита. Предварительная vendor/security оценка ADS также обязательна.

## 8. Техническая карта Stage 8 после Stage 7

Ни Kafka Client 4.x, ни Gradle/Shadow 9.x здесь не подключались. Целевые patch-релизы
надо выбирать после Stage 7 по стабильным релизам, advisories и vendor matrix ADS.
Ссылки `/43/javadoc` описывают ветку API 4.3 с текущими patch-релизами, а не подтверждают
возможности конкретной сборки ADS 4.3.1 или достоверность её ответов.

| Место | Конкретное действие будущей миграции |
| --- | --- |
| `TopicConfigSecurity.SAFE_CONFIG_KEYS` | Удалённые в 4.3 `TopicConfig.MESSAGE_FORMAT_VERSION_CONFIG` и `MESSAGE_TIMESTAMP_DIFFERENCE_MAX_MS_CONFIG` заменить собственными legacy spelling constants для чтения старого JSON; современные before/after timestamp limits учитывать отдельно |
| `MESSAGE_DOWNCONVERSION_ENABLE_CONFIG` | В 4.3 ещё существует, deprecated/no-op; не объявлять удалённым compile blocker. [TopicConfig 4.3](https://kafka.apache.org/43/javadoc/org/apache/kafka/common/config/TopicConfig.html) |
| `KafkaService.describeTopics`, metadata reader, executor | Сохранить allTopicNames/topicNameValues; не возвращать удалённые aliases DescribeTopicsResult.all()/values(). Другие Result.values()/all() не удалять по аналогии. [DescribeTopicsResult 4.3](https://kafka.apache.org/43/javadoc/org/apache/kafka/clients/admin/DescribeTopicsResult.html) |
| `KafkaService.updateTopicConfig()` | Сохранить incrementalAlterConfigs. Старый Admin.alterConfigs удалён; createPartitions/alterPartitionReassignments/listPartitionReassignments не являются сами по себе compile blockers. AdminClient всё ещё существует. [Admin 4.3](https://kafka.apache.org/43/javadoc/org/apache/kafka/clients/admin/Admin.html) |
| `ReplicationMetadataReader.read()/nodes()` | Читать includeFencedBrokers(true), Node.isFenced и подтверждать поддержку/полноту ответа, не выводить VERIFIED из default false. [Options 4.3](https://kafka.apache.org/43/javadoc/org/apache/kafka/clients/admin/DescribeClusterOptions.html), [Node 4.3](https://kafka.apache.org/43/javadoc/org/apache/kafka/common/Node.html) |
| `ReplicationMetadataReader.uncordoned()` | Добавить LogDirDescription.isCordoned с проверкой доступности поля и состояния каталогов; сохранить safe projection без paths. [LogDirDescription 4.3](https://kafka.apache.org/43/javadoc/org/apache/kafka/clients/admin/LogDirDescription.html) |
| Snapshot/Evidence/Policy | Перенести ELR/lastKnownElr и признаки полноты/epoch в независимые наблюдения. Эти методы уже есть в 3.9.2; включённый ELR не становится безопасным от одной смены версии. [TopicPartitionInfo 3.9](https://kafka.apache.org/39/javadoc/org/apache/kafka/common/TopicPartitionInfo.html) |
| Domain keys / service adapters | По необходимости выделить свой topic/partition key вместо Kafka TopicPartition на границе Snapshot/ExecutionResult, не вводя Kafka API в pure planner. JSON schema 1 и saved assignments сохранить |
| Строгие fake Admin / Result fixtures | Перепроверить сигнатуры/доступность constructors/factories, включая используемый тестами DeleteTopicsResult.ofTopicNames; не делать фиктивные READY ради прохождения новой версии |

Расширения reader должны учитывать version negotiation и fallback: наличие нового getter
или успешный Admin RPC не доказывает, что broker передал достоверное поле. Даже 4.x сам
по себе не даёт placement admission, расчёта sufficient reserve, CAS/atomic snapshot,
durable journal или vendor security approval. Эти блокировки требуют отдельной реализации.

Для инфраструктуры после Stage 7:

- `build.gradle`: перейти от mainClassName к application.mainClass и убрать Convention/IDE
  API из цепочки org.inferred.processors 3.7.0; сохранить явные FreeBuilder annotationProcessor/
  compileOnly и проверить generated sources/IDE integration.
- `settings.gradle`: проверить поддерживаемые Develocity/Foojay, не допустить неявной публикации
  scan; проверить Sonar и остальные плагины на новой версии Gradle.
- Явно определить JUnit Platform launcher/engine и classpath/testClassesDirs отдельных Test
  tasks. Не отключать failOnNoMatchingTests и не запускать старые опасные specs по умолчанию.
  Gradle 9 удаляет Conventions и требует JVM 17+; Java 21 сохраняется.
  [Gradle 9 migration guide](https://docs.gradle.org/current/userguide/upgrading_major_version_9.html).
- Выбрать согласованную стабильную пару Gradle/Shadow 9.x, сохранить Main-Class/services/native
  packaging и проверять collisions/traversal. По опубликованной матрице Shadow 9.3+ требует
  Gradle 9.0+, 9.5+ — 9.2+; это требования совместимости, не выбор patch-релиза.
  [Shadow compatibility matrix](https://github.com/GradleUp/shadow#compatibility-matrix).
- Оценить снятие жёсткого AZUL vendor pin при сохранении Java 21; отдельно проверить
  Temurin/Windows и целевой Linux. В этом этапе pin не менялся.

Приёмка Stage 8: раздельная компиляция/контрактные fake tests 3.9.2 и выбранного 4.x,
старые JSON/CLI/configs, feature fallback/ELR/fencing/cordoning/ACL denial, races и restart;
затем разрешённый isolated ADS стенд с TLS/Kerberos/OAuth и контролем capacity/concurrency.
Для Gradle/Shadow — Windows/Linux packaging, ServiceLoader provider, JNI и byte comparisons.
Повторить CVE/GHSA/OSV/NVD/vendor audit всех runtime/build/test и embedded библиотек.

## 9. Оставшиеся риски и итоговая оценка Stage 6

1. **Обязательно до production RF:** достоверные fencing, placement admission и достаточный
   disk reserve; ELR-aware safety при включённом ELR; отдельное явное включение production
   binding только после выполнения всех предусловий. Force/unsafe обходов нет.
2. **Concurrency/recovery:** отсутствуют CAS и атомарность read→write, durable execution journal
   и эксплуатационная проверка восстановления. Принятые запросы могут продолжаться после
   timeout/interrupt/close. Автоматического rollback нет и не должно появляться неявно.
3. **Совместимость:** настоящий ADS, его ACL/Ranger, backports, auth modules, log placement и
   Linux JNI не проверены. Совместимость протокола 3.9.2↔4.3.1 этого не доказывает.
4. **Legacy IO:** восемь прежних методов всё ещё полагаются на Kafka timeout и get()/close()
   без единого application deadline. Исправлена диагностика/interrupt, не переработана
   асинхронная архитектура. Для Stage 7 нужен внешний timeout стендового harness.
5. **Зависимости:** старый [Stage 4 audit](../audit/audit-report-stage4-2026-11-09.md)
   не отменён. Его client/build/broker advisories и исключения требуют отдельной актуализации;
   нового полного CVE-сканирования здесь не было. Совпадение SHA не означает отсутствие CVE.

Stage 6 подтверждён на уровне моделей, JSON, детерминированного preview, строгой policy,
внутреннего executor и testable application orchestration. Исправлены подтверждённые
ошибки секретной диагностики и command-config. Production-ready и право положительного
реального RF исполнения **не подтверждены**; защитная блокировка сохранена.

## 10. Git-проверки

`git diff --check` выполнен для tracked изменений, новые файлы дополнительно проверены
через `git diff --no-index --check`; whitespace errors отсутствуют.
Версии зависимостей, Wrapper, Gradle/Shadow, JSON-модели/эталоны и прежние unit specs
защищены сравнением SHA-256 с исходным HEAD. Коммит и staging не выполнялись.

`git diff --stat` tracked: **10 файлов, 45 insertions, 53 deletions**.
Он не включает три новых файла (два Spec и этот отчёт). Полная статистика: **13 файлов,
965 insertions, 53 deletions**; детализация в `.gradle/ads-replication-stage6b4/complete-diff-stat.txt`.

Stage 7 и Stage 8 не начинались. Подключений к Kafka/ADS и реальных Kafka write API не было.
