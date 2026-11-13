<!-- SPDX-License-Identifier: Apache-2.0 -->
> Historical stage evidence: the dates, versions, hashes and test outcomes below describe that stage and artifact. Personal machine paths were anonymized for publication. See [0.4.0 preparation](../release/v0.4.0-release-preparation.md) for the current release status.

# Stage 6B3C — безопасная интеграция RF apply

Дата проверки: 2026-11-10. Ветка: `feature/ads-gitops`, исходный HEAD: `ec5a4bf`.
Перед работой рабочее дерево было чистым. Изучены проекты Stage 6A, 6B2, 6B3A и 6B3B;
модели 6B1, планирование и прежние 699 тестов присутствуют.

Реализован внутренний сквозной путь JSON → CLI → ApplyManager → preflight → executor
→ независимая проверка завершения. **Production RF остаётся заблокированным.**
Kafka Client 3.9.2 не позволяет текущему reader подтвердить все обязательные условия.
Синтетический READY используется исключительно в тестовых зависимостях; CLI, JSON
и переменные окружения не предоставляют способа подменить safety evidence.

## Изменённые файлы и методы

Все пути Java ниже относительно `src/main/java/com/devshawn/kafka/gitops/`.

| Файл | Изменение |
| --- | --- |
| `MainCommand.java` | Внутренние `StateManagerFactory` и конструктор для внедрения зависимостей; `createStateManager()`. Публичный конструктор сохраняет production-барьер |
| `cli/ApplyCommand.java` | `call()` создаёт StateManager через фабрику родительской команды; флаги и коды завершения сохранены |
| `StateManager.java` | Внутренний конструктор принимает RF application; `apply()` вызывает общий `ApplyManager.applyPlan()` |
| `manager/ApplyManager.java` | Новый `applyPlan()` координирует весь apply. Выделены `preflightPartitions()`, `applyTopicOperations()`, `applyAclOperations()`; прежние публичные методы остаются закрыты для RF |
| `service/ReplicationApplication.java` — новый | Контракт применения явных RF TopicPlan с независимыми Permissions и типизированным ExecutionResult |
| `service/AdminReplicationApplication.java` — новый | `apply()` владеет одной RF Admin-сессией, выполняет свежую policy-проверку, запускает executor и подтверждает завершение; `completed()` поддерживает безопасный повтор после достигнутого target |
| `service/AdminReassignmentExecutor.java` | `verifyCompleted()` выполняет независимое терминальное чтение; `category()` доступен адаптеру внутри пакета. Алгоритм отправки/наблюдения не изменён |
| `service/ReassignmentExecutor.java` | Добавлен `Submission.UNKNOWN` для утраты сведений о вызове; уточнён контракт production-блокировки. JSON-схема не затронута |
| `service/KafkaService.java` | `replicationApplication()` создаёт строгий адаптер с существующей фабрикой Admin и реальным reader. Стандартный CLI этот адаптер не устанавливает |
| `exception/ReplicationExecutionException.java` — новый | Сохраняет типизированные результаты всех партиций и фазу EXECUTION / VERIFICATION / FOLLOWUP / SESSION_CLOSE, выдаёт безопасный текст |
| `util/LogUtil.java` | Перегрузка `printTopicPreApply()` показывает подтверждённый RF как COMPLETED; прежний preview и маскирование сохранены |
| `src/test/groovy/com/devshawn/kafka/gitops/ReplicationApplicationWorkflowSpec.groovy` — новый | 43 сквозных изолированных сценария с настоящими JSON reader, Picocli и ApplyManager |
| `src/test/groovy/com/devshawn/kafka/gitops/service/ReplicationApplicationTestHarness.groovy` — новый | Строгий fake Admin с учётом открытия/закрытия, контролируемые часы, тестовый READY; отдельный путь неизменённого настоящего reader на fake Admin |
| `docs/design/stage6b3c-integration.md` — новый | Этот отчёт |

Существующие тесты и JSON-эталоны не изменены. Исходники моделей плана,
ReplicaAssignmentPlanner, ReplicationSafetyPolicy, ReplicationMetadataReader,
KafkaGitopsConfigLoader, зависимости и Gradle-конфигурация сохранены побайтно.

## Архитектура и порядок выполнения

1. PlanManager читает и валидирует JSON прежним механизмом. Неизвестные RF schema/action
   и конфликт partitionPlan + replicationPlan отклоняются до получения метаданных.
2. `ApplyManager.applyPlan()` проверяет permissions всего RF-набора, включая уменьшение
   и приоритет no-delete. Отдельные partitionPlan других топиков также проходят прежний
   preflight до возможной первой RF-записи.
3. При явно установленной внутренней зависимости `AdminReplicationApplication.apply()`
   повторяет структурную валидацию и permissions, открывает Admin и получает свежий
   снимок. Все RF-топики должны пройти `ReplicationSafetyPolicy.evaluateForExecution()`.
4. Только результаты READY становятся PreparedOperation. Executor получает ту же Admin,
   одни permissions и общий монотонный deadline, включающий начальный RF preflight.
   Он сохраняет повторные проверки перед каждой отправкой; исторический READY не даёт права записи.
5. Executor возвращает результат для каждой запрошенной партиции. Неполный/чужой набор
   результата либо потерянный ответ преобразуются в UNKNOWN с `Submission.UNKNOWN`.
   Это исключает ложное утверждение, что запросы не отправлялись.
6. После COMPLETED выполняется отдельное чтение cluster ID, topic UUID, числа партиций,
   ordered replicas, ISR, leader и активных reassignment той же Admin. Все target должны
   совпасть; лидер должен быть в target и ISR; активных операций не должно оставаться.
7. Admin закрывается владельцем ровно один раз с `Duration.ZERO`, в том числе при ошибке.
   Executor и metadata reader не создают и не закрывают клиентов. Ошибка close не маскирует
   первичную ошибку; после успешного RF она становится SESSION_CLOSE с ненулевым exit code.
8. Только после полного подтверждения RF и закрытия RF-сессии выполняются прежние topic
   configs, partition operations других топиков и ACL. Ошибка этого этапа даёт FOLLOWUP:
   RF уже мог завершиться, поэтому сообщения «ничего не изменилось» нет.

Владение едино для RF preflight/execution/verification. Прежние проверки партиций и
последующие операции KafkaService сохраняют свои жизненные циклы; весь apply не является
Kafka-транзакцией. Подтверждение не кешируется между вызовами applyTopics/applyAcls.

Стандартные конструкторы MainCommand/StateManager/ApplyManager сохраняют прежний
fail-closed путь. Даже поданный ему тестовый READY завершается EXECUTION_NOT_IMPLEMENTED.
Новая внутренняя точка доступна через Java-внедрение зависимостей; production-фабрика
KafkaService использует строгий reader и также блокируется при UNKNOWN. Нет новой
действующей CLI-команды, скрытого флага, JSON-признака доверия или автоматического opt-in.

## Матрица Kafka Client 3.9.2 и ADS Kafka 4.3.1

AVAILABLE означает доступность конкретного наблюдения при полном успешном ответе API,
а не безусловную достоверность данных или разрешение операции. Ошибки авторизации,
unsupported API, неполные ответы и null сохраняют UNKNOWN. Ответы не атомарны.
Реальный ADS не опрашивался; vendor backports и настройки кластера не подтверждены.

| Условие | Статус | Данные API и фактическое ограничение |
| --- | --- | --- |
| Broker fencing | UNKNOWN | В 3.9.2 нет `Node.isFenced()` и `DescribeClusterOptions.includeFencedBrokers()`. Наличие broker ID, доступность RPC и rack не доказывают unfenced. Reader сохраняет UNKNOWN. [Node 3.9](https://kafka.apache.org/39/javadoc/org/apache/kafka/common/Node.html) |
| Cordoned log directories | PARTIAL | Reader проверяет эффективный `cordoned.log.dirs` и повторяет чтение, если конфигурация доступна. В `LogDirDescription` 3.9.2 нет `isCordoned()`; пустой конфиг сам по себе не является полной проверкой назначения на конкретный каталог. [LogDirDescription 3.9](https://kafka.apache.org/39/javadoc/org/apache/kafka/clients/admin/LogDirDescription.html) |
| Placement admission | UNKNOWN | Текущий адаптер не имеет полного подтверждения допуска размещения на broker/log directory; успешные describe/ACL/reachable не заменяют его. Admissions остаются пустыми. Это вывод из возможностей текущего адаптера, а не утверждение об отсутствии любых механизмов у ADS |
| Наблюдаемое свободное место | PARTIAL | `usableBytes()`/`totalBytes()` — OptionalLong; reader сохраняет минимальное доступное место только при полном ответе исправных каталогов. Эти байты не доказывают достаточный резерв после копирования реплик. [LogDirDescription 3.9](https://kafka.apache.org/39/javadoc/org/apache/kafka/clients/admin/LogDirDescription.html) |
| Достаточный дисковый резерв | UNKNOWN | Нет достоверной оценки объёма переноса, роста данных, резерва и конкурирующих размещений. Положительное usableBytes не становится diskCapacitySufficient |
| ELR | PARTIAL | `describeFeatures()` даёт поддерживаемые/финализированные уровни и epoch; reader принимает лишь известный стабильный DISABLED. **В 3.9.2 уже существуют `TopicPartitionInfo.elr()` и `lastKnownElr()`**, но текущие Snapshot/reader не переносят эти поля. ENABLED, неизвестный уровень/epoch и дрейф блокируются. [FeatureMetadata 3.9](https://kafka.apache.org/39/javadoc/org/apache/kafka/clients/admin/FeatureMetadata.html), [TopicPartitionInfo 3.9](https://kafka.apache.org/39/javadoc/org/apache/kafka/common/TopicPartitionInfo.html) |
| Эффективный min.insync.replicas | AVAILABLE условно | `describeConfigs(topic)` и `ConfigEntry.value()` при разрешённом ненулевом nonsensitive значении. Повторное чтение проверяет дрейф. Неизвестное/некорректное значение блокирует уменьшение; null не заменяется выдуманным default. [ConfigEntry 3.9](https://kafka.apache.org/39/javadoc/org/apache/kafka/clients/admin/ConfigEntry.html) |
| ISR, ordered replicas, leader | AVAILABLE условно | `describeTopics()`/`TopicPartitionInfo`; UUID и число партиций проверяются отдельно, active reassignment — отдельным API. Известный ISR не подменяет ELR и не гарантирует отсутствие последующего дрейфа. [TopicPartitionInfo 3.9](https://kafka.apache.org/39/javadoc/org/apache/kafka/common/TopicPartitionInfo.html) |

Наличие ELR методов подтверждено также локальным `javap` по исходному Shadow JAR
с неизменёнными runtime-классами. Поэтому неверно относить их появление только к 4.x.
В [KafkaAdminClient 3.9.2](https://github.com/apache/kafka/blob/3.9.2/clients/src/main/java/org/apache/kafka/clients/admin/KafkaAdminClient.java)
есть путь DescribeTopicPartitions и fallback Metadata; fallback строит прежний вариант
TopicPartitionInfo. Пустые списки без сведений о полноте ответа не доказывают выключенный ELR.
Reader этого этапа не расширялся и более доверчивым не стал.

Протокольное пересечение Admin API со старшим брокером не подтверждает новые поля,
состояния fencing/cordoning, admission или vendor-specific ограничения. Для этого
нужны отдельные наблюдения и проверка возможностей ответа.

## Условия допуска и статусы CLI

Сохраняются независимые `--manage-replication`, `--allow-replication-decrease`,
приоритет `--no-delete`, строгая policy, запрет неявного RF и запрет RF + partitions
одного существующего топика. Создание новых топиков использует прежнюю семантику.
Нет `--force`, `--unsafe`, `--ignore-preflight`, отмены reassignment или rollback.

| Состояние | Результат |
| --- | --- |
| Permissions отсутствуют / no-delete запрещает decrease | exit 2 до открытия RF Admin, без topic/ACL writes |
| Preflight BLOCKED, включая находки UNKNOWN | exit 2 с причинами policy; executor не вызывается, никаких последующих writes |
| READY в стандартном production-пути | По-прежнему exit 2, EXECUTION_NOT_IMPLEMENTED; ложного успеха нет |
| READY во внутренней тестируемой интеграции | Свежие проверки → executor → независимое подтверждение; exit 0 только при полном COMPLETED и успешных последующих операциях |
| ACCEPTED / RUNNING / UNKNOWN / FAILED или частичное завершение | exit 2; типизированные исходы по всем партициям сохранены в исключении, последующие ресурсы не применяются |
| Потеря ответа / timeout / connection loss | Никакого повторного blind submit. После исчерпания deadline неподтверждённая отправка остаётся UNKNOWN; неначатые партиции могут стать NOT_SUBMITTED/FAILED |
| InterruptedException | Флаг прерывания восстановлен, exit 2, Admin закрыт, принятые операции не отменяются |
| Сбой независимой проверки / close / followup | exit 2 с отдельной фазой; сообщение предупреждает о возможных уже принятых изменениях |

Пример безопасной диагностики частичного выполнения:

```text
[INVALID] RF apply stopped: phase=EXECUTION, state=FAILED, completed=1/2, submission attempts=2, reasons=AUTHORIZATION, NONE. Requests may have been accepted or changes already observed. Inspect Kafka state before retrying; no automatic rollback.
```

Сообщение строится из enum, счётчиков и фиксированного текста. Исключение не сохраняет
raw сообщения/cause от Kafka, callback, close или followup. Полный ExecutionResult содержит
идентификаторы партиций, state/error/submission/lastKnownState, а не Kafka Config или credentials.
В JSON-план этот результат не добавляется. Секреты, JAAS, токены и пути log directories
не выводятся новой интеграцией. Прежняя безопасная сериализация и verbose logging не менялись.

После таймаута новый запуск заново читает реальное состояние. Уже полностью достигнутый
target распознаётся как OBSERVED без повторной отправки, но проходит глобальные safety
проверки. Активный/неподтверждённый reassignment на начальном preflight остаётся блокирующим;
возобновление во время активного переноса не обещается. Журнал отправок между процессами
в этом этапе не введён.

## Регрессия и сборка

Выполнены офлайн на Windows 11, Java 21, Gradle 8.14.6, Shadow 8.3.11.
Launcher JVM — Temurin/Adoptium 21.0.13; существующий toolchain vendor AZUL сохранён.
Для неизменённого `buildExecutableJar` только в PATH текущего процесса добавлен Git Bash.
CI/DEBUG очищены для процесса сборки; `--offline --no-scan`, build scan не публиковался.

```powershell
.\gradlew.bat --gradle-user-home "$env:USERPROFILE\.gradle" --offline --no-daemon --no-scan --console plain --warning-mode all --init-script .gradle/ads-replication-stage6b3c/safe-unit.init.gradle --init-script .gradle/ads-kafka-upgrade-4c/inventory.init.gradle -PadsInventoryOutput=.gradle/ads-replication-stage6b3c/runtime-inventory.json clean classes testClasses test shadowJar buildExecutableJar buildRelease adsKafkaInventory
```

`test` запускался только со строгим init allowlist из 31 unit Spec, с явным исключением
`**/*IntegrationSpec*`. Все новые тесты используют proxy fake Admin и in-memory KafkaService;
настоящий AdminClient не создавался. Часы executor управляемые; отдельный тест реального
reader сохраняет его собственный System.nanoTime и не допускает ожидания/исполнения.

| Проверка | Результат |
| --- | --- |
| `clean classes testClasses` | SUCCESS |
| 699 прежних тестов | Все прошли; набор классов и число сценариев сопоставлены с 6B3B |
| 43 новых сквозных сценария | Все прошли; итого 742, 0 failures/errors/skips |
| `shadowJar` | SUCCESS, `build/libs/kafka-gitops-all.jar`, 23 856 624 байта |
| `buildExecutableJar` | SUCCESS, `build/output/kafka-gitops`, 23 856 826 байт; JAR-секция побайтно совпадает с Shadow JAR |
| `buildRelease` | SUCCESS, `build/distributions/kafka-gitops.zip`, 22 584 863 байта; содержит тот же исполняемый файл |
| CLI smoke tests | 39 проверок: help/version, локальная validation, skipped workflows, прежние планы, permission guards и verbose secrets; все прошли без открытия Admin |
| Classpaths | compile/runtime/testRuntime, annotationProcessor и jacocoAgent совпадают с 6B3B по координатам, версиям и SHA-256 |
| Shadow JAR | 6 695 классов, 53 native resources, 6 service descriptors; Main-Class сохранён, единственный SLF4J provider — Logback; старые runtime entries и байты библиотек сохранены |
| Защищённые файлы | Зависимости, старые исходники вне разрешённого набора, все прежние тесты, JSON-эталоны и audit documents совпали по SHA-256; build/audit восстановлен после clean |
| Git | HEAD не изменён; `git diff --check` проходит |

Новые тесты покрывают RF 2→3 / 3→2, permissions/no-delete, fencing/admission/disk/ELR UNKNOWN,
недостаточный диск, cluster/topic drift, ISR/topology/config drift, active conflict,
блокировку всего RF-набора, частичные ответы, ACCEPTED/RUNNING/UNKNOWN/FAILED,
ложный COMPLETED, transport timeout/loss, общий deadline и безопасный повтор,
interrupt, ошибки close/followup, старый JSON без RF, неизвестные schema/action,
конфликт partitions+RF, stale partitionPlan другого топика, некорректный executor result,
skip-acls/both-skipped и отсутствие raw secrets. Строгий настоящий reader проверен
внутри полного CLI-пути: UNKNOWN не превращается в READY, writes отсутствуют.

Остаются прежние warnings: Gradle Convention/JavaPluginConvention/ApplicationPluginConvention,
`mainClassName`, IDEA `testSourceDirs`, Groovy DSL `group '...'`, автоматическая загрузка
test framework implementation, замена com.gradle.enterprise на com.gradle.develocity.
Компилятор предупреждает об устаревших Kafka API-константах в TopicConfigSecurity.
Конфигурация не менялась ради подавления предупреждений.

`git diff --stat` для восьми ранее отслеживаемых файлов: **119 insertions, 20 deletions**.
Он не учитывает шесть новых файлов, перечисленных выше; они намеренно не добавлялись
в index. Полная статистика с новыми файлами: **14 файлов, 1199 insertions, 20 deletions**, вычислена без staging.

## Конкретный план Stage 8 после Stage 7

Здесь миграция не выполнялась и клиент 4.x не подключался. Следующие действия требуют
отдельной задачи после Stage 7 и согласования с поддержкой ADS.

1. Выбрать поддерживаемую ADS стабильную 4.x с учётом security fixes, а не просто равенства
   версии брокера. Получить vendor matrix, metadata.version/features, подтверждение
   ELR режима, ACL/Ranger, log-directory/cordoning и источников placement admission,
   политики capacity/reserve и конкурентного администрирования.
2. Устранить два конкретных compile blockers в `util/TopicConfigSecurity.java`:
   `TopicConfig.MESSAGE_FORMAT_VERSION_CONFIG` и `MESSAGE_TIMESTAMP_DIFFERENCE_MAX_MS_CONFIG`
   отсутствуют в 4.3 API. Сохранить чтение legacy spellings собственными строковыми
   константами; новые timestamp limits — before/after. `MESSAGE_DOWNCONVERSION_ENABLE_CONFIG`
   пока существует как deprecated/no-op, не объявлять его удалённым.
   [TopicConfig 4.3](https://kafka.apache.org/43/javadoc/org/apache/kafka/common/config/TopicConfig.html).
3. Сохранить `allTopicNames()`/`topicNameValues()` вместо удалённых старых aliases
   `DescribeTopicsResult.all()/values()`. Текущий RF-код уже использует allTopicNames.
   Сохранить `incrementalAlterConfigs`, не возвращать удалённый `Admin.alterConfigs()`.
   Не путать это с действующими `values()/all()` других result-классов.
   [DescribeTopicsResult 4.3](https://kafka.apache.org/43/javadoc/org/apache/kafka/clients/admin/DescribeTopicsResult.html).
4. Расширить версионный Kafka metadata adapter: запросить fenced brokers через
   `DescribeClusterOptions.includeFencedBrokers(true)` и читать `Node.isFenced()`,
   учитывать полноту ответа/поддержку API и epoch/drift. Неполный ответ остаётся UNKNOWN.
   [Options 4.3](https://kafka.apache.org/43/javadoc/org/apache/kafka/clients/admin/DescribeClusterOptions.html),
   [Node 4.3](https://kafka.apache.org/43/javadoc/org/apache/kafka/common/Node.html).
5. Читать `LogDirDescription.isCordoned()` и сопоставлять состояние с допустимыми
   каталогами/размещением; не выводить пути в диагностику. Добавить отдельный надёжный
   источник admission и расчёт достаточного резерва: обновление клиента само их не создаёт.
   [LogDirDescription 4.3](https://kafka.apache.org/43/javadoc/org/apache/kafka/clients/admin/LogDirDescription.html).
6. Расширить независимые Snapshot/Policy наблюдениями ELR/lastKnownElr и полнотой их
   получения. Эти методы есть уже в 3.9.2; задача состоит в корректном переносе/проверке
   данных, а не в механической замене dependency. Проверить enabled/disabled/unknown,
   смену feature epoch, fallback и неполные ответы; не ослаблять minISR/ISR checks.
7. Сохранить Kafka Admin только в service adapters. Ранее существующие Snapshot и
   ExecutionResult используют Kafka `TopicPartition` как ключ: при дальнейшем отделении
   домена ввести собственный ключ topic/partition и преобразовывать на границе.
   Pure ReplicaAssignmentPlanner не должен получать Kafka Node/Config/Admin.
   Сохранить существующий JSON schemaVersion и чтение старых планов.
8. В отдельном разрешённом этапе организовать раздельные сборки/тесты 3.9.2 и выбранного
   4.x через временный init script или отдельный адаптер, с прежними JSON и fake contract
   tests. Затем — отдельные согласованные стендовые тесты ADS: TLS, SASL/GSSAPI, PLAIN,
   SCRAM, OAuth callbacks, Java 21, timeouts, ACL, mixed-version и concurrency.
9. Только после подтверждения всех обязательных фактов явно подключать адаптер в
   production construction. Сохранить fail-closed, permissions, no-delete и запрет
   автоматического rollback; не использовать версию клиента как свидетельство READY.

## Оставшиеся ограничения

Production RF не готов к включению: fencing, placement admission и достаточный дисковый
резерв не подтверждены; ELR ENABLED/UNKNOWN блокируется. Реальная совместимость с ADS
4.3.1, авторизация и vendor-specific поведения без подключения не проверены.
Снимки и последний read не дают compare-and-set с reassignment; конкурентные операции
могут начаться между проверкой и записью. Безусловной атомарности всего apply нет.

Уже принятый reassignment может продолжаться после deadline, interrupt или закрытия
Admin. При ошибке проверяется состояние Kafka и заново строится/проверяется план;
автоматического rollback нет. In-memory diagnostics не заменяют долговечный execution
journal. Прежние security/audit выводы зависимостей не пересканировались и не отменены.

Утечки из новых обработчиков покрыты тестами; глобальная перенастройка логгеров JVM,
сторонние login/callback modules, crash dump и внешние конфигурации вне их контроля.
Никакие реальные подключения или Kafka writes, интеграционные тесты и коммиты не выполнялись.
Stage 6B4 и Stage 8 не начинались.
