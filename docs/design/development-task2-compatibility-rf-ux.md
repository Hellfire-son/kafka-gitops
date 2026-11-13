<!-- SPDX-License-Identifier: Apache-2.0 -->
> Historical stage evidence: the dates, versions, hashes and test outcomes below describe that stage and artifact. Personal machine paths were anonymized for publication. See [0.4.0 preparation](../release/v0.4.0-release-preparation.md) for the current release status.

# Задание 2: Kafka compatibility, RF и CLI UX

Дата: 12.11.2026 (Europe/Moscow). Ветка: `feature/ads-gitops`.
Исходный HEAD: `cc9c54876a5be986ab9f758e2c548d7748b3dad0`.
Выполнено только Задание 2. Коммит не создавался.

## Результат и границы доказательств

RF включён по умолчанию в CLI `plan` и `apply`. Планирование читает состояние,
показывает известные изменения и причины ограничений. Заблокированный отчёт
не экспортируется как обычный исполняемый JSON. Apply использует существующие
reader → safety policy → executor, заново проверяет разрешения и metadata,
дожидается `COMPLETED` и независимой проверки результата.

Исходные **879** тестов воспроизведены до изменений. Итог: **929**, failures/errors/
skipped **0**; прирост **50** сценариев. Число сценариев в каждой исходной test suite
сохранено либо увеличено. Ожидания старых тестов скорректированы там, где задание
намеренно меняет default RF, источник конфигурации и трактовку доказательств.
Ни один прежний безопасный сценарий не отключён. `*IntegrationSpec*` исключены
существующей настройкой Gradle; Docker, реальные Kafka/ADS и `ccloud` не запускались.
Все Admin-вызовы в тестах используют fake/proxy; их счётчики writes относятся
только к памяти тестового процесса.

Java 21, Gradle 9.9.1, Shadow 9.6.1, Kafka Client 4.4.0, Jackson 3.3.0 и все
координаты зависимостей сохранены. Это одна универсальная сборка; live-совместимость
с перечисленными ниже брокерами **NOT_TESTED**.

## Почему прежний RF-план исключал брокеров

Проверена реализация, включая `ReplicationMetadataReader`,
`ReplicaAssignmentPlanner`, `ReplicationSafetyPolicy`, применение и тесты.
Прежний reader всегда выставлял fencing `UNKNOWN`, трактовал отсутствие
`cordoned.log.dirs` как `UNKNOWN`, а planner требовал положительный `usableBytes`
даже от старого ответа DescribeLogDirs, где этого поля нет. Отдельно preflight
не имел источника generic placement admission и достаточного запаса под копирование:
его пустые admissions делали применение невозможным независимо от остальных фактов.
Обычная CLI-конструкция также не связывала существующее RF-приложение с executor.
Проверка итогового target теперь распространяется и на сохранённые старые реплики:
cordoned, неизвестный критический статус или известное отсутствие места не могут
попасть в экспортируемый target через сохранение previous replicas.

Воспроизведён случай `acl-test`: четыре partitions с replicas/leader/ISR на broker 0,
broker 0/rack 1 и broker 1/rack 2, RF 1 → 2, `max.message.bytes` 1048588 → 10485880.
Fake Admin возвращает эти сведения без обращения к указанным в задании адресам.
При положительном доказательстве отсутствия cordoning/ELR, здоровых log directories
и полном ISR расчёт даёт `[0, 1]` для каждой partition. Если оставить только факты
«два брокера отвечают» и «известен log.dirs», допустимость размещения **не доказана**:
получается критический `UNKNOWN`, объяснение по каждому брокеру и отказ экспорта.
Отсутствие динамических overrides тоже не доказывает effective config или eligibility.

## Возможности Kafka 3.2–4.4

Версия клиента и ответ ApiVersions не считаются точной версией сервера. Решения
принимаются по контрактам API, полным effective configs и feature metadata.

| Возможность | Kafka 3.2.x / 3.7.1 / 3.9.x | ADS 4.3.1 / Kafka 4.4.x | Решение реализации |
|---|---|---|---|
| Cluster ID, topic UUID, replicas, leader/ISR | Базовые Admin API | Те же API | Обязательные факты; неполнота блокирует |
| Topics/configs/ACL, CreatePartitions, incrementalAlterConfigs | Используются API, доступные в целевом диапазоне | Те же операции | Без обязательных новых 4.x options; ACL-профили не менялись |
| DescribeTopics | Переговоры протокола и fallback внутри Kafka Client | Клиент выбирает доступный API | Нет самостоятельного определения release по одному диапазону |
| List/AlterPartitionReassignments | Стандартные API | Стандартные API | Никакой эмуляции конфликта или успешного завершения |
| Broker fencing | Default DescribeCluster broker endpoint исключает fenced brokers; ZooKeeper отдельно подтверждается configs | Тот же контракт, новое поле учитывается | `includeFencedBrokers=true` не требуется; явный fenced блокирует |
| `cordoned.log.dirs` | Возможна положительная неприменимость по metadata level либо ZooKeeper configs | Проверяется явно; ADS backports не предполагаются | Отсутствие поля само по себе = UNKNOWN |
| ELR | Возможна положительная неприменимость по feature level либо ZooKeeper | Проверяется finalized feature и epoch | Включённый ELR текущим RF-алгоритмом не поддерживается |
| Rack | Опциональное поле Node | То же | RACK_AWARE требует известные racks; BROKER_ONLY выбирается явно |
| Здоровье log directories | DescribeLogDirs | DescribeLogDirs | Полный успешный ответ обязателен; путь из конфигурации его не заменяет |
| `usableBytes` | Старые ответы могут не содержать поле | Учитывается, если получено | Отсутствие = UNKNOWN, а не 0; известный 0 = UNSAFE |
| Generic placement admission / запас под копирование | Отдельного read-only доказательства произвольной server policy нет | Та же граница Admin evidence | Только конкретный некритический риск может стать WARNING |

Положительные доказательства `NOT_APPLICABLE`:

- Полные несекретные broker configs: `process.roles` явно пуст и
  `zookeeper.connect` непуст. Значения подключения в snapshot/отчёт не переносятся.
- Положительная верхняя граница `metadata.version` ниже первого feature level:
  ELR — **23** (`4.0-IV1`), cordoned log directories — **30** (`4.3-IV0`).
  Finalized level приоритетен; более низкий supported bound не маскирует
  противоречащий finalized level. Это граница возможности, а не определение release.
- Для cordoning явный effective config приоритетнее такого вывода:
  непустое значение всегда UNSAFE, sensitive/null остаётся UNKNOWN.

Первичные источники: [KIP-1073: fenced brokers](https://cwiki.apache.org/confluence/pages/viewpage.action?pageId=327979279),
[KIP-1066: cordoning](https://cwiki.apache.org/confluence/spaces/KAFKA/pages/311627566/KIP-1066%2BMechanism%2Bto%2Bcordon%2Bbrokers%2Band%2Blog%2Bdirectories),
[Kafka 4.4 MetadataVersion](https://raw.githubusercontent.com/apache/kafka/4.4.0/server-common/src/main/java/org/apache/kafka/server/common/MetadataVersion.java),
[ELR operations](https://kafka.apache.org/40/operations/eligible-leader-replicas/).
Диапазоны базовых API также исследованы в [аудите Stage 1](../audit/modernization-stage1-2026-11-11.md).

| Цель одной сборки | Что проверено здесь | Что не подтверждено |
|---|---|---|
| Kafka 3.2.x | Старые формы metadata/log directories через fake Admin; отсутствие обязательных новых 4.x options | Реальный ZooKeeper/KRaft, protocol negotiation, права и RF execution |
| Kafka 3.7.1 | Те же контракты и положительные feature-bound proofs | Реальный broker |
| Kafka 3.9.x | Те же контракты | Реальный broker |
| ADS Kafka 4.3.1 | Стандартные Admin-контракты без предположений о vendor backports | Реальные ADS features, policies и security |
| Kafka 4.4.x | Компиляция с Client 4.4.0 и fakes новых полей/опасных условий | Реальный broker; ELR-enabled RF остаётся заблокирован |

В частности, старый KRaft без доступной положительной feature-level информации
может остаться BLOCKED: имя release не подставляет отсутствующее доказательство.
Это консервативное ограничение, а не заявление о проверенной полной поддержке.

## SUPPORTED / NOT_APPLICABLE / UNKNOWN / UNSAFE

`CapabilityAssessment` содержит state, стабильную reason и признак critical:

| Состояние | Значение | Последствие |
|---|---|---|
| SUPPORTED | Возможность применима, данные получены | Используется конкретное наблюдение; это не общее разрешение write |
| NOT_APPLICABLE | Есть положительное доказательство неприменимости | Принимается только с допустимой причиной и повторной проверкой |
| UNKNOWN | Применимость/состояние надёжно не установлены | Critical блокирует; обоснованный дополнительный риск предупреждает |
| UNSAFE | Получен опасный факт | Блокирует соответствующее размещение/операцию |

Для broker выводятся rack, storage, fencing, cordoning, usableBytes и все reason codes.
Неизвестные значения не заменяются нулями. Preview-кандидат не является apply READY.
Никаких `--force` / `--unsafe` не добавлено.

| Проверка | Класс | Поведение apply |
|---|---|---|
| Cluster ID, topic UUID, partition IDs/count, ordered previous replicas | Обязательный инвариант | Drift/неполнота блокирует |
| Target RF, уникальные broker IDs, структура и algorithm/schema | Обязательный инвариант | Неверный saved JSON отклоняется |
| Leader в replicas/ISR, полный ISR, сохранение leader при decrease | Обязательный инвариант | Degraded/unknown блокирует |
| Effective min.insync.replicas, RF ≥ min ISR, стабильность configs | Обязательный инвариант | Неизвестность/изменение/конфликт блокирует |
| Reachability, fencing, здоровые log directories | Обязательный инвариант | UNKNOWN/опасный факт блокирует |
| Cordoning и повторяемость доказательства | Обязательный инвариант для target | UNKNOWN/cordoned/изменение блокирует |
| ELR и стабильность features | Обязательный инвариант | UNKNOWN, включённый ELR и drift блокируют |
| Реальный активный reassignment / свежесть snapshot | Обязательный инвариант | Конфликт/неизвестность/устаревание блокирует |
| CLI manage/decrease/no-delete | Независимое разрешение | Проверяется заново, до любых writes |
| Rack при RACK_AWARE | Обязательное условие выбранной политики | Неполнота блокирует; явный BROKER_ONLY даёт предупреждение |
| Generic read-only placement admission | Дополнительная оценка риска только с typed reason `NO_READ_ONLY_PLACEMENT_ADMISSION_API` | UNKNOWN → `PLACEMENT_ADMISSION_NOT_PROVEN`; известный запрет блокирует |
| Объём копирования, рост, concurrent placement, disk reserve | Дополнительная оценка риска только с typed noncritical capacity evidence | UNKNOWN → `DISK_RESERVE_NOT_PROVEN`; известное отсутствие места/отказ блокирует |

Положительный usableBytes не доказывает достаточность места под transfer. Отсутствие
usableBytes не заменяет проверку здоровья storage. Для legacy/internal fake providers
без typed capability evidence прежние строгие blockers admission/capacity сохранены.
Все UNKNOWN в READY не переводятся; также нет общего запрета за любой UNKNOWN.

## CLI и RF

| Вызов / флаг | Поведение |
|---|---|
| `plan` | RF из YAML учитывается по умолчанию, включая decrease |
| `--manage-replication` | Сохранённый deprecated alias включённого default |
| `--no-manage-replication` | Явно исключает RF; drift виден как WARNING |
| Оба противоположных флага | `RF_OPTION_CONFLICT`, exit 2 до создания StateManager/чтения Kafka |
| `plan --allow-replication-decrease` | Compatibility option, для показа decrease не требуется |
| `apply --allow-replication-decrease` | Свежая независимая авторизация decrease |
| Глобальный `--no-delete` | Запрещает decrease даже при allow; plan его всё равно показывает |
| `--replication-rack-policy RACK_AWARE` | Default; нужны подтверждённые racks |
| `--replication-rack-policy BROKER_ONLY` | Явное принятие риска отсутствия rack-защиты |

Отключение RF вместе с другими RF-specific options отклоняется как конфликт.
CLI help описывает default, alias, opt-out, rack policy и независимое decrease
permission. `plan --help` и `apply --help` поддерживаются с exit 0.
Default Java `ManagerConfig.Builder` оставлен прежним для embedded callers;
новую CLI-семантику устанавливают команды.

```bash
java -jar build/libs/kafka-gitops-all.jar -c client.properties plan -o plan.json
java -jar build/libs/kafka-gitops-all.jar -c client.properties plan --no-manage-replication
java -jar build/libs/kafka-gitops-all.jar -c client.properties apply -p plan.json
java -jar build/libs/kafka-gitops-all.jar -c client.properties apply -p decrease.json --allow-replication-decrease
```

Эти команды — примеры для будущего стенда; в этой работе реальные подключения
и изменения не выполнялись. `plan` не вызывает topic/config/partition/reassignment/
ACL writes и не запускает apply автоматически.

## Как выглядит plan

Ниже сокращённые **синтетические** примеры. Сначала известные факты/изменения,
затем WARNING, затем BLOCKED и SUMMARY. Статус предварительных assignments
явно ограничен будущим fresh preflight. Resource count считает ресурсы, а не
число partition assignments или config keys.

```text
[PLAN] Topic: acl-test
  Partitions: 4 -> 4
  Replication: [1] -> 2
  Current ordered assignments:
    Partition 0: [0]
    Partition 1: [0]
    Partition 2: [0]
    Partition 3: [0]
  Proposed ordered assignments (subject to fresh apply preflight):
    Partition 0: [0, 1]
    Partition 1: [0, 1]
    Partition 2: [0, 1]
    Partition 3: [0, 1]
  Config max.message.bytes: 1048588 -> 10485880 [UPDATE]
  Broker 1: ELIGIBLE_FOR_PREVIEW, rack=2, storage=VERIFIED, fencing=VERIFIED, cordoning=NOT_APPLICABLE, usableBytes=UNKNOWN
    CORDONING: NOT_APPLICABLE (METADATA_LEVEL_BELOW_CORDONING)
    DISK_CAPACITY: UNKNOWN (USABLE_BYTES_NOT_REPORTED)
[WARNING] USABLE_BYTES_NOT_REPORTED Resource: acl-test/broker-1
  Actual: UNKNOWN; required: DISK_CAPACITY; state: UNKNOWN
  Reason: This additional operational risk is not established by the available Admin evidence.
  Action: Review broker capacity/admission policy; apply still rechecks all critical invariants.
[SUMMARY]
  Proposed executable resource changes: 1
  Status: PLAN_BUILT
  Ready for apply: FRESH_PREFLIGHT_REQUIRED
```

```text
[PLAN] Topic: acl-test
  Partitions: 4 -> 4
  Replication: [1] -> 2
  Proposed assignments: NOT_CALCULATED (see diagnostics)
  Config max.message.bytes: 1048588 -> 10485880 [UPDATE]
  Broker 1: EXCLUDED, rack=2, storage=VERIFIED, fencing=VERIFIED, cordoning=UNKNOWN, usableBytes=UNKNOWN
    CORDONING: UNKNOWN (CAPABILITY_NOT_ESTABLISHED)
[BLOCKED] INSUFFICIENT_BROKERS Resource: acl-test
  Actual: Discovered=2, eligible=1; required: RF=2; state: BLOCKED
  Reason: Too few verified eligible brokers for the target RF; UNKNOWN, unavailable or cordoned brokers cannot be used.
  Action: Resolve this condition and re-plan; no writes were sent.
[SUMMARY]
  Proposed executable resource changes: 1
  Status: PLAN_BLOCKED
  Ready for apply: NO
  Executable JSON: NOT_WRITTEN; any existing output file is unchanged.
```

Во втором примере ELR отдельно известен disabled; если его режим тоже неизвестен,
добавляется `ELR_MODE_UNKNOWN`, а при enabled — `ELR_UNSUPPORTED`.
Не рассчитываем assignments, которым мешает критическая неопределённость.
При ошибке получения всего metadata показываются desired values и UNKNOWN actual.
При RF/partition конфликте или несовместимом config изменении сохраняются уже
известные сведения, но итог остаётся BLOCKED.
`TARGET_BROKER_INELIGIBLE` отдельно указывает IDs исключённых брокеров, которые
алгоритм сохранил бы из прежнего назначения; такие assignments не экспортируются.

## Подключение, ожидания и секреты

Прежнее ожидание происходило на unbounded `KafkaFuture.get()` в KafkaService,
после парсинга YAML. Последняя строка `Parsing desired state file...` не указывала
на причину. Скрытый bootstrap `localhost:9092` удалён. При отсутствии bootstrap
`BOOTSTRAP_MISSING` возвращается до `AdminClient.create`; явно заданный localhost
допускается. Локальный validate не требует bootstrap.

Этапы: `[CONFIG]` загрузка properties, `[CONNECT]` начало получения данных,
`[METADATA]` чтение, `[PLAN]` расчёт/сводка. CONNECT — сообщение о начале,
а не утверждение об уже успешном соединении. В production wrapper все восемь
прежних общих Admin-future ожиданий ограничены deadline. Planning имеет единый
бюджет; RF reader дополнительно ограничивает весь свой snapshot. Нет бесконечных
retries. Default `default.api.timeout.ms=30000`, `request.timeout.ms=min(10000,
default.api.timeout.ms)` добавляются только при отсутствии соответствующих settings;
положительные пользовательские timeout сохраняются. Admin закрывается с Duration.ZERO.

| Code | Значение / действие |
|---|---|
| BOOTSTRAP_MISSING | Не задан bootstrap; указать properties/env |
| COMMAND_CONFIG_UNREADABLE | Файл отсутствует, недоступен или некорректен; исправить файл |
| DNS_FAILURE | Тип UnknownHostException; проверить DNS/bootstrap host |
| CONNECTION_REFUSED | Тип ConnectException; проверить listener/доступность |
| KAFKA_UNAVAILABLE | NetworkException/BrokerNotAvailableException |
| AUTHENTICATION_FAILED | SASL/другая authentication error; проверить механизм и credentials |
| SSL_HANDSHAKE_FAILED | SSL authentication/handshake error; проверить certificates/trust |
| AUTHORIZATION_FAILED | Недостаточно Kafka ACL; автоматические права не добавляются |
| TIMEOUT | Истёк API/общий deadline; проверить доступность и timeout |
| INTERRUPTED | Прерывание, interrupt flag восстанавливается |
| INVALID_TIMEOUT / METADATA_INVALID / KAFKA_ERROR | Некорректная настройка, metadata либо неклассифицированная ошибка |

Классификация выполняется по типам и ограниченной цепочке причин, без публикации
сырых exception messages/causes. Kafka Client может свести сетевую причину к timeout;
в этом случае выводится TIMEOUT, а не выдуманный DNS/refused. Имена хостов, paths,
пароли, JAAS, keytabs и полный Config не попадают в RF snapshot/ошибки. Диагностический
allowlist loader ограничен bootstrap/client ID/protocol/mechanism. Sensitive topic
configs скрываются в обоих значениях text plan. Прерывание не теряется при закрытии.

## Приоритет command-config

Порядок: raw `KAFKA_*` defaults → properties явно заданного файла → client.id default,
если всё ещё отсутствует. Файл выигрывает для каждого указанного в нём свойства;
environment продолжает заполнять отсутствующие свойства. Ошибка чтения файла
не приводит к fallback. Env username/password shortcuts применяются после merge,
только когда inline JAAS ещё нет и протокол/механизм допускает shortcuts.
PLAINTEXT/SSL не требуют SASL; GSSAPI может использовать внешний JVM JAAS/keytab
без подстановки username/password из постороннего environment.

Loader-тесты покрывают PLAINTEXT, SSL, SASL_PLAINTEXT, SASL_SSL, PLAIN и GSSAPI
с конфликтующим environment, без чтения реальных сертификатов/ключей и без login.
SCRAM helpers сохранены. Проверка properties не является реальным TLS/Kerberos
handshake; это остаётся предметом будущей интеграционной приёмки.

## Apply и совместимость JSON

Схема executable JSON не менялась: сохранены schemaVersion, previousReplicas,
targetReplicas, previousValue, прежние actions и strict validation. `PlanningReport`
и capability reasons существуют только в памяти/текстовом отчёте, разрешения
в JSON не сериализуются. Старые fixtures проходят прежние read/round-trip тесты
и отдельные JAR-smoke чтения с обеими skip-категориями.

Permission gate отвергает RF JSON вместе с `--no-manage-replication` до любых
операций, включая случай `--skip-topics --skip-acls` и no-change fast path.
Decrease/no-delete проверяются независимо на каждом apply. Fresh preflight
проверяет весь RF scope до первой записи; executor повторяет необходимые проверки
между batch, наблюдает состояние и независимо подтверждает результат.
`ACCEPTED`/`RUNNING` не считаются успехом. После неуспеха нет автоматического
rollback, нет пересчёта или изменения сохранённых target assignments.

Обычная production-конструкция StateManager теперь лениво связывает существующее
`AdminReplicationApplication`: создание объекта и plan не создают write session.
Тесты с реальным reader/policy/executor поверх fake Admin проверяют успешное
завершение перед follow-up config writes и отказ без единого write при неизвестном
критическом cordoning. Старые injection seams сохраняют закрытое поведение,
если приложение явно не предоставлено.

Заблокированный report отвергается как на CLI, так и в обычном JSON writer.
Существующий output file остаётся нетронутым. Старый JSON никогда не заменяет
fresh metadata или новые CLI permissions.

## Exit codes и автоматизация

Сохранена совместимость: **0** — успешный `NO_CHANGES`, `PLAN_BUILT`, apply/validate/help;
**2** — blocked planning, connection/config failure, failed preflight/apply.
Различия внутри группы ошибок выражены стабильными codes, scope, actual/required,
state, reason и action. No-change и построенный план различаются SUMMARY status.
Не введены новые несовместимые числовые codes; прочие исторические parser exits
вне этих команд отдельно не перепроектировались.

Риски миграции скриптов:

1. Ранее RF drift игнорировался без manage-флага. Теперь он создаёт RF operation
   и требует metadata/прав. Для прежнего поведения нужен явный opt-out при plan.
2. Обычный apply теперь действительно выполняет разрешённый RF план через safety
   pipeline; dry-run обеспечивается командой plan. Уменьшения требуют разрешения.
3. Text output изменился: нельзя полагаться на старые строки/порядок/symbols;
   для machine input используется существующий JSON, exit code проверяется всегда.
4. При BLOCKED старый output file сохраняется: не применять его автоматически
   как результат провалившегося plan. Желательны уникальные пути для CI run.
5. Command-config теперь выигрывает у environment; скрытого localhost больше нет.
6. Дополнительные capacity/admission warnings не являются гарантией достаточного
   места или отсутствия custom policy. Критические UNKNOWN сохраняют отказ.

ACL READ/WRITE/CREATE, существующий opt-in DESCRIBE и профили не изменялись;
новые профили и автоматический DESCRIBE не добавлены.

## Проверки и артефакты

Выполнена финальная offline-команда:

```powershell
.\gradlew.bat --gradle-user-home "$env:USERPROFILE\.gradle" --offline --no-daemon --no-scan --console plain --warning-mode all clean classes testClasses test shadowJar buildExecutableJar buildRelease
```

Все перечисленные задачи успешны: **BUILD SUCCESSFUL, 27s, 9 executed tasks**.
Есть прежнее javac deprecation notice для `TopicConfigSecurity`; опасные
integration suites не запускались. Исходный и итоговый test counts прочитаны
из XML, а не из отсутствия строки об ошибке. Новые tests покрывают RF flags,
decrease/no-delete, read-only planning, known/unknown capability evidence,
двух брокеров, partial diagnostics, export guard, production binding,
connection categories, timeout/interrupt, config precedence и GSSAPI.

Дополнительно проверены локальные JAR CLI help/version/validate, no-bootstrap,
конфликты флагов, malformed/missing properties, no changes, старые JSON и ранний
отказ RF-disabled JSON. Никакой smoke с недоступным адресом не создаёт реальный
Admin: сетевые отказы воспроизводятся исключительно fake futures.

Итоговый `build/libs/kafka-gitops-all.jar`: **25 830 563 bytes (24.63 MiB)**.
SHA-256: `f3b4f816ef3ae4874abc2ee21a05cc7fca3303c26bfd9652dc64700377e0ad34`.

| Проверка артефакта | Результат |
|---|---|
| Runtime dependency JARs | 13, координаты/хеши неизменны относительно Stage 1 |
| ZIP entries / class entries | 8034 / 7286 |
| Duplicate entries / duplicate runtime classes | 0 / 0 |
| Native resources / merged service descriptors | 53 / 5 |
| Main-Class | `com.devshawn.kafka.gitops.MainCommand` |
| Manifest | Multi-Release=true, нет внешнего Class-Path |
| Application/effective Java 21 bytecode | major 65; MR entries 22/23 игнорируются JVM 21 |
| Kafka clients | Только 4.4.0, сверено kafka-version.properties и runtime classes |
| Runtime class/native/pom contents | Побайтово совпадают с 13 dependency JARs, services объединены корректно |
| Test libraries / Jackson 2 core-databind | Не попали в fat JAR; Jackson 2 annotations сохранены как зависимость Jackson 3 |
| Executable/release | Shell-prefixed executable содержит тот же JAR; release ZIP содержит этот executable |
| Локальный CLI smoke | 18 проверок, все прошли |
| Codec/JNI | 59 unit tests; отдельно 156 round trips: 60 arrays, 60 direct buffers, 36 Kafka record batches |

LZ4, Snappy и Zstd native backends реально загрузились на текущей Windows/JDK21.
Runtime: Kafka 4.4.0; Jackson core/databind/YAML 3.3.0 + annotations 2.23;
SnakeYAML engine 3.2; Picocli 4.7.7; SLF4J 2.0.20 + Logback classic/core 1.6.6;
lz4-java 1.14.0; snappy-java 1.1.10.12; zstd-jni 1.5.7-26.

Локальные воспроизводимые evidence находятся в `.gradle/development-task2/`:
`baseline.log`, `baseline-tests.json`, `final-build.log`, `verify.py`,
`verification.json`, CLI logs и `codec-smoke.log` (каталог игнорируется Git).

## Изменённые файлы

Задание меняет **39 файлов: 29 существующих + 10 новых**. Из них 22 production
Java, 13 test Groovy и 4 documentation. Список новых/изменённых файлов:

```text
README.md
docs/documentation.md
docs/quick-start.md
docs/design/development-task2-compatibility-rf-ux.md (new)
src/main/java/com/devshawn/kafka/gitops/StateManager.java
src/main/java/com/devshawn/kafka/gitops/cli/ApplyCommand.java
src/main/java/com/devshawn/kafka/gitops/cli/PlanCommand.java
src/main/java/com/devshawn/kafka/gitops/config/KafkaGitopsConfigLoader.java
src/main/java/com/devshawn/kafka/gitops/domain/metadata/CapabilityAssessment.java (new)
src/main/java/com/devshawn/kafka/gitops/domain/metadata/ReplicationClusterSnapshot.java
src/main/java/com/devshawn/kafka/gitops/domain/metadata/ReplicationSafetyEvidence.java
src/main/java/com/devshawn/kafka/gitops/domain/plan/PlanningReport.java (new)
src/main/java/com/devshawn/kafka/gitops/domain/plan/ReplicationSafetyResult.java
src/main/java/com/devshawn/kafka/gitops/exception/KafkaExecutionException.java
src/main/java/com/devshawn/kafka/gitops/exception/PlanBlockedException.java (new)
src/main/java/com/devshawn/kafka/gitops/exception/ReplicationPreflightException.java
src/main/java/com/devshawn/kafka/gitops/manager/ApplyManager.java
src/main/java/com/devshawn/kafka/gitops/manager/PlanManager.java
src/main/java/com/devshawn/kafka/gitops/service/AdminReplicationApplication.java
src/main/java/com/devshawn/kafka/gitops/service/KafkaCapabilities.java (new)
src/main/java/com/devshawn/kafka/gitops/service/KafkaService.java
src/main/java/com/devshawn/kafka/gitops/service/ReplicationMetadataReader.java
src/main/java/com/devshawn/kafka/gitops/service/ReplicationSafetyPolicy.java
src/main/java/com/devshawn/kafka/gitops/util/KafkaDiagnostics.java (new)
src/main/java/com/devshawn/kafka/gitops/util/LogUtil.java
src/main/java/com/devshawn/kafka/gitops/util/PlanReportRenderer.java (new)
src/test/groovy/com/devshawn/kafka/gitops/DevelopmentTask2WorkflowSpec.groovy (new)
src/test/groovy/com/devshawn/kafka/gitops/ReplicationApplicationWorkflowSpec.groovy
src/test/groovy/com/devshawn/kafka/gitops/config/KafkaGitopsConfigLoggingSpec.groovy
src/test/groovy/com/devshawn/kafka/gitops/manager/ReplicationPlanningSpec.groovy
src/test/groovy/com/devshawn/kafka/gitops/service/KafkaCompatibilityTask2Spec.groovy (new)
src/test/groovy/com/devshawn/kafka/gitops/service/KafkaConnectionTask2Spec.groovy (new)
src/test/groovy/com/devshawn/kafka/gitops/service/KafkaDiagnosticsAuditSpec.groovy
src/test/groovy/com/devshawn/kafka/gitops/service/ReassignmentExecutorSpec.groovy
src/test/groovy/com/devshawn/kafka/gitops/service/ReplicationApplicationTestHarness.groovy
src/test/groovy/com/devshawn/kafka/gitops/service/ReplicationMetadataSpec.groovy
src/test/groovy/com/devshawn/kafka/gitops/service/ReplicationPreflightMetadataSpec.groovy
src/test/groovy/com/devshawn/kafka/gitops/service/ReplicationRegressionAuditSpec.groovy
src/test/groovy/com/devshawn/kafka/gitops/service/ReplicationSafetyPolicySpec.groovy
```

Существовавшие до начала untracked `gradle.properties` и
`docs/audit/modernization-stage1-2026-11-11/` сохранены и не входят в 39 файлов.
Build outputs и локальные verification scripts находятся в игнорируемых каталогах.
`git status --short`, `git diff --check` и `git diff --stat` выполнены.
Whitespace errors отсутствуют; Git предупреждает только о штатном LF → CRLF
при будущем обновлении index. HEAD/ветка не изменены, staging не выполнялся.

## Ограничения и рекомендации для Задания 3

- Нет live-доказательства совместимости ни с одной конкретной broker/ADS версией,
  реального handshake SASL/TLS/GSSAPI, permissions, DNS и advertised listeners.
  Требуется отдельный авторизованный стенд для целевой матрицы версий/режимов.
- Проверить ZooKeeper/KRaft proofs на старых серверах, отсутствие feature/config
  прав, vendor backports cordoning/ELR, старые DescribeLogDirs и topic UUID fallback.
  При недостатке положительных доказательств отказ сохраняется.
- ELR-enabled RF не поддержан алгоритмом; generic placement admission и disk
  reserve не могут быть гарантированы одними доступными read-only metadata.
- Snapshot не атомарный; между проверкой и записью возможен drift. Executor
  ограничивает риск повторными проверками, но не создаёт Kafka transaction.
  Частичные изменения возможны, rollback не выполняется.
- Протестировать асинхронное RUNNING/timeout, частичное выполнение, независимую
  финальную проверку, сохранение leader при decrease, нагрузку/объём данных и
  сценарии restart на отдельном стенде. Не считать broker CVE исправленными
  обновлением клиента. Не переносить unit/JNI-результаты на другие OS/CPU.
- Зависимости в этом задании не обновлялись и новый vulnerability scan не заявляется.
  Остаточные Gradle Guava/Jackson риски из Stage 1 остаются в том же статусе;
  см. [аудит](../audit/modernization-stage1-2026-11-11.md).
- Задание 3 и интеграционная приёмка здесь **не начинались**.
