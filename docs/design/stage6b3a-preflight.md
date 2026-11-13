<!-- SPDX-License-Identifier: Apache-2.0 -->
> Historical stage evidence: the dates, versions, hashes and test outcomes below describe that stage and artifact. Personal machine paths were anonymized for publication. See [0.4.0 preparation](../release/v0.4.0-release-preparation.md) for the current release status.

# Stage 6B3A — RF preflight и разрешения

Дата: 2026-11-10. Ветка `feature/ads-gitops`. Java 21, Gradle 8.14.6,
Shadow 8.3.11, Kafka Client 3.9.2; заявленные брокеры ADS Kafka 4.3.1.
Основа: [Stage 6A/6B1](stage6-replication.md), [Stage 6B2](stage6b2-planning.md).
Данный этап не содержит отправки reassignment и не подключается к реальному ADS при проверках.

## Граница выполнения

RF-план всегда останавливает apply до любых Kafka writes. Добавлены проверки,
но исполнитель отсутствует. Даже полностью успешный preflight возвращает через
`ReplicationPreflightException` исход `EXECUTION_NOT_IMPLEMENTED`, CLI exit code 2
и сообщение `RF execution not implemented`. Успех применения не печатается.

Для реальных снимков текущего клиента полноценный READY не достигается:
fencing, общее разрешение размещения и достаточность требуемого дискового резерва
не могут быть надёжно подтверждены реализованными API. Эти факты остаются UNKNOWN.
VERIFIED admission используется только в unit fixtures для проверки положительной ветви policy;
боевой reader не создаёт таких подтверждений.

## Модели решения

`ReplicationSafetyPolicy.evaluate()` — чистая функция от проверенного RF-плана,
свежего `ReplicationClusterSnapshot`, независимых Permissions и Constraints.
`ReplicationSafetyResult` содержит:

- вычисляемый общий `READY / BLOCKED`;
- Finding с enum Reason, категорией `INVALID / UNKNOWN`, optional partition ID и broker ID;
- типизированные предупреждения;
- решения по каждой партиции: INCREASE / DECREASE / UNCHANGED, статус и причины.

Решения не принимаются по текстам ошибок. Диагностика exception составляется
из кодов причин, без входных значений, Kafka exception messages и configs.
Одно нарушение блокирует все партиции RF-операции. Отказ одного топика
блокирует весь apply до любых изменений остальных ресурсов.
При отказе разрешений непроверенные соседние RF-операции получают
`PREFLIGHT_NOT_PERFORMED / BLOCKED`, а не фиктивный READY.

`ReplicationSafetyEvidence` добавляет только сведения снимка в памяти:
monotonic timestamp, режим ELR, стабильность features/configs/cordoning,
типизированные подтверждения placement и disk capacity. Планировочный конструктор
снимка Stage 6B2 сохранён и создаёт UNKNOWN evidence. Это не формат разрешений в JSON.

## Разрешения

Для `apply` добавлены локальные параметры команды:

```text
apply --plan plan.json --manage-replication
apply --plan plan.json --manage-replication --allow-replication-decrease
```

Это примеры синтаксиса, не команды, выполненные против Kafka в рамках этапа.

Без manage RF отклоняется до metadata calls. Уменьшение хотя бы одной партиции
требует обоих параметров. `--no-delete` имеет приоритет и запрещает уменьшение.
allow-decrease без manage — ошибка до инициализации StateManager/клиента.
Разрешения plan не переносятся в apply; JSON не хранит trusted approval.
Без RF-плана сохраняется прежняя семантика команд.
`--force` и обходы UNKNOWN отсутствуют.

## Проверяемые предусловия

| Область | Правило |
| --- | --- |
| Структура | Существующая строгая JSON-валидация, schema 1 и UPDATE |
| Идентичность | Точные cluster ID, topic UUID, существующий топик |
| Партиции | Полное число/набор IDs, ordered previousReplicas совпадают |
| Reassignment | Известное состояние; любой активный перенос этого топика блокирует |
| Лидер | Известен, в репликах и ISR; при уменьшении остаётся в target |
| ISR | Полный исходный ISR; оставляемые реплики подтверждены ISR; ELR не учитывается как ISR |
| Min ISR | Известен, совпадает с expectedMinInSyncReplicas, target RF не меньше текущего значения |
| Configs | Запрещены совместные изменения min ISR, unclean election и topic throttle; скрытые критические настройки в topicDetails также блокируют |
| PartitionPlan | Не может сочетаться с RF одного топика; структурная проверка выполняется до metadata |
| Топология | Снимок broker IDs/racks совпадает; новые пригодные кандидаты вне снимка требуют нового плана |
| Здоровье | Все исходные/целевые брокеры доступны, unfenced и с исправным storage |
| Cordoning | Для целевых брокеров известное отсутствие cordoning и стабильность между чтениями |
| Placement | Для целевых брокеров требуется подтверждённое размещение; UNKNOWN/запрет блокируют |
| Диск | Для добавляемых брокеров положительный известный usableBytes и отдельное доказательство достаточного резерва |
| Racks | RACK_AWARE требует racks и не допускает избегаемого ухудшения fault domains; BROKER_ONLY предупреждает |
| ELR | Неизвестный режим блокирует; включённый ELR консервативно блокируется при ограничениях клиента 3.9.2 |
| Свежесть | Снимок завершён после начала запрошенного чтения; возраст и полная длительность чтения не превышают 30 секунд |

Уменьшение проверяется отдельно для каждой партиции, включая неоднородный RF.
Уже изменённые assignments не считаются совпадающими с previous: на этом этапе
нужно сформировать свежий план. Продолжение частично выполненного reassignment
будет отдельной задачей executor/recovery, а не неявной возможностью preflight.

## Получение свежих данных

`KafkaService.readReplicationPreflightSnapshot()` создаёт одну Admin-сессию.
`ReplicationMetadataReader.read(..., true)` использует общий deadline существующего
default.api.timeout.ms. В отличие от preview, он дополнительно:

1. Читает `describeFeatures().featureMetadata()` до и после сбора данных.
2. Повторяет `describeConfigs(includeSynonyms=true)` для effective min ISR и cordoning.
3. Фиксирует известное изменение, неизвестность или стабильность.
4. Возвращает последние значения min ISR/cordoning и время окончания сбора.

Сохраняются повторные проверки cluster topology, topic UUID/replicas/ISR/leader
и активных reassignment из Stage 6B2. Брокерная нагрузка берётся из replicaInfos,
не из потенциально ACL-фильтрованного списка топиков. Полные Config, hostnames,
пути log directories, JAAS и credentials не входят в evidence или результат policy.

Feature `eligible.leader.replicas.version` читается как finalized/supported range.
Finalized level 1 означает ENABLED. Отсутствие finalized feature считается DISABLED
только при известном epoch и явном supported range, включающем уровни 0 и 1.
Отсутствие metadata, RPC denial, неизвестная версия или неопределённый epoch — UNKNOWN.
Смена feature metadata между чтениями блокирует preflight.

Admin закрывается с Duration.ZERO, interrupt status сохраняется. Исключения
источника метаданных превращаются в METADATA_UNAVAILABLE без внешнего текста.
Отсутствующая конфигурация не получает выдуманное значение min ISR=1.

## Интеграция

`ApplyManager.applyTopics()` выполняет структурную проверку и batch preflight
до legacy partition preflight и до цикла create/update/remove. Поэтому ни более
ранние configs/partitions, ни создание/удаление топиков не обходят RF-отказ.
`applyAcls()` имеет ту же защиту перед ACL writes, в том числе при skip-topics.
При полном пропуске категорий без применимых операций сохраняется прежний no-op.
Старый `PlanValidation.validateForApply()` остаётся консервативным запретом для
существующих прямых вызывающих; основной ApplyManager теперь использует новую policy.

## Контракт будущего executor

`ReassignmentExecutor` — только интерфейс, без реализации и без вызовов execute.
Request содержит именованные RF-операции, покрывающий все партиции READY preflight,
независимые permissions и общий monotonic deadline. Будущий исполнитель обязан
заново проверить условия: результат READY не является долговременным разрешением.

ExecutionResult содержит результат **каждой** запрошенной партиции. Неизвестный
результат должен быть явным UNKNOWN. Статусы: ACCEPTED, RUNNING, COMPLETED, FAILED,
UNKNOWN. Завершение выводится только при COMPLETED всех партиций. Смешанные
результаты сохраняются, partiallyCompleted отражает частичное завершение.
Ошибки классифицируются enum: validation, authorization, unsupported, stale plan,
broker unavailable, timeout, interrupted, partial failure, internal, unknown.
Внешние тексты ошибок не являются частью контракта.
Timeout/interrupt не означают откат, автоматическую отмену или завершение принятой работы.

## Совместимость и блокирующие ограничения

- Схема TopicReplicationPlan, старые JSON и эталоны не меняются. Алгоритм Stage 6B2 не изменён.
- Schema v1 не содержит прежний leader ID: произвольную смену лидера со времени plan
  определить нельзя. Проверяется свежий лидер и его сохранение; выдаётся предупреждение.
- Schema v1 не содержит исторический ELR baseline. При включённом ELR недоступные
  клиенту 3.9.2 per-partition ELR сведения не имитируются; операция BLOCKED.
- Node клиента 3.9.2 не даёт требуемого подтверждения fencing. Наличие Node не является
  admission, а положительный usableBytes — доказательством достаточного диска.
- Cordoned directories проверяются через effective config; неизвестные сведения блокируют.
  Частичный cordoning консервативно исключает целевой брокер.
- Чтения не атомарны. Даже повторные чтения не создают CAS/lease и не исключают гонку
  после preflight. Перед реальным executor нужны отдельно согласованные fencing,
  placement/capacity evidence, single-writer меры и проверки непосредственно перед запросами.

## Проверки

Unit-матрица включает позитивный READY на явно аттестованных fake-снимках и негативные
условия, все запреты permissions/no-delete, mixed RF, drift, UNKNOWN, ELR, JSON/CLI,
отсутствие секретов, контракт частичных результатов и нулевое число write-вызовов.
Сохраняется база 509 сценариев. В прежних тестах обновлены ожидания запрета RF и
приёма apply-флагов; fake Kafka дополнен отдельным read-only preflight методом.
Тесты не отключались, IntegrationSpec не запускаются.

Источники проверки API:

- [FeatureMetadata 3.9.2](https://kafka.apache.org/39/javadoc/org/apache/kafka/clients/admin/FeatureMetadata.html)
- [FinalizedVersionRange 3.9.2](https://kafka.apache.org/39/javadoc/org/apache/kafka/clients/admin/FinalizedVersionRange.html)
- [ELR и min ISR в Kafka 4.3](https://kafka.apache.org/43/operations/eligible-leader-replicas/)
- Локальный source JAR kafka-clients 3.9.2 и компиляция с текущими зависимостями.
