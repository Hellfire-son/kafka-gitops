# Stage 6B2 — планирование replication factor

Дата: 2026-11-10. Основа: [проект Stage 6A и контракт Stage 6B1](stage6-replication.md).
Java 21, Kafka Client 3.9.2; целевые брокеры ADS Kafka 4.3.1.
Версии зависимостей и JSON-схема TopicReplicationPlan не изменены.

## Граница безопасности

Результат — воспроизводимый **preview**, применение RF запрещено существующим
`PlanValidation.validateForApply()` до обращений к Kafka и любых topic writes.
ReassignmentExecutor и отправка reassignment отсутствуют. Наличие флагов планирования
или JSON-плана не является разрешением на применение.

## CLI

Новые параметры принадлежат только команде `plan`:

- `--manage-replication`: явное включение RF-планирования существующих топиков.
- `--allow-replication-decrease`: отдельное разрешение **только на планирование**
  уменьшения. Требует первого флага; не сохраняется в JSON.
- `--replication-rack-policy RACK_AWARE|BROKER_ONLY`: явная политика; по умолчанию
  RACK_AWARE. Указание политики также требует `--manage-replication`.

Примеры синтаксиса, без выполнения в рамках разработки:

```text
java -jar kafka-gitops-all.jar --skip-acls plan --manage-replication
java -jar kafka-gitops-all.jar --skip-acls plan --manage-replication --allow-replication-decrease
java -jar kafka-gitops-all.jar --skip-acls plan --manage-replication --replication-rack-policy BROKER_ONLY
```

`--no-delete` запрещает планировать уменьшение даже с отдельным разрешением.
Команда `apply` не принимает новые флаги. Применение сохранённого RF-плана запрещено
и без флагов. В Stage 6B3 разрешения должны проверяться заново при apply, независимо
от входного JSON и разрешения, которое использовалось при plan.

Без opt-in сохраняются предупреждение Stage 5 и отсутствие RF-операции.
`--skip-topics` исключает все topic/RF metadata calls. Создание новых топиков,
`partitionPlan`, `previousValue`, topic configs, ACL и prefix filters сохраняют
прежнюю семантику. Увеличение партиций вместе с RF для одного топика отклоняется.
Изменения min ISR и throttle вместе с RF требуют отдельных планов.

## Метаданные

`KafkaService.readReplicationSnapshot()` открывает одну Admin-сессию и передаёт её
`ReplicationMetadataReader.read()`. Вложенных AdminClient нет. Все RPC имеют timeout,
все ожидания используют единый deadline на основании `default.api.timeout.ms`.
Клиент закрывается с `Duration.ZERO`, interrupt status восстанавливается.

Используются реальные методы Admin API 3.9.2:

| API | Использование |
| --- | --- |
| describeCluster | cluster ID, broker IDs, racks; повторная проверка topology |
| describeTopics().allTopicNames() | topic UUID, все partition IDs, ordered replicas, лидер, ISR; повторная проверка управляемых топиков |
| listPartitionReassignments | Обнаружение активных переносов; повторная проверка появления/исчезновения |
| describeConfigs(includeSynonyms=true) | Только извлечение effective min.insync.replicas и cordoned.log.dirs |
| describeLogDirs().descriptions() | Доступность, ошибки каталогов, OptionalLong usableBytes и replicaInfos всех текущих реплик брокера |

`ReplicationClusterSnapshot` — неизменяемый снимок в памяти. Broker observations
имеют состояния VERIFIED / UNAVAILABLE / UNKNOWN. Неизвестные лидер, min ISR,
rack и usableBytes представлены Optional, без выдуманных значений. Broker fencing
выражен отдельным observation `unfenced=UNKNOWN`: клиент 3.9.2 не предоставляет
соответствующие поля Kafka 4.x. Полные Config, credentials, hostnames и пути к
log directories в снимок и RF JSON не попадают. Тексты Kafka exceptions не печатаются.

Ошибки обязательных cluster/topic/reassignment RPC запрещают получение плана.
Ошибки чтения отдельных конфигураций оставляют UNKNOWN. Отказ/таймаут конкретного
брокера исключает его из кандидатов. Если достаточных проверенных кандидатов нет,
планирование завершается ошибкой. Изменение метаданных во время чтения требует нового плана.
Порядок ISR как множества и порядок ответа partitions не создают искусственный drift;
изменение **порядка replicas** считается существенным.

## Ограничения Kafka 3.9.2 с брокерами 4.3.1

Наличие Node в describeCluster не доказывает пригодность к размещению.
Для нового preview-кандидата нужны успешное чтение log directories, отсутствие ошибок
всех каталогов, положительное известное usableBytes каждого каталога и явно полученное
пустое effective `cordoned.log.dirs`. Любое непустое значение cordoned.log.dirs,
включая `*` или один каталог, консервативно исключает **весь брокер из новых назначений**:
старый клиент не умеет видеть новый признак isCordoned отдельного каталога.
Отсутствие настройки не интерпретируется как пустое значение; это UNKNOWN.
Существующие здоровые ISR-реплики могут оставаться на cordoned-брокере.

`Broker.isPreviewCandidate()` не является доказательством безопасности применения.
UNKNOWN fencing допускается только в preview, для которого apply безусловно заблокирован.
Заведомо fenced/недоступные брокеры исключаются. До Stage 6B3 необходим способ надёжно
проверять fencing, eligibility и свежесть cordoning; подмена этого проверки наличием Node запрещена.
Самая новая broker-side информация не становится доступной простым чтением старого API.

Минимальный usableBytes — наблюдение, а не резервирование места и не гарантия достаточного
размера для новой реплики. Алгоритм балансирует **число реплик**, не объём, IOPS или
сетевой трафик. Проверка требуемого объёма данных и дисковых ресурсов остаётся обязательной
границей будущего apply. Сбор метаданных не атомарен, topology/ISR/configs могут измениться
после чтения. Повторные чтения обнаруживают часть гонок, но не устраняют их.

## Детерминированный алгоритм

`ReplicaAssignmentPlanner.plan()` — чистая функция, без Kafka IO и случайного выбора.
Сначала обрабатываются лексикографически отсортированные имена топиков, затем partition IDs.
Нагрузка берётся из broker-scoped `describeLogDirs().replicaInfos()`, включая внутренние
и скрытые topic ACL топики. `listTopics()` не используется как доказательство полноты:
он может возвращать только разрешённые топики. Future log-directory copies не удваивают
счётчик. Недоступный/ошибочный каталог оставляет число реплик этого брокера неизвестным
(нет значения в карте), вместо нулевого. Для всех выбранных и исходных брокеров требуется
подтверждённое число реплик, не меньше числа наблюдаемых исходных назначений.

Увеличение: сохраняется весь ordered previousReplicas, добавляется брокер, которого
ещё нет в партиции. В RACK_AWARE приоритеты: меньше уже выбранных реплик в этой rack,
затем меньше прогнозируемое число реплик на брокере, затем меньший broker ID.
Добавление сразу увеличивает прогноз для следующих выборов и следующих топиков.

Уменьшение: текущий живой лидер выбирается обязательно. Остальные выбираются из
исходных ISR-реплик с теми же tie-breakers. Результат фильтруется через исходный порядок,
сохраняя ordered subsequence. Удалённые реплики сразу вычитаются из прогноза нагрузки.
Для всех партиций изменяемого топика требуются лидер, полный исходный ISR и доступные
здоровые исходные брокеры. Неполный ISR, неизвестный min ISR или targetRF < min ISR — отказ.

Партиция с уже целевым RF сохраняет точные IDs и порядок. Неоднородный исходный RF
не сворачивается в выдуманное скалярное previousReplicationFactor. Разрешение на уменьшение
нужно, если уменьшается хотя бы одна партиция, включая смешанный increase/decrease.
Активный reassignment управляемого RF-топика запрещает расчёт.

RACK_AWARE требует полных rack metadata всех допустимых/сохраняемых брокеров.
Число реплик может превышать число racks: тогда используются доступные fault domains
и минимизируются повторы. В BROKER_ONLY racks не влияют на выбор; выводится предупреждение,
что защита от отказа стойки не установлена. Частичные racks не меняют политику автоматически.

## Пример полного JSON-плана

Синтетические UUID и racks; назначения согласованы с моделью Stage 6B1:

```json
{
  "topicPlans": [{
    "name": "orders",
    "action": "UPDATE",
    "topicDetails": {"partitions": 2, "replication": 3, "configs": {}},
    "topicConfigPlans": [],
    "replicationPlan": {
      "schemaVersion": 1,
      "action": "UPDATE",
      "clusterId": "AQIDBAUGBwgJCgsMDQ4PEA",
      "topicId": "ERITFBUWFxgZGhscHR4fIA",
      "partitionCount": 2,
      "targetReplicationFactor": 3,
      "algorithm": "preserve-rack-load-v1",
      "rackPolicy": "RACK_AWARE",
      "expectedMinInSyncReplicas": 1,
      "brokerSnapshot": [{"id": 1, "rack": "a"}, {"id": 2, "rack": "b"}, {"id": 3, "rack": "c"}],
      "assignments": [
        {"partition": 0, "previousReplicas": [1, 2], "targetReplicas": [1, 2, 3]},
        {"partition": 1, "previousReplicas": [2, 1], "targetReplicas": [2, 1, 3]}
      ]
    }
  }],
  "aclPlans": []
}
```

Пример вывода увеличения:

```text
~ [TOPIC] orders
    ~ replication: 2 -> 3
        partition 0: [1, 2] -> [1, 2, 3]
        partition 1: [2, 1] -> [2, 1, 3]
    [WARNING] RF preview only; application remains unavailable in Stage 6B2.
```

При уменьшении 3 -> 2 с лидером 1 исходный `[1, 2, 3]` может стать `[1, 2]`;
выводится предупреждение о независимом apply permission, ISR/min ISR/preflight и --no-delete.
При исходном RF 2/3 и цели 3 отображается `replication: mixed [2, 3] -> 3`
и назначения обеих партиций, включая неизменённую.

## Проверяемые границы и следующие этапы

Unit-тесты используют чистый planner, in-memory KafkaService и fake Admin с allowlist
read-only методов. Любой write API в этом fake вызывает ошибку. Покрываются
RF 1->2/3, 2->3, 3->2/1, mixed RF, перестановки Map/Set/partition responses,
прогноз нагрузки нескольких топиков, лидер, racks, ISR/min ISR, cordoning, UNKNOWN,
отказы RPC, timeout, interrupt, drift, JSON, CLI и отказ apply.

Схема TopicReplicationPlan v1 не меняется; old JSON и все 393 прежних теста сохраняются.
Диагностика Config и сериализация sensitive configs из Stage 1–2 не ослабляются.
Stage 6B3 не реализуется: потребуется отдельно согласовать авторизацию, возможности
ADS, свежий preflight, безопасность исполнения/восстановления и наблюдение завершения.

Источники проверки API:

- [Admin 3.9.2](https://kafka.apache.org/39/javadoc/org/apache/kafka/clients/admin/Admin.html)
- [LogDirDescription 3.9.2](https://kafka.apache.org/39/javadoc/org/apache/kafka/clients/admin/LogDirDescription.html)
- [Kafka 4.3: cordoned.log.dirs](https://kafka.apache.org/43/configuration/broker-configs/#brokerconfigs_cordoned.log.dirs)
- [KIP-1066](https://cwiki.apache.org/confluence/spaces/KAFKA/pages/311627566/KIP-1066%2BMechanism%2Bto%2Bcordon%2Bbrokers%2Band%2Blog%2Bdirectories)
- Локальный source JAR kafka-clients 3.9.2, фактическая компиляция с текущей зависимостью.
