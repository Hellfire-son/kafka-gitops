<!-- SPDX-License-Identifier: Apache-2.0 -->
> Historical stage evidence: the dates, versions, hashes and test outcomes below describe that stage and artifact. Personal machine paths were anonymized for publication. See [0.4.0 preparation](../release/v0.4.0-release-preparation.md) for the current release status.

# ELR и безопасное изменение replication factor

Дата: 12.11.2026. Основа: `feature/ads-gitops`, HEAD `91a7c974beb098dbe49d339ed1bfddb282aad399`.
Java 21, Kafka Client 4.4.0; зависимости, Gradle 9.9.1 и Shadow 9.6.1 сохранены.
Дополнение 13.11.2026: проверка Kafka 3.x и согласование конечных metadata описаны ниже.

## Проверенные свойства Kafka

ISR отражает синхронизированные с лидером реплики, участвующие в обычном продвижении high watermark.
ELR хранит дополнительных кандидатов для восстановления лидерства при пустом ISR. Наличие реплики
в ELR не доказывает её текущую синхронизацию. Включение cluster feature не означает непустой ELR
каждой partition. См. принятый [KIP-966](https://cwiki.apache.org/confluence/spaces/KAFKA/pages/263426970/KIP-966%2BEligible%2BLeader%2BReplicas).

В Kafka 4.4 `PartitionChangeBuilder` сначала предпочитает допустимый ISR; ELR рассматривается,
когда ISR пуст. При ISR размером не меньше effective min ISR ELR очищается. В этой реализации
LastKnownELR также может хранить последнего лидера для balanced recovery: это история восстановления,
а не подтверждение актуального ISR. Изменение min ISR влияет на расчёт этих наборов. GitOps не меняет
min ISR автоматически. См. методы `isValidNewLeader`, `maybePopulateTargetElr`,
`maybeUpdateLastKnownLeader` в [PartitionChangeBuilder 4.4.0](https://raw.githubusercontent.com/apache/kafka/4.4.0/metadata/src/main/java/org/apache/kafka/controller/PartitionChangeBuilder.java).

Контроллер применяет reassignment поэтапно: добавляет реплики, ожидает нужный ISR, затем завершает
удаление старых реплик. Некоторые изменения завершаются сразу. Серверная допустимость шире нашей
консервативной политики; один успешный ACK не является доказательством завершения. См.
`changePartitionReassignment` в [ReplicationControlManager 4.4.0](https://raw.githubusercontent.com/apache/kafka/4.4.0/metadata/src/main/java/org/apache/kafka/controller/ReplicationControlManager.java).

## Откуда берутся ELR и LastKnownELR

`describeTopics` в Client 4.4.0 использует `DescribeTopicPartitions`, когда он доступен, и может
перейти на Metadata API. Первый путь вызывает `DescribeTopicPartitionsResponse.partitionToTopicPartitionInfo`;
Metadata fallback создаёт четырёхаргументный `TopicPartitionInfo`. Поэтому самого имени
`describeTopics` недостаточно для заключения о полноте ELR.
См. [KafkaAdminClient 4.4.0](https://raw.githubusercontent.com/apache/kafka/4.4.0/clients/src/main/java/org/apache/kafka/clients/admin/KafkaAdminClient.java)
и [преобразование ответа](https://raw.githubusercontent.com/apache/kafka/4.4.0/clients/src/main/java/org/apache/kafka/common/requests/DescribeTopicPartitionsResponse.java).

В `TopicPartitionInfo` четырёхаргументный конструктор оставляет оба ELR-поля `null`, а шестипараметрический
принимает реальные списки. В модели GitOps `null` превращается в `Optional.empty()` — неизвестность;
подтверждённый `[]` становится `Optional.of(List.of())`. Нормализации отсутствия в пустой список нет.
См. [TopicPartitionInfo 4.4.0](https://raw.githubusercontent.com/apache/kafka/4.4.0/clients/src/main/java/org/apache/kafka/common/TopicPartitionInfo.java).

Wire API `DescribeTopicPartitions` v0 содержит nullable `EligibleLeaderReplicas` и `LastKnownElr`,
оба с default `null`. Подтверждённая пустота берётся только из возвращённого API списка. При ошибке
декодирования/недоступности API не возникает положительного доказательства. Источник:
[схема Kafka 4.4.0](https://raw.githubusercontent.com/apache/kafka/4.4.0/clients/src/main/resources/common/message/DescribeTopicPartitionsResponse.json).

## Модель и политика

Использованы существующие `ReplicationSafetyEvidence.ElrMode` и `CapabilityAssessment`.
Независимый дублирующий enum состояния ELR не введён.

| Наблюдение | Assessment | Основание |
|---|---|---|
| Положительно подтверждённая неприменимость | NOT_APPLICABLE | ELR_NOT_APPLICABLE |
| Подтверждённый disabled | SUPPORTED | ELR_DISABLED |
| Enabled, оба набора известны и пусты, полный здоровый ISR | SUPPORTED | ELR_VERIFIED_EMPTY |
| Непустой ELR или LastKnownELR | UNSAFE | ELR_NONEMPTY / LAST_KNOWN_ELR_NONEMPTY |
| Неверные ID, дубликаты, участники вне replicas | UNSAFE | ELR_METADATA_INVALID |
| Enabled и нездоровый ISR/лидер | UNSAFE | ELR_ISR_UNHEALTHY |
| Неизвестная feature или отсутствующее поле при enabled | UNKNOWN | ELR_MODE_UNESTABLISHED / ELR_METADATA_UNAVAILABLE |

`ElrSafetyPolicy` используется preview, planner, execution preflight и executor. Противоречащие
непустые наборы блокируются даже при заявленном disabled/N/A. Состояние SUPPORTED относится
только к ELR: оно не выдаёт общего READY.

Feature level определяется по свежим `describeFeatures`, finalized range и известному epoch.
Поддержан enabled level 1; неизвестные будущие уровни, отрицательный/отсутствующий epoch и
ошибки чтения сохраняют UNKNOWN. Наличие версии клиента не доказывает версию сервера.

В KRaft 4.4 feature epoch — offset metadata image. Он может расти без смены features, включая
записи reassignment. Для enabled level 1 два чтения должны подтвердить равенство **обеих полных
карт** finalized/supported features и известный неубывающий epoch. Регресс epoch или изменение
карты при enabled блокируют. Более строгое правило disabled сохранено.
Источник: `features()` в [KRaftMetadataCache 4.4.0](https://raw.githubusercontent.com/apache/kafka/4.4.0/metadata/src/main/java/org/apache/kafka/metadata/KRaftMetadataCache.java).

Для положительно подтверждённого NOT_APPLICABLE два чтения тоже требуют одинаковых полных карт
и допустимых известных epochs, но допускают разные offsets, включая меньший второй: запросы могут
попасть на разные broker caches. Это подтверждено на Kafka 3.7.1 и соответствует `features()` в
[KRaftMetadataCache 3.7.1](https://raw.githubusercontent.com/apache/kafka/3.7.1/core/src/main/scala/kafka/server/metadata/KRaftMetadataCache.scala).
Изменившаяся карта или отсутствие положительного mode proof не превращается в READY.
Прежний ZooKeeper fallback с подтверждённым режимом и одинаковыми ответами сохранён.

## Increase, decrease и завершение

RF 1→2 и 2→3 допустимы при полном исходном ISR, допустимом лидере, подтверждённо пустых
ELR/LastKnownELR и прохождении всех прежних проверок. Они включают cluster ID, topic UUID,
partition count, ordered previous assignments, topology/racks, broker health/fencing,
cordoning, healthy log directories, effective min ISR, конфликтующие reassignment,
свежесть данных и разрешения. Подтверждённые запреты placement/disk admission сохраняются.
Прежние предупреждения о недоказанном generic admission и запасе места остаются предупреждениями;
ELR-доработка не меняет их классификацию.

RF 3→2 дополнительно требует `--allow-replication-decrease` и отсутствия `--no-delete`.
Текущий лидер сохраняется в target, каждая target-реплика принадлежит исходному ISR,
target RF не ниже min ISR, rack policy соблюдена. При непустом ELR/LastKnownELR decrease
заблокирован: безопасность удаления потенциальных кандидатов не доказана.

`plan` выполняет только чтения, показывает actual/desired, ordered assignments, ISR/ELR/
LastKnownELR и конкретные причины отказа. Допустимый preview заканчивается
`FRESH_PREFLIGHT_REQUIRED`; apply повторно получает доказательства. JSON schema v1 не меняется:
новые ELR-поля и mode являются только in-memory metadata, а не полномочиями из сохранённого JSON.
Старые конструкторы оставлены совместимыми; отсутствие ELR в них означает UNKNOWN.

Одна управляемая Admin-сессия принадлежит `AdminReplicationApplication`; reader/executor
используют её с ограниченными RPC и общим deadline. Перед каждым пакетом повторяется полная
policy и дополнительное сравнение identity/assignments/leader/ISR/ELR после snapshot.
Во время известного RUNNING новые реплики могут догонять ISR; retained-реплики обязаны
оставаться в ISR, лидер — в ISR/replicas, ELR/LastKnownELR — известными и пустыми.
Новая опасность прекращает отправку следующих пакетов.

COMPLETED требует отсутствия active reassignment, точного ordered target, полного target ISR,
допустимого лидера, прежних cluster ID/topic UUID, свежих features/configs/brokers и ELR-инвариантов.
Интеграционная граница независимо повторяет конечную проверку. Потеря доказательства даёт UNKNOWN/
FAILED, сохраняя submission и lastKnownState. Автоматических rollback/cancel и изменения target нет.
Диагностика содержит enum reasons и числовые ID, не сообщения Kafka с возможными секретами.

После наблюдавшегося COMPLETED независимый read может получить старые assignments/ISR от другого
broker cache. С 13.11.2026 эта фаза допускает только повторные чтения в пределах одного RPC window
и общего deadline. Новых отправок нет. Ожидание разрешено лишь при допустимом лидере и сохранении
retained ISR; identity drift, foreign assignments, опасный/неизвестный ELR прекращают проверку.
Успех по-прежнему требует полного terminal proof; постоянный lag заканчивается UNKNOWN.
Реальные traces, регрессионные тесты и итоговая матрица: [Kafka 3.x results](../testing/kafka-3x-compatibility-results.md).

## Старые Kafka и границы поддержки

| Сервер | Сохранённое поведение | Live-проверка в этой работе |
|---|---|---|
| 3.2.x / 3.7.1 / 3.9.x | Положительная неприменимость через ZooKeeper либо metadata bound ниже ELR; nullable Metadata fallback не требует нового ELR API | Нет |
| ADS 4.3.1 | Capability proofs без предположения о vendor backports; UNKNOWN блокирует | Нет |
| Apache Kafka 4.4.0 | Enabled level1 + подтверждённые пустые наборы + прежние invariants | RF 1→2→3→2 |

Граница metadata level для ELR остаётся 23 (`4.0-IV1`). Одного отсутствия feature недостаточно
для N/A. Если старый сервер не предоставляет положительные доказательства, операция остаётся
BLOCKED, а не требует отключения ELR или подмены данных. Обратная совместимость проверена
прежними 972 тестами и старыми JSON-планами; это не live-сертификация перечисленных версий.

TOCTOU остаётся: Kafka не предоставляет атомарную транзакцию между preflight и reassignment.
Schema v1 не закрепляет исходного лидера. Реальные дисковые объёмы, конкурирующая нагрузка,
ACL/authorizer/vendor policy и поведение при отказе брокеров требуют отдельной эксплуатационной
проверки. Поддержка непустого ELR намеренно отсутствует. Данные стенда и результаты:
[Kafka 4.4 ELR/RF](../testing/kafka-4.4-elr-rf-results.md).
