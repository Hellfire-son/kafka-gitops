# Stage 6B3B — внутренний ReassignmentExecutor

Дата: 2026-11-10. Java 21, Kafka Client 3.9.2; заявленные брокеры ADS Kafka 4.3.1.
Основа: [Stage 6A](stage6-replication.md), [планирование](stage6b2-planning.md),
[preflight и разрешения](stage6b3a-preflight.md).

## Изоляция

`AdminReassignmentExecutor` — package-local компонент. Он принимает готовую Admin-сессию,
не создаёт и не закрывает клиент, не регистрируется через ServiceLoader и не подключён
к ApplyManager, StateManager или CLI. Их прежний барьер сохранён: BLOCKED при отказе
policy; EXECUTION_NOT_IMPLEMENTED при READY. Новых CLI-флагов нет.
Все проверки executor выполнены с fake Admin. Реальные Kafka writes не выполнялись.

Стандартный reader использует `ReplicationMetadataReader.read(..., true)` с той же
переданной сессией. Он по-прежнему возвращает UNKNOWN для неподтверждённых fencing,
placement admission и достаточности дискового резерва. Такие снимки запрещают отправку.
Применение к ADS и устранение этих ограничений не входят в Stage 6B3B.

## Архитектура

`execute(Request)` строит стабильный порядок topic name / partition ID и хранит
отдельное состояние каждой партиции. Request содержит исходный неизменяемый RF-план,
READY предыдущего preflight, независимые разрешения и общий monotonic deadline.
Предыдущий READY не считается действующим разрешением: перед каждым пакетом
необходимо заново получить свежие данные и пройти ReplicationSafetyPolicy.

Внедряемые зависимости ограничены package-local API:

- Admin — единственная сессия для чтений и отправки;
- SnapshotReader — полное чтение safety metadata; в боевой реализации сведения не выдумываются;
- TimeSource — monotonic clock и прерываемое ожидание; тесты используют виртуальные часы;
- Settings — batch size 1..1000, RPC timeout, положительный ограниченный poll interval.

По умолчанию batch size 100, RPC timeout 10 секунд, poll interval 100 мс.
Общий deadline задаёт Request и включает проверки, ответы и перенос данных.

## Минимальные уточнения контракта

Старый PartitionOutcome(state, error) не различал собственную попытку отправки
и наблюдение чужой операции. Также он терял известное состояние после timeout.
Добавлены поля с совместимым двухаргументным конструктором:

- submission: NOT_SENT / SENT / OBSERVED;
- lastKnownState: последнее подтверждённое состояние перед неопределённым исходом.

SENT означает только попытку вызова API этим запуском, а не доказательство принятия.
OBSERVED не заявляет авторство — перенос мог начать другой процесс или предыдущий запуск.
К существующим ErrorCategory добавлены PREFLIGHT_BLOCKED, CONFLICT, NOT_SUBMITTED,
RPC_TIMEOUT и DEADLINE_EXCEEDED. Старое значение TIMEOUT сохранено для совместимости.
JSON RF-плана не изменяется; эти модели являются результатом исполнения.

## Проверка перед записью

Сначала читаются cluster ID, describeTopics и active reassignment. Проверяются topic UUID,
полный набор partition IDs и текущее назначение каждой партиции.
Перед пакетом выполняется полноценный свежий preflight с неизменёнными требованиями:
permissions/no-delete, cluster/topic identity, min ISR, ELR, topology, racks, health,
fencing, cordoning, admission, capacity и свежесть.
Потом ещё раз читаются identity/replicas/ISR/leader/reassignment и сверяются со снимком.
Drift запрещает отправку. Повторное чтение не является атомарной блокировкой или CAS.

Обычная policy Stage 6B3A требует previousReplicas всех партиций. После первого
завершённого пакета это условие стало бы ложным для следующего. Поэтому добавлен
отдельный `evaluateForExecution(..., completedPartitions)`:

- previous/target в плане не меняются и не пересчитываются;
- объявленная завершённой партиция обязана **фактически** иметь точные target replicas,
  полный ISR и корректного лидера;
- остальные партиции обязаны иметь исходные ordered previousReplicas;
- все глобальные UNKNOWN, права и проверки брокеров остаются блокирующими;
- активный reassignment по-прежнему блокирует новую запись.

Executor сначала завершает наблюдение предыдущего пакета/совместимого переноса,
и только затем переходит к следующему. Старый `evaluate()` сохраняет строгую семантику
Stage 6B3A. Объявить исходную или нездоровую партицию завершённой через scope нельзя.

## Отправка и частичные ответы

Пакет содержит только ещё исходные, не отправленные этим запуском партиции.
В `alterPartitionReassignments()` передаются конкретные ordered targetReplicas из плана,
исключительно `Optional.of(new NewPartitionReassignment(...))`.
Нет cancellation через Optional.empty, random placement, автоматического rollback,
пересчёта targets, пересылки совместимых чужих операций или повторов после потери ответа.

Используются отдельные futures `values()`, не общий all() как признак завершения.
Успешный future даёт ACCEPTED. Все futures пакета используют один RPC deadline;
уже завершённые ответы соседних партиций разбираются и после timeout другой партиции.
Definitive rejection сохраняется как FAILED с категорией по типу исключения.
Timeout/transport uncertainty сохраняется как UNKNOWN, без вывода текста исключения.

При любом частичном отказе/неопределённом ответе следующие пакеты не отправляются.
Для неотправленных партиций фиксируется NOT_SUBMITTED. Принятые и неопределённые
попытки продолжают безопасно наблюдаться до результата или deadline.
Если последующие чтения надёжно подтверждают target, неизвестный ответ может быть
разрешён в COMPLETED; повторная отправка для этого не нужна.

## Наблюдение и повторный запуск

| Наблюдаемое состояние | Действие |
| --- | --- |
| Исходные ordered replicas, нет переноса | Возможна отправка только после свежей policy |
| Точные target, полный ISR и здоровый target leader, нет active | COMPLETED; при первом чтении это наблюдаемый no-op |
| Target уже установлен, ISR/leader ещё не готовы | RUNNING, только ждать |
| Совместимый активный перенос | Наблюдать, не отправлять и не присваивать авторство |
| Конфликтующий active | CONFLICT, запрет следующих записей |
| Иное назначение или identity drift | STALE_PLAN, требуется повторная проверка/план |

Совместимость active определяется по типизированным replicas/adding/removing:
union брокеров соответствует original ∪ target, дельты совпадают с планом,
а replicas без removing точно дают ordered target. Это учитывает удаление
средней реплики, когда временный union имеет другой порядок, чем original.

COMPLETED требует одновременно отсутствия active, точных ordered target replicas,
всех target в ISR, лидера в target и ISR, неизменных cluster ID/topic UUID и полного
набора партиций. Одного отсутствия reassignment или ACK недостаточно.

После UNKNOWN следующий запуск заново читает Kafka. Историческое SENT не переносится
в авторство нового запуска: уже выполняемое/завершённое состояние помечается OBSERVED.
Долговременное хранение результатов и операционная координация остаются обязанностью
будущего вызывающего компонента; скрытого локального journal/rollback здесь нет.

## Timeout, interrupt и диагностика

RPC_TIMEOUT означает неуспех ограниченного ожидания RPC. DEADLINE_EXCEEDED —
исчерпание общего времени операции. UNKNOWN сохраняет lastKnownState, например
ACCEPTED или RUNNING. Это не означает отмену работы брокеров.
InterruptedException восстанавливает interrupt flag; уже прерванный вызывающий
поток не начинает чтения или записи. Следующие пакеты не отправляются.
Все ожидания конечны и используют внедрённые часы. Admin не закрывается executor.

Диагностика основана на enum-категориях и данных плана: credentials, JAAS, пароли,
токены, полные Config и сырые exception messages не журналируются и не включаются
в ExecutionResult. Общий COMPLETED возможен только при COMPLETED всех партиций;
индивидуальные FAILED/UNKNOWN и partiallyCompleted сохраняются.

## Проверки и ограничения

Проверяются increase/decrease, несколько топиков/партиций, последовательные пакеты,
partial acceptance, отдельные futures, metadata drift, ISR/leader, replay/no-op,
чужие переносы, RPC/общий timeout, interrupt, нулевые writes при BLOCKED и секреты.
Отдельный тест стандартного reader подтверждает, что UNKNOWN 3.9 safety не становится READY.
Сохраняются 639 базовых unit-сценариев и production CLI barrier.

Остаются ограничения Stage 6B3A: fencing, placement admission, достаточный дисковый
резерв, часть ELR данных и отсутствие атомарного compare-and-set в Admin API.
Совпадение метаданных непосредственно перед отправкой не исключает внешнюю гонку.
Нужны отдельно согласованные решения Stage 6B3C, включая надёжные источники safety
evidence и single-writer координацию. Этот этап не подтверждает готовность RF к production.

Источники проверки API:

- [AlterPartitionReassignmentsResult 3.9.2](https://kafka.apache.org/39/javadoc/org/apache/kafka/clients/admin/AlterPartitionReassignmentsResult.html)
- [PartitionReassignment 3.9.2](https://kafka.apache.org/39/javadoc/org/apache/kafka/clients/admin/PartitionReassignment.html)
- [NewPartitionReassignment 3.9.2](https://kafka.apache.org/39/javadoc/org/apache/kafka/clients/admin/NewPartitionReassignment.html)
- Локальный source JAR Kafka Client 3.9.2 и компиляция с текущими зависимостями.
