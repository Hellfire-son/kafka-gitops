<!-- SPDX-License-Identifier: Apache-2.0 -->
> Historical stage evidence: the dates, versions, hashes and test outcomes below describe that stage and artifact. Personal machine paths were anonymized for publication. See [0.4.0 preparation](../release/v0.4.0-release-preparation.md) for the current release status.

# Программа будущих интеграционных испытаний Kafka GitOps

Статус: **ПОДГОТОВЛЕНО, НЕ ВЫПОЛНЕНО**. Дата: 12.11.2026.
Ни Docker, ни Kafka/ADS, ни настоящий `ccloud` в Задании 3 не запускались.
Здесь описаны действия для последующего отдельно разрешённого испытания.

## Изоляция и входные условия

Для каждого прогона нужны отдельный disposable cluster, зафиксированный cluster ID,
версия/дистрибутив/patch level, metadata mode и feature levels. Production и ADS
организации не должны быть достижимы из тестового окружения. Bootstrap и все
advertised listeners проверяются по allowlist стенда. Каждый run получает уникальный
prefix `qa-<run-id>-`, каталог evidence и отдельные credentials с ограниченным сроком.
Не использовать IP из прежнего пользовательского испытания как готовую конфигурацию.

Перед первым подключением ответственный оператор подтверждает целевой bootstrap
и ожидаемый cluster ID из независимого источника. Сам bootstrap не доказывает identity.
Read-only Admin probe читает `describeCluster().clusterId()` с конечным timeout,
сравнивает его с allowlist и сохраняет результат без секретов. Несовпадение запрещает
все write-проверки. Та же проверка выполняется заново непосредственно перед **каждым**
write-case и cleanup; встроенный RF preflight дополняет её. Обычные non-RF ADD/config/
partition/ACL планы не содержат универсальной cluster-ID блокировки, поэтому этот
внешний gate обязателен для всей программы.

Для **каждого W-case** требуется отдельное явное разрешение ответственного: case ID,
run ID, cluster ID, ресурсный prefix, конкретные API, ожидаемые изменения, время и
cleanup. Разрешение одного case не разрешает остальные. Отсутствие ответа,
неизвестная identity, истёкший доступ или смена endpoint означают STOP.
Программа не добавляет `--force` или способа обхода preflight.

Для topic-only cases используйте `--skip-acls`; для ACL — выделенный кластер
без посторонних ACL либо заранее согласованный полный desired ACL inventory.
Topic include-prefix **не фильтрует ACL**. На read-only фазе всегда используйте
`--no-delete`; отсутствие write-прав должно независимо запрещать любые writes.

Пример topic-only YAML стенда (не исполнялся):

```yaml
settings:
  topics:
    include:
      prefixed: [qa-RUN123-]
topics:
  qa-RUN123-acl-test:
    partitions: 4
    replication: 2
    configs:
      max.message.bytes: 10485880
```

Для настоящего run замените prefix и topic name одновременно. Include ограничивает
управляемый namespace; он не является заменой сетевой изоляции, identity и ACL.

## Матрица окружений

| Поставка | Режим / особенности | Обязательный scope |
|---|---|---|
| Apache Kafka 3.2.x, точный patch фиксируется | ZooKeeper; KRaft отдельно, если доступен в выбранном patch | Read-only API negotiation, legacy log dirs/features, отказ при недостатке proofs |
| Apache Kafka 3.7.1 | ZooKeeper и KRaft раздельно | Полный набор, доступные mode proofs |
| Apache Kafka 3.9.x | ZooKeeper/KRaft согласно выбранной поставке | Полный набор, metadata.feature limits |
| ADS Kafka 4.3.1 | Фактическая поставка, backports и включённые features записываются | Vendor API/config/policy differences, security и RF |
| Apache Kafka 4.4.x | KRaft, ELR disabled/enabled отдельно | Стандартный RF при disabled; ожидаемый BLOCKED при enabled |

Одна и та же Java 21 сборка используется в каждой строке. Нельзя выводить точный
release из одного ApiVersions range. Данные реального distribution manifest и
Admin evidence сохраняются отдельно. Старые KRaft metadata могут не дать достаточного
доказательства cordoning/ELR: ожидаемый безопасный результат тогда BLOCKED,
а не искусственно созданный READY. Такая строка не считается полной RF-поддержкой.

Минимум два brokers с racks `1`/`2` нужны для исходного случая RF 1 → 2. Для 2 → 3
и 3 → 2/1 нужны минимум три подходящих brokers; для проверки выбора альтернатив
желателен четвёртый. Для BROKER_ONLY отдельно фиксируется принятый rack-risk.
Disks должны иметь контролируемый запас; данные должны быть синтетическими.

## Credentials и безопасность

- Read-only principal: только права чтения, необходимые фактически вызываемым
  DescribeCluster/Topics/Configs/LogDirs/Features, ListPartitionReassignments и,
  если включён ACL case, DescribeAcls. Точный mapping прав берётся из документации
  выбранного broker patch и проверяется отказами, а не расширением профиля приложения.
- Write principal: краткосрочный, ограничен prefix и разрешёнными API; cluster-level
  RF права выдаются только изолированному стенду на время RF cases. App не добавляет
  DESCRIBE, не меняет READ/WRITE/CREATE и не создаёт новые профили.
- `client.properties`, JAAS, сертификаты/keytabs/token files находятся вне Git/evidence,
  доступны только владельцу процесса. Логи не содержат credentials или полного Config.
- Для PLAINTEXT используйте доверенную изолированную сеть. Отдельно испытайте SSL,
  SASL_PLAINTEXT/PLAIN, SASL_SSL/PLAIN и GSSAPI. Kerberos требует KDC, principal,
  keytab/JAAS, синхронизации времени и DNS; без них case помечается NOT_RUN.
- Не запускать CVE exploit/DoS payloads. Проверить vendor fixes/configuration
  upstream/server CVE отдельно; Client 4.4.0 не исправляет старый broker.

## Порядок read-only испытаний

Перед каждым R-case: разрешённое подключение к стенду, identity gate, read-only
credentials и исходный snapshot. Все ресурсы для R-фазы заранее создаёт оператор
по отдельному разрешению; plan их не создаёт. Наблюдайте mutating API через
broker audit logs/контролируемый API proxy, поскольку сравнение итогового состояния
само по себе не доказывает отсутствие попытки записи.

Базовый шаблон команды для последующего исполнения:

```powershell
java -jar build/libs/kafka-gitops-all.jar --command-config TEST-client.properties --file TEST-state.yaml --no-delete --skip-acls plan --output RUN-plan.json
```

| ID | Предпосылки и действие | Критерий приёмки / evidence |
|---|---|---|
| R01 | Локально `--help`, version, validate до подключения | Java 21, expected hash; все help/validate exit 0, без Kafka IO |
| R02 | Identity probe, безопасный bootstrap, standard read APIs | Ожидаемые cluster ID/topic UUID/broker IDs/racks; конечный timeout; нет writes |
| R03 | Конфликтующие env/file settings; файл выигрывает; затем env без файла | Выбран правильный стенд; секреты отсутствуют в stdout/stderr |
| R04 | Bootstrap отсутствует, файл отсутствует/повреждён | Exit 2, BOOTSTRAP_MISSING/COMMAND_CONFIG_UNREADABLE, без скрытого localhost |
| R05 | Изолированная DNS/refused/unavailable fault, плохой SASL/TLS, отозванные read ACL | Exit 2 и соответствующая safe category; если Kafka свёл причину к timeout, честный TIMEOUT; нет ложного YAML error/SUCCESS |
| R06 | Существующий topic; change ADD/UPDATE/REMOVE config, effective defaults, previousValue | Current/desired совпадают с независимым read; plan readonly; JSON корректен |
| R07 | Partitions unchanged/increase/decrease | NO_CHANGES либо корректный partitionPlan; decrease BLOCKED, без createPartitions |
| R08 | Два brokers, racks 1/2, четыре partitions `[0]`, leader/ISR `[0]`, RF 1 → 2 | Если критические proofs полны — четыре `[0,1]`; иначе точный UNKNOWN/BLOCKED; WARNING capacity/admission не выдаётся за guarantee |
| R09 | Три brokers; RF 2 → 3, 3 → 2, 3 → 1 и смешанный RF | Ordered prefix/subsequence, leader retained; decrease виден без allow; JSON не содержит permission |
| R10 | Недостаточно brokers, отсутствует rack/подходящий ID, cordoned/fenced/storage fault, unknown features | Нет выдуманных значений; причины по brokers; critical UNKNOWN/UNSAFE блокирует экспорт |
| R11 | Известный ACL inventory, `--skip-topics`; затем пропуск ACL/topics, include/exclude, no-delete | Прежняя ACL-семантика; нет create/delete ACL; flags влияют на ожидаемый scope |
| R12 | Внешний timeout/interrupt и частичная доступность metadata | Завершение в пределах бюджета; доступные факты перед warnings/blockers; blocked JSON не создан, старый output не перезаписан |
| R13 | Старый JSON с RF; apply с no-manage и обоими skip flags | После отдельного разрешения на безопасную отрицательную команду: exit 2, ноль writes, permission gate не обойдён |

Для metadata collection фиксируйте timeout settings; нормальный planning budget
по умолчанию 30 s. Допуск к wall-clock измерению: budget + 5 s на запуск JVM/закрытие,
на выделенном ненагруженном runner. Зависание за этим пределом — FAIL, а не повтор
без анализа. JSON output path уникален для каждого case; провалившийся plan
никогда не даёт права использовать файл предыдущего run.

## Write-испытания только на изолированном стенде

**Каждая строка требует отдельного явного разрешения и свежего cluster-ID gate.**
Перед apply сохраняются desired YAML, JSON SHA-256, current ordered assignments,
leader/ISR, min ISR, features/config proofs, active reassignments и disk baseline.
План после согласования не редактируется вручную.

| ID | Предпосылки / разрешённые операции | Точные критерии и cleanup |
|---|---|---|
| W01 | Пустой согласованный namespace, разрешён CreateTopics | Один тестовый topic с заданными partitions/RF/config; independent read; повторный plan NO_CHANGES; удалить только через W11 |
| W02 | W01, config baseline, разрешён IncrementalAlterConfigs TOPIC | ADD/UPDATE/REMOVE меняют только согласованные keys; previousValue корректен; effective read после apply совпадает; restore отдельным планом |
| W03 | W01, разрешён CreatePartitions | Count возрастает до target, реальные partitions перечитаны; повторный apply без повторной записи; decrease отвергается |
| W04 | R08 успешно, минимум 2 eligible brokers, разрешён AlterPartitionReassignments | RF 1 → 2; точные четыре `[0,1]`, полный target ISR, leader в ISR, active reassignment отсутствует; `ACCEPTED/RUNNING` не успех |
| W05 | 3 eligible brokers и полные critical proofs | RF 2 → 3, затем независимый snapshot точного target; отсутствие hidden rebalance; исходный план неизменён |
| W06 | RF 3, healthy ISR, разрешён decrease и CLI allow | RF 3 → 2 и отдельным разрешением 3 → 1; leader retained; exact ordered subsequence; no-delete/без allow → exit 2 и ноль writes |
| W07 | Управляемый API fault proxy/test-only permissions, минимум две partitions | Одно per-partition future принято, другое отклонено; общий exit 2, per-partition outcomes сохранены, последующие batches не отправлены, rollback/cancellation отсутствуют |
| W08 | Начатый разрешённый reassignment, контролируемая остановка одного broker | Сначала RUNNING/UNKNOWN либо отказ; после разрешённого восстановления fresh metadata, принятую работу не отправлять повторно; completed только при target/full ISR/leader/no active |
| W09 | План, затем отдельно разрешённая смена UUID/assignments/ISR/cluster fixture | Apply BLOCKED до writes; для drift после preflight — остановка последующих writes, честный partial/unknown outcome; чужой reassignment не отменён |
| W10 | Частично завершённый либо полностью выполненный прежний apply | Fresh read и новые permissions; уже выполненная работа наблюдается без повторной отправки, оставшаяся разрешённая работа выполняется; нет ложного SUCCESS |
| W11 | Согласованный manifest созданных ресурсов, нет чужих active reassignments, разрешены DeleteTopics/DeleteAcls | Повторный cluster-ID gate; удалить только ресурсы run manifest; сверить отсутствие; credentials отозвать; evidence сохранить без секретов |
| W12 | Отдельный ACL fixture/полный desired inventory, разрешены CreateAcls/DeleteAcls | Прежние READ/WRITE/CREATE и только явно заданный DESCRIBE; exact binding comparison; не удалять чужие bindings; cleanup по manifest |

Никаких write на production в этой программе не разрешено. На production без
отдельного разрешения нельзя испытывать create/delete topics/ACL, partitions,
configs, reassignment/RF decrease, throttles, остановку broker, fault injection,
изменение features/credentials и cleanup. Read-only подключение к production
тоже требует отдельной авторизации владельца.

W07 требует контролируемой по-RPC инъекции либо специально подготовленных прав;
обычное выключение broker не гарантирует нужного порядка futures. W08 требует
достаточного transfer volume или изолированного согласованного throttling,
иначе RUNNING может не наблюдаться. W09 требует управляемого interleaving hook;
без него конкретная гонка отмечается NOT_RUN. Нельзя заменять эти испытания
предположением о том, что нужная гонка произошла.

## Протокол evidence и условия выхода

Для каждого case сохраняются version/mode/features, run и cluster ID, redacted
command, timeout, start/end, exit code, JSON hash, before/after read, API audit,
предупреждения и пер-partition outcomes. Пароли, inline JAAS, токены, keytabs,
private keys, исходные secret Configs не попадают в evidence. Частичные изменения
сначала инвентаризируются; автоматического rollback и отмены чужой работы нет.

Read-only gate пройден, когда во всех выполненных R-cases mutating API attempts = 0,
факты достоверны, ошибки ограничены временем и секреты не раскрыты. Write gate
пройден отдельно по каждому W-case при exact verification, корректном exit code,
сохранении safety invariants и согласованном cleanup. FAIL блокирует следующие
зависимые cases. NOT_RUN не считается PASS; для неподтверждённых возможностей
нельзя писать SUPPORTED или production-ready.

Готовность проекта к дальнейшему решению о production оценивается только после
этой матрицы, анализа серверных CVE/backports, security review credentials и
подтверждения эксплуатационных capacity/admission ограничений владельцем стенда.
