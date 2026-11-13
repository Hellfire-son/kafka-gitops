<!-- SPDX-License-Identifier: Apache-2.0 -->
> Historical stage evidence: the dates, versions, hashes and test outcomes below describe that stage and artifact. Personal machine paths were anonymized for publication. See [0.4.0 preparation](../release/v0.4.0-release-preparation.md) for the current release status.

# Финальный аудит зависимостей Kafka GitOps после 4A, 4B и 4C

Дата проверки: **09.11.2026**. Ветка `feature/ads-gitops`, HEAD `24d19e5ed22b5568875f2408a027465d8123b374`. До аудита рабочее дерево чистое. Основа — joschi/kafka-gitops 0.3.1 с исправлениями безопасного логирования и previousValue. Для сравнения прочитан [первоначальный отчёт Stage 3](audit-report-2026-11-09.md); его содержимое сохранено.

**Решение:** обновлённый стек пригоден для дальнейшей локальной разработки этапов №5 и №6 с изолированными модульными тестами. **Безусловная готовность к production не подтверждена.** Нулевой результат OSV для runtime не учитывает все опубликованные advisories: Kafka Client 3.9.2 остаётся version-affected по CVE-2026-111551; Shadow 8.3.11 — по CVE-2026-75598. Кроме того, остаются уязвимые библиотеки сборки. Заявленные брокеры ADS Kafka 4.3.1 входят в upstream-диапазоны нескольких серверных CVE, включая CVE-2026-114349. Наличие backport-патчей Arenadata не установлено.

## 1. Объём и методика

Повторно разрешены compileClasspath, runtimeClasspath, testCompileClasspath, testRuntimeClasspath, annotationProcessor, jacocoAgent, jacocoAnt, classpaths Gradle-плагинов проекта и settings. Проанализированы все 310 JAR дистрибутива Gradle 8.14.6, метаданные встроенных компонентов и фактический Shadow JAR. Это аудит идентифицированных компонентов; точные версии некоторых shaded/native компонентов пока неизвестны.

Сначала выполнены `stage4Inventory help --offline --no-daemon --no-scan --warning-mode all -Dorg.gradle.deprecation.trace=true` через временный init script, затем `stage4Inventory --no-daemon --no-scan` с разрешением недостающего JaCoCo Ant из Maven Central. Все задачи успешны. `CI` очищен для процесса, build scans и Sonar analysis не запускались. Offline- и online-инвентаризации совпадают по всем ранее разрешённым графам и SHA-256 артефактов; добавлена только ранее недоступная конфигурация jacocoAnt. Исходники и Gradle-конфигурация не менялись.

Свежая онлайн-проверка: **221 текущая уникальная Maven-координата с версией**, включая embedded components; вместе с 14 дополнительными историческими версиями для сравнения — **235**. OSV querybatch и полные OSV records сопоставлены с GitHub Advisory API; отозванные записи исключены. Дополнительно прочитаны 21 advisory Gradle, advisory Shadow, 32 advisory производителей компрессии, 18 jackson-databind и 10 jackson-core, Kafka Security/CNA и NVD для трёх обязательных CVE. Публичным сервисам передавались только публичные координаты/версии/CVE. Конфигурации подключения, секреты и исходники проекта не передавались.

Различаются три утверждения: наличие версии в affected range; наличие нужного класса/функции в поставке; достижимость функции через конкретный сценарий приложения. Отрицательный поиск вызова не равен доказательству отсутствия пути во всех возможных окружениях. Severity производителя/CVSS сохранена отдельно от оценки реальной применимости.

Сетевые источники доступны; NVD API вернул все три записи. HTML NVD не дал читаемого содержимого через браузерный инструмент, поэтому использован официальный JSON API. Несколько альтернативных URL changelog/advisory вернули 404; это не заменялось вымышленными результатами: для них использованы доступные CVE/GHSA и ссылки на исправления. Начальные отказы sandbox при обращении к кешу/сети устранены разрешёнными повторными командами; они не являются результатом сканирования.

## 2. Инвентаризация и сравнение версий

| Конфигурация | Физические artifacts | Результат |
| --- | --- | --- |
| compileClasspath | 12 | Разрешена; без запуска задач использования |
| runtimeClasspath | 14 | Разрешена; без запуска задач использования |
| testCompileClasspath | 28 | Разрешена; без запуска задач использования |
| testRuntimeClasspath | 32 | Разрешена; без запуска задач использования |
| annotationProcessor | 1 | Разрешена; без запуска задач использования |
| jacocoAgent | 1 | Разрешена; без запуска задач использования |
| jacocoAnt | 7 | Разрешена; без запуска задач использования |
| buildClasspath | 19 | 19 физических artifacts; 8 дополнительных embedded Maven versions |
| settingsClasspath | 4 | Разрешена |
| Gradle 8.14.6 | 310 | 153 идентифицированные Maven-версии, включая bundled metadata; внутренние JAR отдельно в файле-инвентаризации |

Platform `com.fasterxml.jackson:jackson-bom:2.21.8` присутствует в resolution graph, но не является физическим JAR. Аннотации **2.21** — штатная версия этого BOM; это не смешивание несовместимых Jackson patch releases.

| Компонент | Stage 3 | После 4B | После 4C / 4D | Область |
| --- | --- | --- | --- | --- |
| Jackson BOM | 2.17.1 | 2.21.8 | 2.21.8 | Platform, не JAR |
| org.lz4:lz4-java ? at.yawk.lz4:lz4-java | 1.8.0 | 1.8.0 | 1.14.0 | Транзитивная / constraint |
| ch.qos.logback:logback-classic | 1.5.6 | 1.6.6 | 1.6.6 | Прямая |
| ch.qos.logback:logback-core | 1.5.6 | 1.6.6 | 1.6.6 | Прямая |
| com.fasterxml.jackson.core:jackson-annotations | 2.17.1 | 2.21 | 2.21 | Транзитивная / constraint |
| com.fasterxml.jackson.core:jackson-core | 2.17.1 | 2.21.8 | 2.21.8 | Транзитивная / constraint |
| com.fasterxml.jackson.core:jackson-databind | 2.17.1 | 2.21.8 | 2.21.8 | Прямая |
| com.fasterxml.jackson.dataformat:jackson-dataformat-yaml | 2.17.1 | 2.21.8 | 2.21.8 | Прямая |
| com.fasterxml.jackson.datatype:jackson-datatype-jdk8 | 2.17.1 | 2.21.8 | 2.21.8 | Прямая |
| com.github.luben:zstd-jni | 1.5.5-6 | 1.5.5-6 | 1.5.7-26 | Транзитивная / constraint |
| info.picocli:picocli | 4.7.5 | 4.7.7 | 4.7.7 | Прямая |
| org.apache.kafka:kafka-clients | 3.7.0 | 3.7.0 | 3.9.2 | Прямая |
| org.slf4j:slf4j-api | 2.0.13 | 2.0.20 | 2.0.20 | Прямая |
| org.xerial.snappy:snappy-java | 1.1.10.5 | 1.1.10.5 | 1.1.10.12 | Транзитивная / constraint |
| org.yaml:snakeyaml | 2.2 | 2.5 | 2.5 | Транзитивная / constraint |

Все runtime-компоненты реально присутствуют в Shadow JAR. Столбец Stage 4C соответствует также повторной проверке 4D. Обновления подтверждены не только декларациями build.gradle, но и resolved artifacts и побайтовым сравнением классов/native resources.

| Дополнительный компонент | Stage 3 | Stage 4D | Примечание |
| --- | --- | --- | --- |
| cglib:cglib-nodep | 3.3.0 | 3.3.0 | Без изменения |
| com.github.stefanbirkner:system-rules | 1.19.0 | 1.19.0 | Без изменения |
| com.vaadin.external.google:android-json | 0.0.20131108.vaadin1 | 0.0.20131108.vaadin1 | Без изменения |
| junit:junit | 4.13.2 | 4.13.2 | Без изменения |
| org.apache.groovy:groovy | 4.0.21 | 4.0.21 | Без изменения |
| org.hamcrest:hamcrest-core | 1.3 | 1.3 | Без изменения |
| org.hamcrest:hamcrest | 2.2 | 2.2 | Без изменения |
| org.junit.jupiter:junit-jupiter-api | 5.10.2 | 5.10.2 | Без изменения |
| org.junit.jupiter:junit-jupiter-engine | 5.10.2 | 5.10.2 | Без изменения |
| org.junit.jupiter:junit-jupiter-params | 5.10.2 | 5.10.2 | Без изменения |
| org.junit.jupiter:junit-jupiter | 5.10.2 | 5.10.2 | Без изменения |
| org.junit.platform:junit-platform-commons | 1.10.2 | 1.10.2 | Без изменения |
| org.junit.platform:junit-platform-engine | 1.10.2 | 1.10.2 | Без изменения |
| org.junit.vintage:junit-vintage-engine | 5.10.2 | 5.10.2 | Без изменения |
| org.opentest4j:opentest4j | 1.3.0 | 1.3.0 | Без изменения |
| org.skyscreamer:jsonassert | 1.5.1 | 1.5.1 | Без изменения |
| org.spockframework:spock-core | 2.3-groovy-4.0 | 2.3-groovy-4.0 | Без изменения |
| org.spockframework:spock-junit4 | 2.3-groovy-4.0 | 2.3-groovy-4.0 | Без изменения |
| org.inferred:freebuilder | 2.8.0 | 2.8.0 | Build-time compileOnly/annotationProcessor, не runtime/test-only |
| org.jacoco:* (agent/ant/core/report) | 0.8.11 | 0.8.11 | Build/test instrumentation; ASM 9.6 в jacocoAnt |

Test-only зависимости не включены в исполняемый JAR. Идентифицирован также встроенный `cglib:cglib:3.3.0` внутри cglib-nodep; точная версия repackaged ASM не подтверждена. Устаревший тестовый стек — отдельный риск поддержки Java 21, даже при отсутствии совпадений OSV.

| Компонент | Stage 3 | Stage 4D | Совместимость/остаток |
| --- | --- | --- | --- |
| Gradle Wrapper/core plugins | 8.7 | 8.14.6 | Java 21 поддерживается; core fixes применены; библиотеки дистрибутива проверены отдельно |
| Shadow | com.github.johnrengelman.shadow 8.1.1 | com.gradleup.shadow 8.3.11 | Совместим с Gradle 8.14.6, но CVE-2026-75598 остаётся |
| org.inferred.processors | 3.7.0 | 3.7.0 | Устаревшие Convention/IDE API, blocker Gradle 9 |
| org.sonarqube | 5.0.0.4638 | 5.0.0.4638 | Работает при конфигурации; scanner embedded CVE; analysis не запускался |
| com.gradle.enterprise / Develocity | 3.17.2 | 3.17.2 | Deprecated ID; CI publishAlways требует review |
| org.gradle.toolchains.foojay-resolver-convention | 0.8.0 | 0.8.0 | Работает здесь; до Gradle 9 проверить и модернизировать |
| java/groovy/application/idea/jacoco | Gradle 8.7 | Gradle 8.14.6 | Core plugins; JaCoCo tool отдельно закреплён 0.8.11 |

Java 21 сохранена. JVM Gradle — **Eclipse Adoptium/Temurin 21.0.13**; compiler toolchain по build.gradle остаётся **AZUL**, в 4C использовался cached Zulu 21. Это два разных выбора JVM. Ограничение поставщика создаёт зависимость от кеша/Foojay и мешает одинаковому поведению Temurin-only offline CI; исправление требует отдельной задачи, в этом аудите не выполнялось.

### Библиотеки сборки

| buildClasspath artifact | Stage 3 | Stage 4D |
| --- | --- | --- |
| com.fasterxml.woodstox:woodstox-core | Не было | 7.1.1 |
| com.gradleup.shadow:shadow-gradle-plugin | 8.1.1 (старые координаты) | 8.3.11 |
| commons-io:commons-io | 2.11.0 | 2.19.0 |
| gradle.plugin.org.inferred:gradle-processors | 3.7.0 | 3.7.0 |
| org.apache.ant:ant-launcher | 1.10.13 | 1.10.15 |
| org.apache.ant:ant | 1.10.13 | 1.10.15 |
| org.apache.logging.log4j:log4j-api | 2.20.0 | 2.25.3 |
| org.apache.logging.log4j:log4j-core | 2.20.0 | 2.25.3 |
| org.apache.maven:maven-api-annotations | Не было | 4.0.0-rc-5 |
| org.apache.maven:maven-api-xml | Не было | 4.0.0-rc-5 |
| org.apache.maven:maven-xml | Не было | 4.0.0-rc-5 |
| org.codehaus.groovy:groovy-backports-compat23 | 2.3.5 | 2.3.5 |
| org.codehaus.plexus:plexus-utils | 3.5.1 | 4.0.2 |
| org.codehaus.plexus:plexus-xml | Не было | 4.1.1 |
| org.codehaus.woodstox:stax2-api | Не было | 4.2.2 |
| org.jdom:jdom2 | 2.0.6.1 | 2.0.6.1 |
| org.sonarsource.scanner.api:sonar-scanner-api | 2.16.2.588 | 2.16.2.588 |
| org.sonarsource.scanner.gradle:sonarqube-gradle-plugin | 5.0.0.4638 | 5.0.0.4638 |
| org.vafer:jdependency | 2.8.0 | 2.16 |
| com.google.code.gson:gson | Без изменения | 2.10.1 |
| com.gradle:develocity-gradle-plugin | Без изменения | 3.17.2 |
| com.gradle:gradle-enterprise-gradle-plugin | Без изменения | 3.17.2 |
| org.gradle.toolchains:foojay-resolver | Без изменения | 0.8.0 |

Дополнительно найдены встроенные библиотеки: Woodstox 7.1.1 содержит xsdlib 2022.7 и isorelax 20090621; Sonar Scanner API 2.16.2.588 — OkHttp/okhttp-urlconnection 3.14.2, Okio 1.17.2, minimal-json 0.9.5 и scanner-api-batch-interface 2.16.2.588; jdependency 2.16 — Commons IO 2.22.0 и shaded ASM с неподтверждённой точной версией. Встроенный Commons IO 2.22.0 и обычный 2.19.0 имеют разные namespaces; это не две версии runtime Commons IO приложения.

В Gradle 8.14.6, среди прочего: Guava 33.4.6-jre; Commons Lang 2.6 **и** Lang3 3.18.0; Jackson 2.18.11; Groovy 3.0.25; Kotlin 2.0.21; Commons Compress 1.26.1; Commons IO 2.15.1; jsoup 1.23.2; Bouncy Castle jdk18on 1.85; JGit 5.13.5.202508271544-r; MINA SSHD 2.15.0; SLF4J 1.7.36. Это classloader среды сборки, не runtime приложения. Наличие исправленного Lang3 не устраняет отдельный legacy Lang 2.6.

## 3. Сопоставление результатов 37 → 15 → 0

| Срез | Runtime: уникальные активные OSV/GHSA version matches | Изменение |
| --- | ---: | --- |
| Stage 3: Kafka 3.7.0, Jackson 2.17.1, Logback 1.5.6 | **37** | Исходное состояние |
| После 4A | **37** | Runtime не менялся; обновилась среда сборки |
| После 4B | **15** | Устранены 22 совпадения: 16 Jackson core/databind и 6 Logback core |
| После 4C | **0** | Устранены 15 оставшихся OSV runtime matches Kafka/compression |
| Повторная проверка 4D | **0** | Свежий запрос воспроизвёл 37 и 15 на исторических версиях, 0 на текущих |

37 — число уникальных advisories, а не dependency occurrences, файлов или доказанных эксплуатируемых CVE. Stage 3 дополнительно имел **44** OSV records для проекта/плагинов и **59** с библиотеками Gradle 8.7. Поэтому сравнивать старые полные 59 с нынешним runtime 0 некорректно.

Текущий расширенный OSV/GitHub результат: **10 уникальных активных version matches вне runtime** — 8 в build/plugin artifacts с учётом embedded Sonar и 2 в дистрибутиве Gradle. Settings и test-only — 0 найденных совпадений для идентифицированных версий. Из 10 один флаг Log4j bridge требует снятия по фактическому составу, см. ниже. Ещё **2 Jackson build-time advisories**, Shadow и Kafka ListDeserializer выявлены по первоисточникам вне этого OSV-результата.

Совместная текущая/историческая выборка дала 47 активных OSV/GHSA records; GitHub вернул 49, включая два withdrawn дубликата Snappy: `GHSA-94x4-5g3f-jjxp`, `GHSA-wm36-87w8-wg22`. Это объясняет прежние 39 raw/37 active и 17 raw/15 active. Эти дубликаты не считаются отдельными оставшимися уязвимостями. Разные snapshots, неполные package mappings, repository advisories и задержки включения в OSV могут менять числа независимо от кода.

Полная поимённая сверка исходных 37 приведена в приложении A. Дополнительные vendor-записи компрессии проверены отдельно; текущие LZ4 1.14.0, Snappy 1.1.10.12, Zstd 1.5.7-26 вне затронутых диапазонов всех 32 полученных записей. Это включает новые Zstd GHSA с fix 1.5.7-26 и Snappy issues, не входившие в прежние 15. Отсутствие совпадений не покрывает неизвестные CVE и все native C/C++ компоненты.

## 4. Runtime vulnerabilities

### CVE-2026-111551 — остаётся в Kafka Client 3.9.2

| Поле | Результат |
| --- | --- |
| Затронутые версии | 0.10.0.0–4.1.2; 4.2.0–4.2.1; 4.3.0–4.3.1. Текущий 3.9.2 входит в диапазон |
| CVSS | **7.5, v3.1**, Secondary CISA в NVD; Apache: важная проблема |
| Условия | Недоверенные записи, декодируемые ListDeserializer/ListSerde; для kafka-streams — отдельные внутренние deserialization paths |
| Воздействие | Размер/счётчик из записи вызывает чрезмерную аллокацию и OOM; ошибка может повторяться после рестарта |
| Текущая применимость | ListDeserializer находится в kafka-clients, но приложение создаёт только AdminClient. Вызовы Consumer/Producer/ListSerde не найдены; kafka-streams отсутствует. Модель KafkaStreamsService в YAML не является Kafka Streams runtime |
| Исправление | **Client 4.2.2 / 4.3.2 / 4.4.0**; опубликованного 3.x fix в проверенных источниках нет |
| Риск | Уязвимая версия подтверждена; достижимость через текущий Admin-only CLI не установлена, практический риск ограничен этой архитектурой |
| Снижение риска | Сохранить Admin-only scope; не добавлять Consumer/ListSerde без пересмотра; оформить временное VEX/exception с владельцем и сроком, либо получить поддерживаемый backport |
| До production | Нужна исправленная версия или утверждённое исключение на основании ограниченной достижимости. Сканерный флаг нельзя закрывать заявлением «OSV=0» |

Источники: [Apache CNA](https://www.cve.org/CVERecord?id=CVE-2026-111551), [NVD](https://nvd.nist.gov/vuln/detail/CVE-2026-111551). Исправление брокера само по себе не заменяет клиентскую библиотеку; удалять классы ради скрытия scanner finding не предлагается.

### Что исправлено и какие настройки остаются значимыми

| Запись | Состояние после 4C | Применимость и действие |
| --- | --- | --- |
| [CVE-2026-33558](https://www.cve.org/CVERecord?id=CVE-2026-33558), NetworkClient DEBUG secrets | Исправлена в 3.9.2 | AdminClient использует NetworkClient. В 4C 15 проверок request/response/builder подтверждали redaction и неизменность исходных данных. Raw protocol Data.toString() и произвольный новый пользовательский лог автоматически безопасными не становятся |
| [CVE-2026-35554](https://www.cve.org/CVERecord?id=CVE-2026-35554), producer misrouting | Исправлена в 3.9.2 | Producer текущим CLI не создаётся |
| [CVE-2025-27817](https://www.cve.org/CVERecord?id=CVE-2025-27817), OAuth URLs | Версия исправлена начиная с 3.9.1 | В 3.x URL allowlist по умолчанию разрешает всё; при OAuth задать явный `org.apache.kafka.sasl.oauthbearer.allowed.urls`. Это hardening, а не новый version match |
| [CVE-2024-31141](https://www.cve.org/CVERecord?id=CVE-2024-31141), client ConfigProviders | 3.9.2 вне affected range | Connection properties должны быть доверенными. Если automatic providers не нужны, рассмотреть `org.apache.kafka.automatic.config.providers=none`; не менять рабочую конфигурацию без проверки |
| Jackson/Logback CVE из Stage 3 и дополнительные CNA | Указанные runtime версии исправлены | Включая Jackson CVE-2026-110879 и Logback CVE-2026-13006/19880/104721. В Gradle остаётся отдельный Jackson 2.18.11 — см. build-time |

Приложение сохраняет allowlist безопасных connection diagnostics; SASL JAAS/SSL/OAuth secrets не включаются в него. TopicConfigPlanSerializer/TopicConfigSecurity учитывают sensitive и не экспортируют секреты как value/previousValue. Механизм задач №1 и №2 не изменялся. Статическая проверка не гарантирует безопасность произвольных внешних Logback XML, callback/login modules, exception messages сторонних библиотек и JVM diagnostics: они требуют контроля deployment configuration. Штатный verbose оставляет Kafka logger на WARN; внешние DEBUG overrides нужно отдельно оценивать.

## 5. Build-time vulnerabilities

Эти компоненты не входят в runtime JAR Kafka GitOps, но могут влиять на целостность сборки, секреты CI и журналы. «В текущей конфигурации путь не обнаружен» не означает, что версия исправлена. В колонке production речь о допуске **цепочки сборки** производственного артефакта.

| CVE | Обнаруженная версия / диапазон | Severity | Условие и применимость | Исправление | Mitigation и production gate |
| --- | --- | --- | --- | --- | --- |
| [CVE-2025-67030](https://www.cve.org/CVERecord?id=CVE-2025-67030) | Plexus Utils 4.0.2; <3.6.1 / 4.0.0–4.0.2 | CVSS не опубликован; GH High | Недоверенный архив через Expand.extractFile; traversal/запись файлов. Вызов текущими задачами не доказан | 3.6.1 / 4.0.3 | Доверенные build inputs, безопасная распаковка; обновить через поддерживаемый plugin graph. До production — исправление либо обоснованный запрет пути |
| [CVE-2026-34477](https://www.cve.org/CVERecord?id=CVE-2026-34477) | Log4j core 2.25.3; 2.12.0–2.25.3 | 6.3 v4 | TLS SMTP/Socket/Syslog с verifyHostName XML attribute и MITM; таких appenders здесь не найдено | 2.25.4 | Ограничить logging configuration/network appenders; обновить сборочный plugin. Production CI: исключение только с проверенной конфигурацией |
| [CVE-2026-34478](https://www.cve.org/CVERecord?id=CVE-2026-34478) | Log4j core 2.25.3; 2.21.0–2.25.3 | 6.9 v4 | Прямой Rfc5424Layout со старыми атрибутами; CRLF/log framing. В проекте не настроен. Не присутствовал в исходной 2.20.0 — новый version match | 2.25.4 | Не использовать затронутый layout/attributes; plugin update. Production: закрыть либо документировать недостижимость |
| [CVE-2026-34480](https://www.cve.org/CVERecord?id=CVE-2026-34480) | Log4j core 2.25.3; до 2.25.4 | 6.9 v4 | Недоверенные XML-forbidden символы в XmlLayout; потеря/повреждение build log events, штатного layout нет | 2.25.4 | Контролировать layouts; обновить plugin. Для production CI — исправление либо доказанное отсутствие пути |
| [CVE-2026-49844](https://www.cve.org/CVERecord?id=CVE-2026-49844) | Log4j API 2.25.3; 2.13.1–2.25.4 | 6.3 v4 | MapMessage.asJson с NaN/Infinity → invalid JSON; такой logging path не найден | 2.25.5 / 2.26.1 | Контроль MapMessage/layouts; синхронный plugin Log4j upgrade. Production CI: закрыть либо обосновать неприменимость |
| [CVE-2026-34479](https://www.cve.org/CVERecord?id=CVE-2026-34479) | Scanner относит к core 2.25.3 | 6.9 v4 | Фактически нужен log4j-1.2-api/Log4j1XmlLayout; artifact и класс отсутствуют. Не считать подтверждённой проблемой текущего состава | Bridge 2.25.4 при его наличии | Снять finding по фактическому составу; не добавлять legacy bridge. Отдельного production fix здесь не требуется |
| [CVE-2026-102554](https://www.cve.org/CVERecord?id=CVE-2026-102554) | Guava 33.4.6-jre в Gradle; 4.0–33.7.1 | 8.2 v4 CNA; 5.9 v3 GH | Java deserialization недоверенного Guava collection → OOM. Core pre-auth Gradle пути исправлены, прочие пути не доказаны | 33.7.2-jre | Изолированный CI/кеш, без недоверенной serialization; поддерживаемый Gradle с исправленным Guava. До production pipeline — fix либо scoped exception |
| [CVE-2025-48924](https://www.cve.org/CVERecord?id=CVE-2025-48924) | Commons Lang 2.6 в Gradle; legacy 2.0–2.6 | 6.5 v3 GH | ClassUtils.getClass с чрезмерно длинным именем → StackOverflowError; путь с untrusted input не установлен | В legacy 2.x fix нет; Lang3 ≥3.18.0 | Gradle без legacy компонента либо исключение по недостижимости. Уже имеющийся Lang3 3.18.0 не заменяет 2.6. Для production CI решение обязательно |
| [CVE-2026-110879](https://www.cve.org/CVERecord?id=CVE-2026-110879) | Jackson databind 2.18.11 в Gradle; >=2.10,<2.18.12 | 9.8 v3 CNA | Default/class-name polymorphism и недоверенный JSON → validator bypass/инстанцирование классов. Нужная конфигурация Gradle не установлена; не объявлять доказанным RCE сборки | 2.18.12; в runtime уже 2.21.8 | Поддерживаемый Gradle с patched Jackson, доверенные inputs и запрет опасного polymorphism; до production pipeline fix либо проверенное исключение |
| [CVE-2026-54516](https://www.cve.org/CVERecord?id=CVE-2026-54516) | Jackson databind 2.18.11 в Gradle по vendor advisory/release notes | 5.3 v3 vendor | Renamed getter + ignored setter + INFER_PROPERTY_MUTATORS → запись закрытого поля. Модели/путь в Gradle не установлены | 2.18.12; runtime 2.21.8 исправлен | Обновление Gradle, проверка read-only DTO. CNA перечисляет только 2.21/3.x, vendor дополнительно 2.0–2.18.11: расхождение сохранено. Production: оценить путь/exception |
| [CVE-2021-0341](https://www.cve.org/CVERecord?id=CVE-2021-0341) | OkHttp 3.14.2, shaded в Sonar Scanner API; GH <4.9.2 | 7.5 v3 GH | Ошибочная hostname verification при особом hostname/certificate. CNA исходно описывает Android; эксплуатация в конкретном shaded scanner на JDK21 не доказана | 4.9.2+ для upstream OkHttp | Обновить Sonar/scanner, который реально заменит встроенный код; trusted endpoint/HTTPS/egress. Обычный Gradle constraint shaded код не заменяет. Перед production Sonar CI — review/fix/exception |
| [CVE-2023-3635](https://www.cve.org/CVERecord?id=CVE-2023-3635) | Okio 1.17.2, shaded в Sonar; <1.17.6 / 2.x–<3.4.0 | 5.9 v3 GH | Malformed GZIP/XLEN при чтении ответа → исключение/DoS scanner. Нужен недоверенный сжатый ответ; Sonar в 4D не запускался | 1.17.6 / 3.4.0 | Обновить содержащий scanner; trusted server/egress. Production analysis: fix либо ограниченное документированное исключение |

### CVE-2026-75598: уточнение прежней оценки Shadow

Shadow **8.3.11 < 9.6.0**, CVSS **6.3 v3.1**; исправление — **9.6.0+**. Условие — вредоносная зависимость с опасными именами ZIP entries; результат может стать архивом для записи файлов вне каталога при небезопасной downstream-распаковке. Текущий JAR не содержит таких entries, но это не исправляет plugin.

Repository advisory упоминает защиту Gradle ZipFileTree на новых Gradle. **Для 8.3.11 нельзя считать обновление Gradle универсальным mitigation:** `ShadowCopyAction.StreamAction.processArchive()` использует `org.apache.tools.zip.ZipFile` напрямую; `copyArchiveEntry()` переносит имя entry в выходной ZIP. Проверены исходник точного тега 8.3.11 и references в локальных class files. Актуальная CNA прямо уточняет этот сценарий. Предыдущую более широкую трактовку защиты Gradle следует считать уточнённой этим отчётом; старый аудит не переписывался.

До исправления: доверенные зависимости и репозитории, проверенные checksum/lock policy, обязательная проверка входных и выходных архивов на traversal, безопасная распаковка. Приёмка чистого текущего JAR возможна только по принятой политике/исключению; произвольные будущие сборки такой проверкой не покрыты. Предпочтительна согласованная миграция Gradle/Shadow с packaging regression, без ручной замены библиотек Gradle. Shadow 9.6.x требует Gradle ≥9.2 и Java ≥17, поэтому это отдельная задача вместе с устранением старых plugins/conventions. [Advisory](https://github.com/GradleUp/shadow/security/advisories/GHSA-rr8v-fxxf-c986), [CNA](https://www.cve.org/CVERecord?id=CVE-2026-75598), [исходник 8.3.11](https://raw.githubusercontent.com/GradleUp/shadow/8.3.11/src/main/groovy/com/github/jengelman/gradle/plugins/shadow/tasks/ShadowCopyAction.groovy), [матрица Shadow](https://github.com/GradleUp/shadow/blob/main/README.md).

### Что закрыто обновлением Gradle и что нельзя переносить из Stage 3

Gradle **8.14.6** вне диапазонов GHSA-j5m7-59rp-24f5 (repository TLS fallback), GHSA-xwqc-3h47-hg64 и GHSA-mvvg-497x-hmj8 (deserialization до аутентификации daemon/worker), CVE-2026-22865 и CVE-2026-22816. Исправлены core-пути, но это не устраняет все проблемы bundled Guava/Jackson/Lang. `--no-daemon` может запускать single-use daemon и сам по себе не был бы полным исправлением старой версии. [Официальные advisories Gradle](https://github.com/gradle/gradle/security/advisories).

Старые совпадения Commons IO 2.11.0/CVE-2024-47554, Compress 1.25.0/CVE-2024-26308/25710, jsoup 1.15.3/CVE-2026-75140/71497, JGit/CVE-2025-4949, MINA SSHD/CVE-2024-41909 и старого bcprov 1.68 не переносятся на текущие версии автоматически. Проверка текущих идентифицированных версий этих библиотек не дала соответствующих matches. В то же время Plexus 4.0.2 всё ещё требует 4.0.3, а Guava 33.4.6 — 33.7.2-jre.

CVE-2026-3505: Stage 3 имел спорный scanner match bcpg-jdk15on 1.68, тогда как диапазон производителя начинается с 1.74. Сейчас используется **bcpg-jdk18on 1.85**, после исправления 1.84; старое спорное совпадение не является новым активным риском. CVE-2025-27148 затрагивала только Gradle 8.12: нельзя объявлять её «закрытой миграцией с 8.7», где она отсутствовала. [Объяснение Bouncy Castle](https://github.com/bcgit/bc-java/wiki/CVE%E2%80%902026%E2%80%903505).

### Wrapper и supply chain

`gradle-wrapper.properties` указывает 8.14.6, `validateDistributionUrl=true` и официальный distribution SHA-256 **7988ed071b2a07900e2ec715fca15c6ab72bce6db433af301fa7fa7e9407bd24**. SHA-256 локального Wrapper JAR **ad16bca46cb71bb5887c546860d93992aeee35c1fdaf4a242b0a612375d937e0** совпал с официальным endpoint. Это закрывает пробел Stage 3 с отсутствовавшей checksum; повторной загрузки distribution ZIP и полной аттестации установленного дерева в 4D не было. [Distribution checksum](https://services.gradle.org/distributions/gradle-8.14.6-bin.zip.sha256), [Wrapper checksum](https://services.gradle.org/distributions/gradle-8.14.6-wrapper.jar.sha256).

Dependency locking и `gradle/verification-metadata.xml` не обнаружены. Версии в основном фиксированы, но это не защита от подмены кеша/репозитория. Settings всё ещё разрешает `publishAlways()` при наличии CI; в аудите публикация не выполнялась. Privacy/scan policy и Sonar server/JRE provisioning требуют отдельного решения.

## 6. Test-only vulnerabilities и риски поддержки

Для разрешённых test-only Maven версий и дополнительного cglib metadata **совпадений OSV/GHSA не найдено**. Это отдельный результат от runtime и build-time. JaCoCo agent/Ant/core/report 0.8.11 и ASM 9.6 также проверены, новых совпадений нет; offline-пробел Stage 3 по jacocoAnt закрыт без запуска тестов.

Подтверждён **44-классовый overlap** `hamcrest:2.2` и `hamcrest-core:1.3` в testRuntimeClasspath. Это порядок загрузки и надёжность тестов, не доказанная CVE. cglib-nodep 3.3.0 не поддерживается upstream как современная Java-библиотека; system-rules 1.19.0 использует reflection с `--add-opens` на Java 21. Точные embedded ASM/Guava в cglib/FreeBuilder, а также часть shaded кода Develocity/Groovy требуют дальнейшего SBOM. Эти пробелы нельзя считать нулевым количеством уязвимостей. [cglib upstream](https://github.com/cglib/cglib).

Для следующей модернизации: согласованный Groovy 4.x/Spock/JUnit BOM, устранение Hamcrest overlap, оценка удаления cglib и замены EnvironmentVariables Rules. Проверять обнаружение тестов, mocks, очистку окружения и coverage только на явном allowlist. Обычный `test`, `build` и `jacocoTestReport` могут запустить интеграционные сценарии с удалением Kafka-ресурсов.

## 7. Уязвимости ADS Kafka 4.3.1 — вне JAR

Версия **4.3.1 сообщена пользователем**; фактические сборки broker/controller, backports Arenadata и конфигурации не проверялись. Ни клиентское обновление 4C, ни OSV runtime=0 не исправляют брокеры. Ниже upstream-оценка; для ADS нужен ответ производителя по конкретному build.

### Обязательная CVE-2026-114349

| Поле | Результат |
| --- | --- |
| Компонент/диапазон | Broker `kafka_2.13`: 2.4.0–4.1.2; 4.2.0–4.2.1; 4.3.0–4.3.1. Для старых Scala artifacts CNA даёт отдельные диапазоны, общий Kafka advisory начинается с 0.11.0.0. Это не finding Maven kafka-clients |
| CVSS/риск | **6.5 v3.1**, Secondary CISA/NVD; Apache — important. Для доступности кластера практический приоритет высокий |
| Условие | Аутентифицированный producer с Write отправляет маленький CRC-valid сжатый batch с огромным объявленным decompressed record size |
| Последствия | OOM в request-handler/consumer, возможное завершение broker JVM; при чтении сохранённой записи на compacted topic гибнет log-cleaner, compaction может остановиться и повторно ломаться после рестарта |
| Исправление | Broker **4.2.2 / 4.3.2 / 4.4.0** либо эквивалентный подтверждённый vendor patch, **плюс ограниченный `max.decompressed.message.bytes`** на broker/topic уровне |
| Важная настройка | Одного upgrade недостаточно: default лимита практически не ограничивает аллокацию (~2 GiB). Выбирать лимит под максимальную легитимную распакованную запись и запас heap с учётом параллельных обработчиков/cleaners |
| До патча | Write только доверенным producers; `max.message.bytes`, compressed request size и quotas проблему не закрывают |
| До production | Подтверждение Arenadata патча и подходящего лимита обязательно либо формальное временное принятие риска владельцем ADS. Задать новый параметр на неподдерживающей его 4.3.1 вместо патча нельзя |

Источники: [Apache CNA](https://www.cve.org/CVERecord?id=CVE-2026-114349), [NVD](https://nvd.nist.gov/vuln/detail/CVE-2026-114349).

### Другие серверные advisories, значимые для 4.3.1

| CVE | Upstream affected | Условия / воздействие | Риск | Исправление / mitigation / production |
| --- | --- | --- | --- | --- |
| [CVE-2026-112375](https://www.cve.org/CVERecord?id=CVE-2026-112375) | 3.2.0–4.1.2; 4.2.0–4.2.1; 4.3.0–4.3.1 | Аутентификация без ACL; BROKER AlterConfigs разрешает providers до authorization → чтение файлов/env, утечка secrets | Высокий практический; Apache important, числовой CVSS CNA не указан | 4.3.2/эквивалентный backport; least-privilege OS permissions и сетевой доступ. allowlist из 31141 не достаточен. До production исправить/принять явный риск |
| [CVE-2026-111108](https://www.cve.org/CVERecord?id=CVE-2026-111108) | 3.3.0–4.1.2; 4.2.0–4.2.1; 4.3.0–4.3.1 | Некорректные counts в protocol arrays → heap amplification до authorization; на PLAINTEXT без auth возможна unauth атака | Высокий для availability; CNA без числового CVSS | 4.3.2; до патча network isolation, меньший socket.request.max.bytes только снижает риск. До production решить patch/backport |
| [CVE-2026-111115](https://www.cve.org/CVERecord?id=CVE-2026-111115) | 2.4.0–4.1.2; 4.2.0–4.2.1; 4.3.0–4.3.1 | Множество tagged fields в flexible headers, включая ApiVersions до auth → OOM/GC stalls; ACL/quotas не защищают | Высокий для availability; CNA без числового CVSS | 4.3.2; ограничить listeners/network и request size как частичную меру. До production требуется patch/backport либо утверждённый временный риск |
| [CVE-2026-83525](https://www.cve.org/CVERecord?id=CVE-2026-83525) | kafka-raft: 2.8.0–<4.2.2; 4.3.0–<4.3.2 | Malformed FetchSnapshot и CLUSTER_ACTION → падение KRaft controller; нужен доступ/право | Условный; Apache Low, числовой CVSS не опубликован | 4.3.2; изоляция controller listener, минимальные CLUSTER_ACTION grants. До production проверить права и patch policy |
| [CVE-2026-110952](https://www.cve.org/CVERecord?id=CVE-2026-110952) | 3.1.0–<4.4.0; 4.3.1 входит | OAuth/JWKS с unset expected issuer/audience; подпись проверяется, но shared JWKS/keys допускают неверного issuer/audience | Условный; Apache Moderate, без числового CVSS | На 4.3.1 уже можно задать expected issuer и audience по каждому listener; secure defaults — 4.4.0. При OAuth исправить конфигурацию до production; GSSAPI-only путь не затронут |

Для всех перечисленных проблем с fix 4.3.2 также опубликованы 4.2.2/4.4.0; для ADS безопаснее согласовать vendor patch текущей линии, чем автоматически менять major. Числовой CVSS для дополнительных записей, где проверенная CNA публикует только текстовую severity, не придумывался.

CVE-2026-110985 (JWT signature fallback), CVE-2026-111073 (PushTelemetry decompression) и CVE-2026-41115 уже не входят в upstream affected range **4.3.1**; не считать их активными только по слову Kafka. CVE-2026-111496 относится к **kafka-streams**, CVE-2026-116593 и CVE-2026-115644 — к **connect-runtime**: этих модулей нет в JAR/брокерном finding. Если ADS поставляет Connect/Streams 4.3.1, проверить их отдельно: для Streams — timestamp/punctuation hang, для Connect — раскрытие config provider secrets через validate API и обход REST extensions на отдельном admin.listeners; fixes 4.3.2/4.4.0. Временно ограничить доступ к validate, убрать отдельный незащищённый admin listener; это не выполнялось в аудите. [Streams CNA](https://www.cve.org/CVERecord?id=CVE-2026-111496), [Connect secrets CNA](https://www.cve.org/CVERecord?id=CVE-2026-116593), [Connect admin CNA](https://www.cve.org/CVERecord?id=CVE-2026-115644).

## 8. Совместимость Kafka Client 3.9.2 и готовность к 4.x

Для Java AdminClient 3.x ↔ broker 4.x есть официальная протокольная совместимость. В 4C дополнительно сопоставлены JSON API schemas точных тегов 3.9.2 и 4.3.1; у всех 15 рассматриваемых запросов есть пересечение версий, включая createPartitions 0–3, alterPartitionReassignments 0, listPartitionReassignments 0 и incrementalAlterConfigs 0–1. Это необходимое условие, а не проверка реального ADS, TLS/GSSAPI, Ranger или ACL. [Kafka compatibility](https://kafka.apache.org/43/getting-started/compatibility/), [Admin API](https://kafka.apache.org/39/javadoc/org/apache/kafka/clients/admin/Admin.html).

Изменение клиента на 4.x **в этом аудите не выполнялось**. Точный публичный исходник 4.4.0 прочитан как справочный материал, без подключения 4.x dependency/компиляции проекта против неё.

| Java API / файл проекта | Конкретная несовместимость | Работа будущей миграции |
| --- | --- | --- |
| `TopicConfigSecurity.java:22`, `TopicConfig.MESSAGE_FORMAT_VERSION_CONFIG` | Удалена в 4.x; текущая ссылка даст ошибку компиляции | Сохранить при необходимости legacy spelling как собственную строковую константу для старых планов; не переписывать JSON |
| `TopicConfigSecurity.java:23`, `TopicConfig.MESSAGE_TIMESTAMP_DIFFERENCE_MAX_MS_CONFIG` | Удалена в 4.x; второй compile blocker | Перейти на актуальные before/after limits для новых broker configs, отдельно сохранить чтение старых ключей |
| `DescribeTopicsResult.values()` / `.all()` | Deprecated aliases удалены | Для будущих partitions/RF использовать `.topicNameValues()` / `.allTopicNames()`. Сейчас describeTopics в KafkaService не используется |
| `Admin.alterConfigs(...)` | Старый API удалён в 4.x | Сохранить уже используемый `incrementalAlterConfigs`; старый PR нельзя переносить без адаптации |
| `AdminClient` | Сам класс существует; формально не объявлен deprecated в проверенной версии | Интерфейс Admin предпочтителен для новых abstractions; это рекомендация, не существующий compile blocker |
| `DescribeAclsResult.values()` и текущие result `.all()` | Не являются удалёнными aliases DescribeTopicsResult | Существующие ACL/config/create/delete вызовы не объявлять несовместимыми по совпадению имени метода |

`MESSAGE_DOWNCONVERSION_ENABLE_CONFIG` в прочитанном 4.4.0 **сохранена**; её нельзя ошибочно включать в список удалённых констант. [TopicConfig 4.4.0](https://github.com/apache/kafka/blob/4.4.0/clients/src/main/java/org/apache/kafka/common/config/TopicConfig.java), [Admin 4.4.0](https://github.com/apache/kafka/blob/4.4.0/clients/src/main/java/org/apache/kafka/clients/admin/Admin.java), [DescribeTopicsResult 4.3](https://kafka.apache.org/43/javadoc/org/apache/kafka/clients/admin/DescribeTopicsResult.html).

Дополнительно при миграции: Java Client 4.x требует Java ≥11 (Java 21 подходит) и broker ≥2.1; проверить OAuth URL defaults/issuer/audience, custom callbacks/login modules и TLS/GSSAPI. Для известных ADS brokers 4.3.1 минимальная версия протокола не препятствие, но vendor support matrix неизвестна. Удаление ZooKeeper на сервере не является Java API blocker клиента. Не выбирать 4.0.0/4.3.1 как промежуточную «исправленную» цель: 111551 требует 4.2.2/4.3.2/4.4.0. [Upgrade guide](https://kafka.apache.org/43/getting-started/upgrade/).

До перехода получить ADS edition/version/build и backport matrix для всех broker/controller, состояние rolling upgrade/metadata.version, поддерживаемые SASL/SSL callbacks и Ranger/ACL policy, целевые OS/архитектуры и native temp policy. Нужны названия/версии и требования, не значения паролей/keytabs/tokens.

## 9. Проверка Shadow JAR и конфликтов

| Параметр | Stage 3 | Stage 4D |
| --- | --- | --- |
| Размер, bytes | 21 458 844 | 23 673 811 |
| ZIP entries | 6788 | 7307 |
| Классы | 6141 | 6611 |
| Native resources | 46 | 53 |
| Service descriptors | Проверены | 6 |
| SHA-256 | ca41461f438ce383f6f2ca2a7361e074fc4ed77decc1dd5dba5cbdc1fd9d4fb2 | 5cfd7e95c9093f4a6d85735354d2cd981c3119d264246ece67e53f95d66ed28e |
| Main-Class | com.devshawn.kafka.gitops.MainCommand | com.devshawn.kafka.gitops.MainCommand |
| Duplicate class names в runtime | Не проверялись в таком объёме | 0 |
| ZIP path traversal entries | Не обнаружены | 0 |

Все runtime-классы, кроме исключённых module-info, и все native-ресурсы совпали побайтово с разрешёнными исходными artifacts. В runtime нет duplicate classes и одновременно загружаемых разных версий одного GA; присутствует один набор `net.jpountz` из at.yawk LZ4. Старых pom.properties `org.lz4` нет, старый artifact отсутствует в графе. Так как fork сохраняет те же Java packages, одного поиска строки `net.jpountz` для определения версии недостаточно.

Единственный `META-INF/services/org.slf4j.spi.SLF4JServiceProvider` указывает на `ch.qos.logback.classic.spi.LogbackServiceProvider`; competing simple/log4j/reload4j providers не найдены. SLF4J 1.7.36 Gradle и runtime 2.0.20 принадлежат разным classloaders. Manifest Main-Class корректен; команды `java -jar ... --help` и `--version` повторно завершились с кодом 0 и пустым stderr, без создания Kafka clients.

Остаётся прежняя особенность: versioned classes в `META-INF/versions/` есть, а `Multi-Release: true` отсутствует. JVM не использует их как MR-JAR overrides. Не следует добавлять атрибут вслепую — нужны проверки всех объединённых модулей/services/Java 21. Избыточный Manifest Class-Path перечисляет уже встроенные библиотеки; deployment с одноимёнными внешними JAR требует отдельного контроля. Это packaging limitations, не доказанные CVE.

Повторной сборки и запуска тестов в 4D не было. Аудирован **тот же SHA-256 JAR**, который проверен в 4C. Сохранённые результаты 4C: 175/175 unit cases без failures/errors/skips; 156 codec roundtrips (arrays/direct ByteBuffer/Kafka record batches), 30 logging checks до/после и 15 DEBUG protocol renderings. Текущие tracked Java/test sources с тех проверок не изменились. Эти результаты приведены как предыдущая верификация, а не новый запуск 4D.

Linux JNI в 4C не исполнялись: WSL/Docker отсутствовали. Проверены наличие/сохранность Linux native ресурсов и ELF headers; остаются ABI/glibc/musl/noexec/temp permissions и ОС другой архитектуры. Windows amd64 native paths проверялись в 4C. Native C/C++ и контейнерная ОС не полностью покрываются Maven OSV-сканированием.

## 10. Предупреждения Gradle и ограничения

В свежем `help --warning-mode all` подтверждены:

1. `org.inferred.processors:3.7.0`: Convention/JavaPluginConvention и IdeaModule.testSourceDirs, удаляемые в Gradle 9. Следующая миграция — убрать plugin, сохранить native annotationProcessor/compileOnly и IDE generated roots.
2. `build.gradle:14`: ApplicationPluginConvention через mainClassName; перейти на application.mainClass в отдельной задаче.
3. `build.gradle:12`: `group '...'` space assignment; предупреждение о Gradle 10, не непосредственный blocker Gradle 9.
4. `settings.gradle`: старый plugin ID com.gradle.enterprise заменён com.gradle.develocity.

В прошлых unit-запусках также было предупреждение automatic loading test framework implementation dependencies; в текущем help оно не проверялось исполнением test. При обновлении явно задать launcher/engines и убедиться в discovery. Java deprecations двух TopicConfig constants — отдельный переходный риск, не основание менять source в аудите.

Сохраняются пробелы: точный SBOM shaded FreeBuilder/ASM/Develocity/Groovy и native dependencies; отсутствие контейнерного/JRE образного сканирования; непроверенные внешние Logback/JAAS callbacks; ADS vendor patch status. Dockerfile по-прежнему использует **eclipse-temurin:21.0.3_9-jre-jammy**, существенно старее локального Temurin 21.0.13; production image нужно отдельно обновить/зафиксировать digest и просканировать. Образ не загружался, вымышленные container CVE не перечисляются.

## 11. Приёмка и рекомендации

| Область | Решение перед этапами №5/№6 | Условие перед production |
| --- | --- | --- |
| Runtime/parser/logging/планы | Разработку продолжать; текущий стек и результаты 4C дают рабочую базу | Временное exception для 111551 либо исправленный клиент; подтверждение deployment diagnostics/auth policy |
| Build pipeline | Локальную разработку продолжать на неизменённых версиях | Закрыть/обосновать Shadow, Plexus, Guava, build Jackson и embedded Sonar; trusted inputs, защищённый CI и контроль артефакта |
| ADS broker 4.3.1 | Алгоритмы и unit tests разрабатывать без кластера | Arenadata patch/backport подтверждён; исправление 114349 и bounded decompression limit; оценка остальных server CVE обязательна |
| Linux/контейнер | Архитектуру не блокирует | Native/CLI smoke на целевой Linux/JRE и отдельный image scan |
| Stage №5: partitions | Использовать createPartitions/NewPartitions, только увеличение | Контракт, idempotency, concurrent changes, ACL/timeouts; изменение key→partition/order учесть в эксплуатационном плане |
| Stage №6: RF/reassignment | Admin API клиента уже есть; не переносить PR #52 целиком | all() подтверждает принятие, не завершение: polling listPartitionReassignments, describeTopics/ISR, racks, throttles, partial errors/retry/cancel/resume обязательны |

Обязательные дальнейшие security-действия: запросить vendor security matrix ADS и согласовать broker patch; определить сроки client exception/4.x migration; спланировать поддерживаемую цепочку Gradle/Shadow и обновлённый Sonar, которая убирает найденные build-time версии, с повторным сканированием resolved и embedded libraries. Для legacy Commons Lang нужен Gradle без этой библиотеки или документированная недостижимость: подмена lib вручную и соседний Lang3 проблему не закрывают. Разрешения на эти изменения в рамках 4D не использовались, версии не менялись.

Рекомендуется отдельно: dependency verification/locking с проверенным происхождением checksum, SBOM для fat JAR и build toolchain, opt-in build scans, устранение AZUL-only toolchain, модернизация test stack/Hamcrest, packaging MR-JAR и Linux checks. Обновления major Jackson/Groovy/JUnit не нужны для задач №5/№6 сами по себе; смешивать их с partitions/RF изменениями не следует.

Для каждого будущего изменения сохранять regression allowlist: previousValue/старые JSON планы/неизвестные операции, ADD/UPDATE/REMOVE/null/sensitive, --verbose/JAAS/SSL/OAuth, --no-delete/--skip-acls/--skip-topics и include/exclude prefixes. Интеграционные тесты, допускающие удаление ресурсов, требуют отдельного изолированного стенда и отдельной задачи; в этом аудите они не запускались.

## 12. Источники и сохранённые доказательства

Дата всех новых API выборок — **09.11.2026**; записи/версии оцениваются на эту дату, без принятия prerelease за стабильный target. Основные первоисточники: [OSV API](https://google.github.io/osv.dev/api/), [GitHub Advisory REST API](https://docs.github.com/en/rest/security-advisories/global-advisories), [Apache Kafka security](https://kafka.apache.org/community/cve-list/), [NVD API](https://nvd.nist.gov/developers/vulnerabilities), [CVE/CNA](https://www.cve.org/), [Jackson 2.21.8](https://github.com/FasterXML/jackson/wiki/Jackson-Release-2.21.8), [Jackson 2.18.12](https://github.com/FasterXML/jackson/wiki/Jackson-Release-2.18.12), [Logback release/security notes](https://logback.qos.ch/news.html), [SLF4J news](https://slf4j.org/news.html), [Picocli 4.7.7](https://github.com/remkop/picocli/releases/tag/v4.7.7). Для отдельных findings ссылки стоят в соответствующих строках таблиц.

Сырые результаты находятся локально в игнорируемом `.gradle/ads-audit-stage4/`: `inventory.json`, `coordinates.json`, `gradle-jars.json`, `embedded-components.json`, `osv-query.json`/`osv-response.json` и дополнительные embedded/JaCoCo queries, `osv-matches.json`, `osv-advisories.json`, `github-advisories.json`, `cna-records.json`, `nvd-CVE-*.json`, vendor snapshots, `vendor-range-checks.json`, `network-log.json`, `supplement-log.json`, `jar-summary.json`, duplicate-class reports, журналы Gradle/CLI и `baseline.json`. Эти файлы не добавлены в Git и не будут доступны после отдельного clean workspace; данный Markdown содержит основные выводы, версии и findings самостоятельно. Stage 3/4B/4C локальные snapshots использованы только как historical evidence, не выданы за новую онлайн-проверку.

Изменён только состав документации: создан настоящий отчёт по запрошенному пути. Ранее отслеживаемые файлы, включая Java, Gradle/Wrapper, build scripts и Stage 3 отчёт, сохранены побайтово; коммитов и staging нет. Тесты, Kafka/ADS connections, reassignment и любые Kafka operations не выполнялись.

## Приложение A. Поимённая сверка 37 исходных runtime matches

| GHSA / источник | CVE alias | Компонент | Stage 3 | После 4B | После 4C / 4D |
| --- | --- | --- | --- | --- | --- |
| [GHSA-25qh-j22f-pwp8](https://github.com/advisories/GHSA-25qh-j22f-pwp8) | CVE-2025-11226 | ch.qos.logback:logback-core | Активно | Версия исправлена | Версия исправлена / 0 matches |
| [GHSA-2x2g-32r7-p4x8](https://github.com/advisories/GHSA-2x2g-32r7-p4x8) | CVE-2024-31141 | org.apache.kafka:kafka-clients | Активно | Активно | Версия исправлена / 0 matches |
| [GHSA-343h-94h5-c4wr](https://github.com/advisories/GHSA-343h-94h5-c4wr) | CVE-2026-106449 | org.lz4:lz4-java | Активно | Активно | Версия исправлена / 0 matches |
| [GHSA-3pjw-73gf-8qr5](https://github.com/advisories/GHSA-3pjw-73gf-8qr5) | CVE-2026-59888 | com.fasterxml.jackson.core:jackson-databind | Активно | Версия исправлена | Версия исправлена / 0 matches |
| [GHSA-4v53-57pg-c464](https://github.com/advisories/GHSA-4v53-57pg-c464) | CVE-2026-106452 | org.lz4:lz4-java | Активно | Активно | Версия исправлена / 0 matches |
| [GHSA-5gvw-p9qm-jgwh](https://github.com/advisories/GHSA-5gvw-p9qm-jgwh) | CVE-2026-59889 | com.fasterxml.jackson.core:jackson-databind | Активно | Версия исправлена | Версия исправлена / 0 matches |
| [GHSA-5jmj-h7xm-6q6v](https://github.com/advisories/GHSA-5jmj-h7xm-6q6v) | CVE-2026-54515 | com.fasterxml.jackson.core:jackson-databind | Активно | Версия исправлена | Версия исправлена / 0 matches |
| [GHSA-5qcv-4rpc-jp93](https://github.com/advisories/GHSA-5qcv-4rpc-jp93) | CVE-2026-35554 | org.apache.kafka:kafka-clients | Активно | Активно | Версия исправлена / 0 matches |
| [GHSA-6cx8-rjf8-pr8g](https://github.com/advisories/GHSA-6cx8-rjf8-pr8g) | CVE-2026-106453 | org.lz4:lz4-java | Активно | Активно | Версия исправлена / 0 matches |
| [GHSA-6v67-2wr5-gvf4](https://github.com/advisories/GHSA-6v67-2wr5-gvf4) | CVE-2024-12801 | ch.qos.logback:logback-core | Активно | Версия исправлена | Версия исправлена / 0 matches |
| [GHSA-72hv-8253-57qq](https://github.com/advisories/GHSA-72hv-8253-57qq) | CVE-2026-18401 | com.fasterxml.jackson.core:jackson-core | Активно | Версия исправлена | Версия исправлена / 0 matches |
| [GHSA-7hhh-6rmp-j9qf](https://github.com/advisories/GHSA-7hhh-6rmp-j9qf) | CVE-2026-89425 | com.fasterxml.jackson.core:jackson-core | Активно | Версия исправлена | Версия исправлена / 0 matches |
| [GHSA-cmp6-m4wj-q63q](https://github.com/advisories/GHSA-cmp6-m4wj-q63q) | CVE-2025-66566 | org.lz4:lz4-java | Активно | Активно | Версия исправлена / 0 matches |
| [GHSA-cxp5-3px4-pw24](https://github.com/advisories/GHSA-cxp5-3px4-pw24) | CVE-2026-91777 | com.fasterxml.jackson.core:jackson-databind | Активно | Версия исправлена | Версия исправлена / 0 matches |
| [GHSA-gm45-99xc-r7wv](https://github.com/advisories/GHSA-gm45-99xc-r7wv) | CVE-2026-106450 | org.lz4:lz4-java | Активно | Активно | Версия исправлена / 0 matches |
| [GHSA-gwq9-9ffj-mvrv](https://github.com/advisories/GHSA-gwq9-9ffj-mvrv) | CVE-2026-93451 | org.xerial.snappy:snappy-java | Активно | Активно | Версия исправлена / 0 matches |
| [GHSA-gx83-3vf8-gh7j](https://github.com/advisories/GHSA-gx83-3vf8-gh7j) | CVE-2026-83557 | com.fasterxml.jackson.core:jackson-databind | Активно | Версия исправлена | Версия исправлена / 0 matches |
| [GHSA-hgj6-7826-r7m5](https://github.com/advisories/GHSA-hgj6-7826-r7m5) | CVE-2026-54514 | com.fasterxml.jackson.core:jackson-databind | Активно | Версия исправлена | Версия исправлена / 0 matches |
| [GHSA-j3rv-43j4-c7qm](https://github.com/advisories/GHSA-j3rv-43j4-c7qm) | CVE-2026-54512 | com.fasterxml.jackson.core:jackson-databind | Активно | Версия исправлена | Версия исправлена / 0 matches |
| [GHSA-jhq6-gfmj-v8fx](https://github.com/advisories/GHSA-jhq6-gfmj-v8fx) | CVE-2026-10532 | ch.qos.logback:logback-core | Активно | Версия исправлена | Версия исправлена / 0 matches |
| [GHSA-mcr4-qmvw-px4g](https://github.com/advisories/GHSA-mcr4-qmvw-px4g) | CVE-2026-106451 | org.lz4:lz4-java | Активно | Активно | Версия исправлена / 0 matches |
| [GHSA-p47f-322f-whfh](https://github.com/advisories/GHSA-p47f-322f-whfh) | CVE-2026-9828 | ch.qos.logback:logback-core | Активно | Версия исправлена | Версия исправлена / 0 matches |
| [GHSA-p6pp-m3f8-5c89](https://github.com/advisories/GHSA-p6pp-m3f8-5c89) | CVE-2026-89407 | com.fasterxml.jackson.core:jackson-core | Активно | Версия исправлена | Версия исправлена / 0 matches |
| [GHSA-pr98-23f8-jwxv](https://github.com/advisories/GHSA-pr98-23f8-jwxv) | CVE-2024-12798 | ch.qos.logback:logback-core | Активно | Версия исправлена | Версия исправлена / 0 matches |
| [GHSA-q233-g92q-qxp6](https://github.com/advisories/GHSA-q233-g92q-qxp6) | CVE-2026-108106 | org.xerial.snappy:snappy-java | Активно | Активно | Версия исправлена / 0 matches |
| [GHSA-q4xh-88c3-wmh7](https://github.com/advisories/GHSA-q4xh-88c3-wmh7) | CVE-2026-68497 | com.fasterxml.jackson.core:jackson-databind | Активно | Версия исправлена | Версия исправлена / 0 matches |
| [GHSA-qqpg-mvqg-649v](https://github.com/advisories/GHSA-qqpg-mvqg-649v) | CVE-2026-1225 | ch.qos.logback:logback-core | Активно | Версия исправлена | Версия исправлена / 0 matches |
| [GHSA-r7wm-3cxj-wff9](https://github.com/advisories/GHSA-r7wm-3cxj-wff9) | CVE-2026-68494 | com.fasterxml.jackson.core:jackson-core | Активно | Версия исправлена | Версия исправлена / 0 matches |
| [GHSA-rmj7-2vxq-3g9f](https://github.com/advisories/GHSA-rmj7-2vxq-3g9f) | CVE-2026-54513 | com.fasterxml.jackson.core:jackson-databind | Активно | Версия исправлена | Версия исправлена / 0 matches |
| [GHSA-vgq5-3255-v292](https://github.com/advisories/GHSA-vgq5-3255-v292) | CVE-2025-27817 | org.apache.kafka:kafka-clients | Активно | Активно | Версия исправлена / 0 matches |
| [GHSA-vqf4-7m7x-wgfc](https://github.com/advisories/GHSA-vqf4-7m7x-wgfc) | CVE-2025-12183 | org.lz4:lz4-java | Активно | Активно | Версия исправлена / 0 matches |
| [GHSA-vvgp-rfg2-7rr6](https://github.com/advisories/GHSA-vvgp-rfg2-7rr6) | CVE-2026-77310 | com.fasterxml.jackson.core:jackson-databind | Активно | Версия исправлена | Версия исправлена / 0 matches |
| [GHSA-wf66-mphr-4c4r](https://github.com/advisories/GHSA-wf66-mphr-4c4r) | CVE-2026-33558 | org.apache.kafka:kafka-clients | Активно | Активно | Версия исправлена / 0 matches |
| [GHSA-wjgm-6hv5-3cvf](https://github.com/advisories/GHSA-wjgm-6hv5-3cvf) | CVE-2026-19032 | com.fasterxml.jackson.core:jackson-databind | Активно | Версия исправлена | Версия исправлена / 0 matches |
| [GHSA-wv8q-qhhj-9h54](https://github.com/advisories/GHSA-wv8q-qhhj-9h54) | CVE-2026-91776 | com.fasterxml.jackson.core:jackson-databind | Активно | Версия исправлена | Версия исправлена / 0 matches |
| [GHSA-xqrx-qh46-34m7](https://github.com/advisories/GHSA-xqrx-qh46-34m7) | CVE-2026-116590 | org.xerial.snappy:snappy-java | Активно | Активно | Версия исправлена / 0 matches |
| [GHSA-xx22-p4ch-683r](https://github.com/advisories/GHSA-xx22-p4ch-683r) | CVE-2026-59949 | org.lz4:lz4-java | Активно | Активно | Версия исправлена / 0 matches |

Таблица фиксирует устранение version match, а не утверждает, что каждая CVE эксплуатировалась исходным CLI. Дополнительные CNA/vendor находки, отсутствующие в исходных 37, рассмотрены в основном тексте.

## Приложение B. Полные compile/runtime/test classpaths

### compileClasspath

| Maven artifact | Версия |
| --- | --- |
| ch.qos.logback:logback-classic | 1.6.6 |
| ch.qos.logback:logback-core | 1.6.6 |
| com.fasterxml.jackson.core:jackson-annotations | 2.21 |
| com.fasterxml.jackson.core:jackson-core | 2.21.8 |
| com.fasterxml.jackson.core:jackson-databind | 2.21.8 |
| com.fasterxml.jackson.dataformat:jackson-dataformat-yaml | 2.21.8 |
| com.fasterxml.jackson.datatype:jackson-datatype-jdk8 | 2.21.8 |
| info.picocli:picocli | 4.7.7 |
| org.apache.kafka:kafka-clients | 3.9.2 |
| org.inferred:freebuilder | 2.8.0 |
| org.slf4j:slf4j-api | 2.0.20 |
| org.yaml:snakeyaml | 2.5 |

### runtimeClasspath

| Maven artifact | Версия |
| --- | --- |
| at.yawk.lz4:lz4-java | 1.14.0 |
| ch.qos.logback:logback-classic | 1.6.6 |
| ch.qos.logback:logback-core | 1.6.6 |
| com.fasterxml.jackson.core:jackson-annotations | 2.21 |
| com.fasterxml.jackson.core:jackson-core | 2.21.8 |
| com.fasterxml.jackson.core:jackson-databind | 2.21.8 |
| com.fasterxml.jackson.dataformat:jackson-dataformat-yaml | 2.21.8 |
| com.fasterxml.jackson.datatype:jackson-datatype-jdk8 | 2.21.8 |
| com.github.luben:zstd-jni | 1.5.7-26 |
| info.picocli:picocli | 4.7.7 |
| org.apache.kafka:kafka-clients | 3.9.2 |
| org.slf4j:slf4j-api | 2.0.20 |
| org.xerial.snappy:snappy-java | 1.1.10.12 |
| org.yaml:snakeyaml | 2.5 |

### testRuntimeClasspath

| Maven artifact | Версия |
| --- | --- |
| at.yawk.lz4:lz4-java | 1.14.0 |
| cglib:cglib-nodep | 3.3.0 |
| ch.qos.logback:logback-classic | 1.6.6 |
| ch.qos.logback:logback-core | 1.6.6 |
| com.fasterxml.jackson.core:jackson-annotations | 2.21 |
| com.fasterxml.jackson.core:jackson-core | 2.21.8 |
| com.fasterxml.jackson.core:jackson-databind | 2.21.8 |
| com.fasterxml.jackson.dataformat:jackson-dataformat-yaml | 2.21.8 |
| com.fasterxml.jackson.datatype:jackson-datatype-jdk8 | 2.21.8 |
| com.github.luben:zstd-jni | 1.5.7-26 |
| com.github.stefanbirkner:system-rules | 1.19.0 |
| com.vaadin.external.google:android-json | 0.0.20131108.vaadin1 |
| info.picocli:picocli | 4.7.7 |
| junit:junit | 4.13.2 |
| org.apache.groovy:groovy | 4.0.21 |
| org.apache.kafka:kafka-clients | 3.9.2 |
| org.hamcrest:hamcrest-core | 1.3 |
| org.hamcrest:hamcrest | 2.2 |
| org.junit.jupiter:junit-jupiter-api | 5.10.2 |
| org.junit.jupiter:junit-jupiter-engine | 5.10.2 |
| org.junit.jupiter:junit-jupiter-params | 5.10.2 |
| org.junit.jupiter:junit-jupiter | 5.10.2 |
| org.junit.platform:junit-platform-commons | 1.10.2 |
| org.junit.platform:junit-platform-engine | 1.10.2 |
| org.junit.vintage:junit-vintage-engine | 5.10.2 |
| org.opentest4j:opentest4j | 1.3.0 |
| org.skyscreamer:jsonassert | 1.5.1 |
| org.slf4j:slf4j-api | 2.0.20 |
| org.spockframework:spock-core | 2.3-groovy-4.0 |
| org.spockframework:spock-junit4 | 2.3-groovy-4.0 |
| org.xerial.snappy:snappy-java | 1.1.10.12 |
| org.yaml:snakeyaml | 2.5 |

### annotationProcessor

| Maven artifact | Версия |
| --- | --- |
| org.inferred:freebuilder | 2.8.0 |

### jacocoAgent

| Maven artifact | Версия |
| --- | --- |
| org.jacoco:org.jacoco.agent | 0.8.11 |

### jacocoAnt

| Maven artifact | Версия |
| --- | --- |
| org.jacoco:org.jacoco.agent | 0.8.11 |
| org.jacoco:org.jacoco.ant | 0.8.11 |
| org.jacoco:org.jacoco.core | 0.8.11 |
| org.jacoco:org.jacoco.report | 0.8.11 |
| org.ow2.asm:asm-commons | 9.6 |
| org.ow2.asm:asm-tree | 9.6 |
| org.ow2.asm:asm | 9.6 |

## Приложение C. Идентифицированные bundled компоненты Gradle 8.14.6

Это 153 координаты/версии, а не число отдельных библиотечных JAR: некоторые архивы содержат несколько embedded metadata, native variants или repackaged classes. Координаты из pom.properties отделены от вывода по имени официального файла. Внутренние Gradle JAR перечислены в локальном gradle-jars.json; им не присвоены вымышленные Maven packages.

| Maven artifact | Версия | Основание |
| --- | --- | --- |
| com.amazonaws:aws-java-sdk-core | 1.12.780 | pom.properties |
| com.amazonaws:aws-java-sdk-kms | 1.12.780 | pom.properties |
| com.amazonaws:aws-java-sdk-s3 | 1.12.780 | pom.properties |
| com.amazonaws:aws-java-sdk-sts | 1.12.780 | pom.properties |
| com.amazonaws:jmespath-java | 1.12.780 | pom.properties |
| com.beust:jcommander | 1.78 | Имя файла в официальном Gradle distribution |
| com.esotericsoftware.kryo:kryo | 2.24.0 | pom.properties |
| com.esotericsoftware.minlog:minlog | 1.2 | pom.properties |
| com.esotericsoftware.reflectasm:reflectasm | 1.09 | pom.properties |
| com.fasterxml.jackson.core:jackson-annotations | 2.18.11 | pom.properties |
| com.fasterxml.jackson.core:jackson-core | 2.18.11 | pom.properties |
| com.fasterxml.jackson.core:jackson-databind | 2.18.11 | pom.properties |
| com.fasterxml.jackson.datatype:jackson-datatype-jdk8 | 2.18.11 | pom.properties |
| com.fasterxml.jackson.datatype:jackson-datatype-jsr310 | 2.18.11 | pom.properties |
| com.github.javaparser:javaparser-core | 3.17.0 | pom.properties |
| com.github.mwiede:jsch | 0.2.16 | pom.properties |
| com.google.api-client:google-api-client | 1.34.0 | pom.properties |
| com.google.apis:google-api-services-storage | v1-rev20220705-1.32.1 | pom.properties |
| com.google.code.findbugs:jsr305 | 3.0.2 | pom.properties |
| com.google.code.gson:gson | 2.10 | pom.properties |
| com.google.errorprone:error_prone_annotations | 2.36.0 | pom.properties |
| com.google.guava:failureaccess | 1.0.3 | pom.properties |
| com.google.guava:guava | 33.4.6-jre | pom.properties |
| com.google.http-client:google-http-client | 1.42.2 | pom.properties |
| com.google.http-client:google-http-client-apache-v2 | 1.42.2 | pom.properties |
| com.google.http-client:google-http-client-gson | 1.42.2 | pom.properties |
| com.google.oauth-client:google-oauth-client | 1.34.1 | pom.properties |
| com.googlecode.jatl:jatl | 0.2.3 | pom.properties |
| com.googlecode.plist:dd-plist | 1.27 | pom.properties |
| com.thoughtworks.qdox:qdox | 1.12.1 | pom.properties |
| commons-codec:commons-codec | 1.16.1 | pom.properties |
| commons-io:commons-io | 2.15.1 | pom.properties |
| commons-lang:commons-lang | 2.6 | pom.properties |
| io.grpc:grpc-context | 1.27.2 | Имя файла в официальном Gradle distribution |
| io.opencensus:opencensus-api | 0.31.1 | Имя файла в официальном Gradle distribution |
| io.opencensus:opencensus-contrib-http-util | 0.31.1 | Имя файла в официальном Gradle distribution |
| it.unimi.dsi:fastutil | 8.5.2 | Имя файла в официальном Gradle distribution |
| jcifs:jcifs | 1.3.17 | pom.properties |
| joda-time:joda-time | 2.12.2 | pom.properties |
| junit:junit | 4.13.2 | pom.properties |
| net.rubygrapefruit:native-platform | 0.22-milestone-28 | Имя файла в официальном Gradle distribution |
| net.rubygrapefruit:native-platform-freebsd-amd64-libcpp | 0.22-milestone-28 | Имя файла в официальном Gradle distribution |
| net.rubygrapefruit:native-platform-linux-aarch64 | 0.22-milestone-28 | Имя файла в официальном Gradle distribution |
| net.rubygrapefruit:native-platform-linux-aarch64-ncurses5 | 0.22-milestone-28 | Имя файла в официальном Gradle distribution |
| net.rubygrapefruit:native-platform-linux-aarch64-ncurses6 | 0.22-milestone-28 | Имя файла в официальном Gradle distribution |
| net.rubygrapefruit:native-platform-linux-amd64 | 0.22-milestone-28 | Имя файла в официальном Gradle distribution |
| net.rubygrapefruit:native-platform-linux-amd64-ncurses5 | 0.22-milestone-28 | Имя файла в официальном Gradle distribution |
| net.rubygrapefruit:native-platform-linux-amd64-ncurses6 | 0.22-milestone-28 | Имя файла в официальном Gradle distribution |
| net.rubygrapefruit:native-platform-osx-aarch64 | 0.22-milestone-28 | Имя файла в официальном Gradle distribution |
| net.rubygrapefruit:native-platform-osx-amd64 | 0.22-milestone-28 | Имя файла в официальном Gradle distribution |
| net.rubygrapefruit:native-platform-windows-amd64 | 0.22-milestone-28 | Имя файла в официальном Gradle distribution |
| net.rubygrapefruit:native-platform-windows-amd64-min | 0.22-milestone-28 | Имя файла в официальном Gradle distribution |
| net.rubygrapefruit:native-platform-windows-i386 | 0.22-milestone-28 | Имя файла в официальном Gradle distribution |
| net.rubygrapefruit:native-platform-windows-i386-min | 0.22-milestone-28 | Имя файла в официальном Gradle distribution |
| org.antlr:antlr4-runtime | 4.7.2 | pom.properties |
| org.apache-extras.beanshell:bsh | 2.0b6 | Имя файла в официальном Gradle distribution |
| org.apache.ant:ant | 1.10.15 | pom.properties |
| org.apache.ant:ant-antlr | 1.10.15 | Имя файла в официальном Gradle distribution |
| org.apache.ant:ant-junit | 1.10.15 | Имя файла в официальном Gradle distribution |
| org.apache.ant:ant-launcher | 1.10.15 | pom.properties |
| org.apache.commons:commons-compress | 1.26.1 | pom.properties |
| org.apache.commons:commons-lang3 | 3.18.0 | pom.properties |
| org.apache.httpcomponents:httpclient | 4.5.14 | pom.properties |
| org.apache.httpcomponents:httpcore | 4.4.14 | pom.properties |
| org.apache.ivy:ivy | 2.5.3 | Имя файла в официальном Gradle distribution |
| org.apache.maven:maven-builder-support | 3.9.5 | pom.properties |
| org.apache.maven:maven-model | 3.9.5 | pom.properties |
| org.apache.maven:maven-repository-metadata | 3.9.5 | pom.properties |
| org.apache.maven:maven-settings | 3.9.5 | pom.properties |
| org.apache.maven:maven-settings-builder | 3.9.5 | pom.properties |
| org.apache.sshd:sshd-common | 2.15.0 | pom.properties |
| org.apache.sshd:sshd-core | 2.15.0 | pom.properties |
| org.apache.sshd:sshd-sftp | 2.15.0 | pom.properties |
| org.bouncycastle:bcpg-jdk18on | 1.85 | Имя файла в официальном Gradle distribution |
| org.bouncycastle:bcprov-jdk18on | 1.85 | Имя файла в официальном Gradle distribution |
| org.bouncycastle:bcutil-jdk18on | 1.85 | Имя файла в официальном Gradle distribution |
| org.codehaus.groovy:groovy | 3.0.25 | Имя файла в официальном Gradle distribution |
| org.codehaus.groovy:groovy-ant | 3.0.25 | Имя файла в официальном Gradle distribution |
| org.codehaus.groovy:groovy-astbuilder | 3.0.25 | Имя файла в официальном Gradle distribution |
| org.codehaus.groovy:groovy-console | 3.0.25 | Имя файла в официальном Gradle distribution |
| org.codehaus.groovy:groovy-datetime | 3.0.25 | Имя файла в официальном Gradle distribution |
| org.codehaus.groovy:groovy-dateutil | 3.0.25 | Имя файла в официальном Gradle distribution |
| org.codehaus.groovy:groovy-docgenerator | 3.0.25 | Имя файла в официальном Gradle distribution |
| org.codehaus.groovy:groovy-groovydoc | 3.0.25 | Имя файла в официальном Gradle distribution |
| org.codehaus.groovy:groovy-json | 3.0.25 | Имя файла в официальном Gradle distribution |
| org.codehaus.groovy:groovy-nio | 3.0.25 | Имя файла в официальном Gradle distribution |
| org.codehaus.groovy:groovy-sql | 3.0.25 | Имя файла в официальном Gradle distribution |
| org.codehaus.groovy:groovy-swing | 3.0.25 | Имя файла в официальном Gradle distribution |
| org.codehaus.groovy:groovy-templates | 3.0.25 | Имя файла в официальном Gradle distribution |
| org.codehaus.groovy:groovy-test | 3.0.25 | Имя файла в официальном Gradle distribution |
| org.codehaus.groovy:groovy-xml | 3.0.25 | Имя файла в официальном Gradle distribution |
| org.codehaus.plexus:plexus-cipher | 2.0 | pom.properties |
| org.codehaus.plexus:plexus-interpolation | 1.26 | pom.properties |
| org.codehaus.plexus:plexus-sec-dispatcher | 2.0 | pom.properties |
| org.codehaus.plexus:plexus-utils | 3.6.1 | pom.properties |
| org.eclipse.jgit:org.eclipse.jgit | 5.13.5.202508271544-r | pom.properties |
| org.eclipse.jgit:org.eclipse.jgit.ssh.apache | 5.13.5.202508271544-r | pom.properties |
| org.fusesource.hawtjni:hawtjni-runtime | 1.17 | pom.properties |
| org.fusesource.jansi:jansi | 1.18 | pom.properties |
| org.fusesource.jansi:jansi-freebsd32 | 1.8 | pom.properties |
| org.fusesource.jansi:jansi-freebsd64 | 1.8 | pom.properties |
| org.fusesource.jansi:jansi-linux32 | 1.8 | pom.properties |
| org.fusesource.jansi:jansi-linux64 | 1.8 | pom.properties |
| org.fusesource.jansi:jansi-native | 1.8 | pom.properties |
| org.fusesource.jansi:jansi-osx | 1.8 | pom.properties |
| org.fusesource.jansi:jansi-windows32 | 1.8 | pom.properties |
| org.fusesource.jansi:jansi-windows64 | 1.8 | pom.properties |
| org.hamcrest:hamcrest-core | 1.3 | pom.properties |
| org.jetbrains:annotations | 24.0.1 | Имя файла в официальном Gradle distribution |
| org.jetbrains.intellij.deps:trove4j | 1.0.20200330 | Имя файла в официальном Gradle distribution |
| org.jetbrains.kotlin:kotlin-assignment-compiler-plugin-embeddable | 2.0.21 | Имя файла в официальном Gradle distribution |
| org.jetbrains.kotlin:kotlin-compiler-embeddable | 2.0.21 | Имя файла в официальном Gradle distribution |
| org.jetbrains.kotlin:kotlin-daemon-embeddable | 2.0.21 | Имя файла в официальном Gradle distribution |
| org.jetbrains.kotlin:kotlin-reflect | 2.0.21 | Имя файла в официальном Gradle distribution |
| org.jetbrains.kotlin:kotlin-sam-with-receiver-compiler-plugin | 2.0.21 | Имя файла в официальном Gradle distribution |
| org.jetbrains.kotlin:kotlin-script-runtime | 2.0.21 | Имя файла в официальном Gradle distribution |
| org.jetbrains.kotlin:kotlin-scripting-common | 2.0.21 | Имя файла в официальном Gradle distribution |
| org.jetbrains.kotlin:kotlin-scripting-compiler-embeddable | 2.0.21 | Имя файла в официальном Gradle distribution |
| org.jetbrains.kotlin:kotlin-scripting-compiler-impl-embeddable | 2.0.21 | Имя файла в официальном Gradle distribution |
| org.jetbrains.kotlin:kotlin-scripting-jvm | 2.0.21 | Имя файла в официальном Gradle distribution |
| org.jetbrains.kotlin:kotlin-scripting-jvm-host | 2.0.21 | Имя файла в официальном Gradle distribution |
| org.jetbrains.kotlin:kotlin-stdlib | 2.0.21 | Имя файла в официальном Gradle distribution |
| org.jetbrains.kotlinx:kotlinx-coroutines-core-jvm | 1.6.4 | Имя файла в официальном Gradle distribution |
| org.jetbrains.kotlinx:kotlinx-metadata-jvm | 0.5.0 | Имя файла в официальном Gradle distribution |
| org.jetbrains.kotlinx:kotlinx-serialization-core-jvm | 1.6.2 | Имя файла в официальном Gradle distribution |
| org.jetbrains.kotlinx:kotlinx-serialization-json-jvm | 1.6.2 | Имя файла в официальном Gradle distribution |
| org.jsoup:jsoup | 1.23.2 | pom.properties |
| org.junit.platform:junit-platform-commons | 1.8.2 | Имя файла в официальном Gradle distribution |
| org.junit.platform:junit-platform-engine | 1.8.2 | Имя файла в официальном Gradle distribution |
| org.junit.platform:junit-platform-launcher | 1.8.2 | Имя файла в официальном Gradle distribution |
| org.objenesis:objenesis | 2.6 | Имя файла в официальном Gradle distribution |
| org.opentest4j:opentest4j | 1.3.0 | pom.properties |
| org.ow2.asm:asm | 9.8 | Имя файла в официальном Gradle distribution |
| org.ow2.asm:asm-commons | 9.8 | Имя файла в официальном Gradle distribution |
| org.ow2.asm:asm-tree | 9.8 | Имя файла в официальном Gradle distribution |
| org.slf4j:jcl-over-slf4j | 1.7.36 | pom.properties |
| org.slf4j:jul-to-slf4j | 1.7.36 | pom.properties |
| org.slf4j:log4j-over-slf4j | 1.7.36 | pom.properties |
| org.slf4j:slf4j-api | 1.7.36 | pom.properties |
| org.testng:testng | 6.3.1 | pom.properties |
| org.tomlj:tomlj | 1.0.0 | Имя файла в официальном Gradle distribution |
| org.yaml:snakeyaml | 2.0 | pom.properties |
| xml-apis:xml-apis | 1.4.01 | Имя файла в официальном Gradle distribution |
