# Модернизация Kafka GitOps — задание 1

Дата: **11.11.2026**, ветка `feature/ads-gitops`, исходный и сохранённый HEAD
`934472a7fb0603929e64ca895451f2d7796f527a`.

**Результат:** выполнена модернизация Gradle 9.9.1, Shadow 9.6.1, Kafka Client 4.4.0,
Jackson 3.3.0, Groovy 6.0.0, Spock 2.5 и JUnit 6.1.3. Обновлены версии
**39 существующих Maven-модулей** в разрешённых project/build/test graphs;
дополнительно заменены координаты Jackson/YAML/scanner, обновлены Wrapper и BOM.
Полная таблица ниже отделяет обновления от новых транзитивных модулей и удалений.
В число 39 не включены сотни внутренних модулей самого Gradle.

Прошли **879 тестов: 814 исходных + 65 новых сценариев**, 49 локальных CLI checks
и 156 дополнительных codec checks непосредственно с fat JAR.
Основной артефакт — `build/libs/kafka-gitops-all.jar`, **25 790 267 bytes**
(24,60 MiB). Минимальная версия Java осталась **21**.

**Безусловная безопасность и production-ready не заявляются:** в официальном
дистрибутиве последнего стабильного Gradle остаются Guava/CVE-2026-102554 и
Jackson Databind/CVE-2026-110879. В runtime приложения этих затронутых версий нет.
Реальные брокеры, SASL/SSL-соединения и Linux JNI не испытывались. Production RF
сохраняет прежний fail-closed барьер. Задания 2 и 3 не выполнялись.

## 1. Исходное состояние и границы

Рабочее дерево перед изменениями было чистым, ветка совпадала с заданием.
Прочитаны build.gradle, settings.gradle, Wrapper, Stage 6B4/6B3C и оба отчёта
Stage 3/4. Файла gradle.properties до работы не было; он создан для запрета
автоматической установки JDK. Чужие незакоммиченные изменения отсутствовали.

Подтверждены Gradle 8.14.6, Shadow 8.3.11, Kafka Client 3.9.2. Уже имелись
Jackson BOM 2.21.8, Logback 1.6.6, SLF4J 2.0.20, Picocli 4.7.7 и исправленные
compression libraries. Они не были ошибочно приняты за первоначальные версии
joschi/kafka-gitops 0.3.1. Версия самого CLI остаётся 0.3.1.

Gradle запускался на установленном Temurin 21.0.13. Исходная компиляция выбирала
AZUL Java 21; итоговая — установленный Eclipse Temurin Java 21. Не устанавливались
JDK, системное ПО или контейнеры. Dockerfile/Compose не изменены и не запускались.
Не использовались реальные credentials, bootstrap servers или внешние ccloud-команды.

Перед базовым test использован прежний проверенный init script с allowlist
33 unit-спецификаций и исключением `**/*IntegrationSpec*`. После модернизации это
исключение постоянно задано в обычном test. Исходные интеграционные спецификации
сохранены; они не выдаются за пройденные. Это предохранитель от обращения к
внешнему Kafka, а не исключение упавших unit-тестов.

## 2. Проверка актуальности и полная таблица зависимостей

Версии проверены непосредственно перед обновлением по
[Gradle Releases API](https://services.gradle.org/versions/current),
[Shadow Plugin Portal](https://plugins.gradle.org/plugin/com.gradleup.shadow),
[Sonar Plugin Portal](https://plugins.gradle.org/plugin/org.sonarqube),
[Develocity Plugin Portal](https://plugins.gradle.org/plugin/com.gradle.develocity),
[Foojay Plugin Portal](https://plugins.gradle.org/plugin/org.gradle.toolchains.foojay-resolver-convention)
и публичным Maven Central metadata каждого реально используемого модуля, например
[Kafka Client](https://repo.maven.apache.org/maven2/org/apache/kafka/kafka-clients/maven-metadata.xml),
[Jackson](https://repo.maven.apache.org/maven2/tools/jackson/jackson-bom/maven-metadata.xml),
[Spock](https://repo.maven.apache.org/maven2/org/spockframework/spock-core/maven-metadata.xml),
[JUnit](https://repo.maven.apache.org/maven2/org/junit/junit-bom/maven-metadata.xml).

| Платформа / BOM | До | После |
| --- | --- | --- |
| Java toolchain / фактический bytecode | 21 / 65 | 21 / 65 |
| Gradle Wrapper и core plugins | 8.14.6 | 9.9.1 |
| Jackson runtime BOM | com.fasterxml.jackson:jackson-bom:2.21.8 | tools.jackson:jackson-bom:3.3.0 |
| Jackson compiler marker BOM | не выделялся | com.fasterxml.jackson:jackson-bom:2.23.0, только compileOnly |
| JUnit BOM | транзитивный 5.10.2 | явный 6.1.3 |

Области: C — compileClasspath; R — runtimeClasspath; TC/TR — test compile/runtime;
AP — annotation processor; JA/JAnt — JaCoCo agent/Ant; B — classpath плагинов
проекта; S — settings plugins. Одна строка — один Maven module, а не одна декларация
или несколько повторений в разных configurations. BOM не является JAR.

| Maven-модуль | До | После | Область после / до удаления |
| --- | --- | --- | --- |
| `at.yawk.lz4:lz4-java` | 1.14.0 | 1.14.0 | R, TC, TR |
| `cglib:cglib-nodep` | 3.3.0 | удалён / заменён | TC, TR |
| `ch.qos.logback:logback-classic` | 1.6.6 | 1.6.6 | C, R, TC, TR |
| `ch.qos.logback:logback-core` | 1.6.6 | 1.6.6 | C, R, TC, TR |
| `com.fasterxml.jackson.core:jackson-annotations` | 2.21 | 2.23 | C, R, TC, TR |
| `com.fasterxml.jackson.core:jackson-core` | 2.21.8 | 2.23.0 | C |
| `com.fasterxml.jackson.core:jackson-databind` | 2.21.8 | 2.23.0 | C |
| `com.fasterxml.jackson.dataformat:jackson-dataformat-yaml` | 2.21.8 | удалён / заменён | C, R, TC, TR |
| `com.fasterxml.jackson.datatype:jackson-datatype-jdk8` | 2.21.8 | удалён / заменён | C, R, TC, TR |
| `com.fasterxml.woodstox:woodstox-core` | 7.1.1 | 7.3.0 | B |
| `com.github.luben:zstd-jni` | 1.5.7-26 | 1.5.7-26 | R, TC, TR |
| `com.github.stefanbirkner:system-rules` | 1.19.0 | 1.19.0 | TC, TR |
| `com.google.code.gson:gson` | 2.10.1 | удалён / заменён | S |
| `com.gradle:develocity-gradle-plugin` | 3.17.2 | 4.6.1 | S |
| `com.gradle:gradle-enterprise-gradle-plugin` | 3.17.2 | удалён / заменён | S |
| `com.gradleup.shadow:shadow-gradle-plugin` | 8.3.11 | 9.6.1 | B |
| `com.vaadin.external.google:android-json` | 0.0.20131108.vaadin1 | 0.0.20131108.vaadin1 | TC, TR |
| `commons-codec:commons-codec` | — | 1.23.0 | B |
| `commons-io:commons-io` | 2.19.0 | 2.22.0 | B |
| `gradle.plugin.org.inferred:gradle-processors` | 3.7.0 | удалён / заменён | B |
| `info.picocli:picocli` | 4.7.7 | 4.7.7 | C, R, TC, TR |
| `io.leangen.geantyref:geantyref` | — | 2.0.1 | TR |
| `junit:junit` | 4.13.2 | 4.13.2 | TC, TR |
| `org.apache.ant:ant` | 1.10.15 | 1.10.18 | B |
| `org.apache.ant:ant-launcher` | 1.10.15 | 1.10.18 | B |
| `org.apache.groovy:groovy` | 4.0.21 | 6.0.0 | TC, TR |
| `org.apache.kafka:kafka-clients` | 3.9.2 | 4.4.0 | C, R, TC, TR |
| `org.apache.logging.log4j:log4j-api` | 2.25.3 | 2.26.1 | B |
| `org.apache.logging.log4j:log4j-core` | 2.25.3 | 2.26.1 | B |
| `org.apache.maven:maven-api-annotations` | 4.0.0-rc-5 | 4.0.0-rc-6 | B |
| `org.apache.maven:maven-api-xml` | 4.0.0-rc-5 | 4.0.0-rc-6 | B |
| `org.apache.maven:maven-xml` | 4.0.0-rc-5 | 4.0.0-rc-6 | B |
| `org.apiguardian:apiguardian-api` | 1.1.2 | 1.1.2 | TC |
| `org.codehaus.groovy:groovy-backports-compat23` | 2.3.5 | удалён / заменён | B |
| `org.codehaus.plexus:plexus-utils` | 4.0.2 | 4.1.0 | B |
| `org.codehaus.plexus:plexus-xml` | 4.1.1 | 4.2.0 | B |
| `org.codehaus.woodstox:stax2-api` | 4.2.2 | 4.3.2 | B |
| `org.gradle.toolchains:foojay-resolver` | 0.8.0 | 1.0.0 | S |
| `org.hamcrest:hamcrest` | 2.2 | 3.0 | TC, TR |
| `org.hamcrest:hamcrest-core` | 1.3 | удалён / заменён | TC, TR |
| `org.inferred:freebuilder` | 2.8.0 | 2.8.0 | AP, C |
| `org.jacoco:org.jacoco.agent` | 0.8.11 | 0.8.16 | JA, JAnt |
| `org.jacoco:org.jacoco.ant` | 0.8.11 | 0.8.16 | JAnt |
| `org.jacoco:org.jacoco.core` | 0.8.11 | 0.8.16 | JAnt |
| `org.jacoco:org.jacoco.report` | 0.8.11 | 0.8.16 | JAnt |
| `org.jdom:jdom2` | 2.0.6.1 | 2.0.6.1 | B |
| `org.jetbrains.kotlin:kotlin-metadata-jvm` | — | 2.4.21 | B |
| `org.jetbrains.kotlin:kotlin-stdlib` | — | 2.4.21 | B |
| `org.jetbrains:annotations` | — | 26.1.0 | B |
| `org.jspecify:jspecify` | — | 1.0.1 | TC |
| `org.junit.jupiter:junit-jupiter` | 5.10.2 | 6.1.3 | TC, TR |
| `org.junit.jupiter:junit-jupiter-api` | 5.10.2 | 6.1.3 | TC, TR |
| `org.junit.jupiter:junit-jupiter-engine` | 5.10.2 | 6.1.3 | TR |
| `org.junit.jupiter:junit-jupiter-params` | 5.10.2 | 6.1.3 | TC, TR |
| `org.junit.platform:junit-platform-commons` | 1.10.2 | 6.1.3 | TC, TR |
| `org.junit.platform:junit-platform-engine` | 1.10.2 | 6.1.3 | TC, TR |
| `org.junit.platform:junit-platform-launcher` | — | 6.1.3 | TR |
| `org.junit.vintage:junit-vintage-engine` | 5.10.2 | удалён / заменён | TR |
| `org.opentest4j:opentest4j` | 1.3.0 | 1.3.0 | TC, TR |
| `org.ow2.asm:asm` | 9.6 | 9.11.1 | JAnt |
| `org.ow2.asm:asm-commons` | 9.6 | 9.11.1 | JAnt |
| `org.ow2.asm:asm-tree` | 9.6 | 9.11.1 | JAnt |
| `org.skyscreamer:jsonassert` | 1.5.1 | 1.5.3 | TC, TR |
| `org.slf4j:slf4j-api` | 2.0.20 | 2.0.20 | C, R, TC, TR |
| `org.snakeyaml:snakeyaml-engine` | — | 3.2 | C, R, TC, TR |
| `org.sonarsource.scanner.api:sonar-scanner-api` | 2.16.2.588 | удалён / заменён | B |
| `org.sonarsource.scanner.gradle:sonarqube-gradle-plugin` | 5.0.0.4638 | 7.5.0.8588 | B |
| `org.sonarsource.scanner.lib:sonar-scanner-java-library-shaded` | — | 4.3.0.11766 | B |
| `org.spockframework:spock-core` | 2.3-groovy-4.0 | 2.5-groovy-6.0 | TC, TR |
| `org.spockframework:spock-junit4` | 2.3-groovy-4.0 | 2.5-groovy-6.0 | TC, TR |
| `org.vafer:jdependency` | 2.16 | 2.17 | B |
| `org.xerial.snappy:snappy-java` | 1.1.10.12 | 1.1.10.12 | R, TC, TR |
| `org.yaml:snakeyaml` | 2.5 | удалён / заменён | C, R, TC, TR |
| `tools.jackson.core:jackson-core` | — | 3.3.0 | C, R, TC, TR |
| `tools.jackson.core:jackson-databind` | — | 3.3.0 | C, R, TC, TR |
| `tools.jackson.dataformat:jackson-dataformat-yaml` | — | 3.3.0 | C, R, TC, TR |

CSV и точные SHA-256 разрешённых артефактов:
[dependencies.csv](modernization-stage1-2026-11-11/dependencies.csv),
[dependency-inventory.json](modernization-stage1-2026-11-11/dependency-inventory.json).

Основные major-переходы: Gradle/Shadow 8→9, Kafka 3→4, Jackson runtime 2→3,
Groovy 4→6, Jupiter 5→6 и Platform 1→6, Sonar 5→7, Develocity 3→4,
Foojay 0→1, Hamcrest 2→3. YAML backend заменён с classic SnakeYAML на
SnakeYAML Engine 3.2, выбранный Jackson 3; classic SnakeYAML не добавлялся обратно.

Новые версии SLF4J 2.1 alpha, Log4j 3 beta, JSONassert 2 RC и Kotlin 2.5 RC
отклонены в пользу подходящих стабильных веток. Logback/Picocli/SLF4J/compression
уже были последними стабильными и сохранены. Mockito, Guava и Byte Buddy в runtime
приложения отсутствуют и ради количества обновлений не добавлялись.

Исключение prerelease в транзитивном build graph: стабильный Plexus XML 4.2.0
использует Maven XML/API 4.0.0-rc-6. Совместимых стабильных версий этих конкретных
4.0 API в Central нет; Maven 3 имеет другие API/артефакты. Они оставлены как
штатные зависимости стабильного плагина, только в сборке. Обновление до rc-7
не навязывалось. Runtime/test targets не используют prerelease.

Groovy compiler classpath дополнительно инвентаризирован по фактическому
GroovyCompile.groovyClasspath: 11 Groovy 6.0.0 модулей, Ant/launcher/junit/antlr
1.10.18, commonmark 0.30.0 и javaparser-core 3.28.2. Это компилятор тестов,
эти библиотеки не вошли в приложение. ASM 9.11.1 явно согласован в jacocoAnt;
встроенные shaded-копии инструментов не подменялись.

## 3. Изменения Gradle и упаковки

- Удалён устаревший org.inferred.processors 3.7.0: его Convention/IDE API мешали
  Gradle 9. Сохранены штатные annotationProcessor и compileOnly FreeBuilder;
  чистая сборка вновь генерирует все builders.
- mainClassName заменён на application.mainClass, зависимости переведены на
  single-string notation, регистрация задач — на tasks.register. Устранены
  собственные предупреждения deprecated Gradle DSL; проверены --warning-mode all.
- Убрана привязка toolchain к AZUL. Зафиксированы Java release/source/target 21,
  отключена автоматическая загрузка JDK. Не требуется Java 25.
- com.gradle.enterprise заменён на com.gradle.develocity 4.6.1 с новым DSL.
  Публикация scan требует явного `-PpublishBuildScan=true`; проверки шли с
  `--no-scan`. Sonar analysis не запускался и исходники не отправлялись.
- buildExecutableJar реализован нативной Gradle-задачей: тот же stub.sh плюс
  fat JAR, без зависимости от sh/chmod на Windows. Launcher нормализуется к LF,
  release ZIP сохраняет mode 0755. Существующие имена задач сохранены.
- Убран внешний Manifest Class-Path; runtime целиком внутри JAR. Настроены
  mergeServiceFiles, отдельная обработка service duplicates, удаление подписей
  исходных JAR и несовместимых module descriptors. Minimize/relocation не используются.
- Добавлен Multi-Release: true, сохранены native resources, стабильный порядок
  entries и отсутствие timestamps. Дубликаты классов дополнительно проверены
  по исходным runtime JAR, поэтому EXCLUDE не скрывает конфликтующие классы.
- JaCoCo обновлён до 0.8.16; report исключает generated *_Builder* без
  afterEvaluate. jacocoTestReport успешно выполнен.

Wrapper сгенерирован официальными Gradle Wrapper tasks сначала через старую,
затем через новую версию. При первом обновлении Windows BAT переписал сам себя:
сама wrapper task завершилась успешно, но старый cmd продолжил чтение со смещением
и вернул 9009. Финальная генерация выполнена Java-вызовом того же Wrapper;
последующие обычные gradlew.bat успешно работают.

Официальный distribution SHA-256:
`d8a18a7a841dd5df825d35a685fad6f2d7f12ac0c280e57b67c27b4a520ba228`.
Фактический Wrapper JAR SHA-256 совпал с официальным:
`621008afc4f5d59357fcf06ffa644dd5aade5e091bdc363742710445f73cdaf8`.
Источники — [release metadata](https://services.gradle.org/versions/current),
[distribution checksum](https://services.gradle.org/distributions/gradle-9.9.1-bin.zip.sha256),
[wrapper checksum](https://services.gradle.org/distributions/gradle-9.9.1-wrapper.jar.sha256).

Configuration cache проверен для classes/test/shadowJar/buildExecutableJar/buildRelease:
первый вызов сохранил запись, второй её использовал. Отдельная read-only audit task,
обращающаяся к resolved configurations, намеренно запускалась без configuration cache.

## 4. Адаптация Kafka Client 4.4

Прочитаны [Kafka upgrade notes](https://kafka.apache.org/44/getting-started/upgrade/)
и исходники точного тега 4.4.0.

1. TopicConfig.MESSAGE_FORMAT_VERSION_CONFIG и MESSAGE_TIMESTAMP_DIFFERENCE_MAX_MS_CONFIG
   удалены из Java API. Их прежние spelling сохранены в диагностическом allowlist
   строками. Старые YAML/JSON ключи не переписываются, чувствительные поля не открыты.
2. Удалён TopicListing(String, boolean). В fake fixtures используется
   TopicListing(String, Uuid.ZERO_UUID, boolean); реальное приложение не конструирует
   эти results. ZERO_UUID здесь обозначает отсутствие фиктивного topic ID, а RF
   identity-проверки продолжают использовать отдельные настоящие UUID fixtures.
3. В AclOperation появился TWO_PHASE_COMMIT. Сохранён прежний разрешённый набор
   custom ACL operations, новая операция отвергается дополнительным тестом.
   Профили/автоматические права/DESCRIBE не добавлены.
4. Новые codec-тесты используют перенесённые в common.record.internal классы
   CompressionType/MemoryRecords/SimpleRecord. Production-код не стал зависеть
   от новых internal record API.
5. Вызовы allTopicNames/topicNameValues, incrementalAlterConfigs, KafkaFuture,
   createPartitions и reassignment results в существующем коде уже соответствуют
   актуальному API. Их обработка futures/deadline/interrupt/partial result
   сохранена и проверена прежними unit-тестами.

ReplicaAssignmentPlanner и ReplicationSafetyPolicy не изменены. ReassignmentExecutor,
AdminReassignmentExecutor, ApplyManager и RF-валидация не ослаблены. В metadata reader
поправлен только устаревший комментарий: наличие новых accessor-ов не доказывает
fencing/admission; UNKNOWN по-прежнему блокирует исполнение. Новые API для допуска
брокеров и изменение production RF относятся к следующему заданию.

## 5. Jackson 3 и сохранение контрактов

[Официальная миграция](https://raw.githubusercontent.com/FasterXML/jackson/master/jackson3/MIGRATING_TO_JACKSON_3.md)
проверена вместе с [Jackson 3.3 release notes](https://github.com/FasterXML/jackson/wiki/Jackson-Release-3.3).

Runtime переведён на tools.jackson. Общие com.fasterxml.jackson.annotation остаются
официальной общей библиотекой annotations 2.23. Удалён отдельный Jdk8Module:
Optional поддерживается самим Jackson 3. Mapper-ы создаются через builders;
PlanManager получает отдельный rebuild со строгими features/coercion rules,
исходный mapper остаётся неизменным. Enum features перенесены в EnumFeature,
STRICT_DUPLICATE_DETECTION — в StreamReadFeature. Строковые числа, float→int,
unknown fields/operations, trailing tokens, null primitives и неизвестная RF schema
по-прежнему отклоняются для исполняемых планов.

YAMLMapper явно включает FAIL_ON_UNKNOWN_PROPERTIES, поскольку его новый default
отличается от прежнего. Остальные прежние YAML defaults сохранены; новые ограничения
схемы не вводились. JSON serializers используют SerializationContext, новые generator
property methods и DatabindException вместо старой иерархии.

JacksonException теперь unchecked и не является IOException. Перехват обновлён в
ParserService, PlanManager и ConfluentCloudService. Сохранены безопасные сообщения,
распознавание отсутствующего файла и RF-specific diagnostics; числовой overflow теперь
может приходить как core exception с property path. Ни исходный JSON/YAML, ни cause
с секретами не выдаются в пользовательскую ошибку. Confluent service account builder
получил JsonIgnoreProperties: Jackson 3 иначе отвергал прежние дополнительные поля
ccloud response. Синтетические valid/invalid/IO responses проверены без запуска ccloud.

**Мост FreeBuilder:** последняя стабильная 2.8.0 распознаёт только аннотацию
com.fasterxml.jackson.databind.annotation.JsonDeserialize при генерации @JsonProperty
на collection/Optional setters. В моделях сохранён этот compiler marker и добавлена
Jackson 3 runtime-аннотация. Jackson 2.23.0 используется только как compileOnly marker
API. В итоговом JAR отсутствуют Jackson 2 core/databind/YAML и FreeBuilder; наличие
неразрешаемой старой annotation descriptor не требует Jackson 2 runtime. Это проверено
запусками единственного JAR без внешнего classpath. Маркер можно убрать после выпуска
FreeBuilder с официальной поддержкой Jackson 3. Исходник JacksonSupport проверен в
официальном [sources artifact](https://repo.maven.apache.org/maven2/org/inferred/freebuilder/2.8.0/freebuilder-2.8.0-sources.jar).

Все прежние JSON/YAML fixtures сохранены побайтово. Изменения импортов/типов исключений
и builders в тестах отражают смену API; ожидаемая бизнес-семантика не заменялась.
previousValue, ADD/UPDATE/REMOVE, старые планы, deterministic placement и RF schema 1
прошли прежнюю регрессию.

## 6. Тестовая инфраструктура и command-config

Groovy 6 совместим с установленным JDK 21; использован соответствующий Spock
2.5-groovy-6.0. Jupiter/Platform/launcher согласованы BOM 6.1.3. Установлены явные
test engine и launcher, без удалённой Gradle auto-injection. JUnit 4 Rules сохранены
через spock-junit4 и system-rules 1.19.0; необходимые --add-opens сохранены.
Vintage engine удалён: отдельных JUnit 4 runner-тестов нет. cglib удалён после
проверки всех mock usages: единственный Spock Mock — интерфейс Admin, остальные
Kafka doubles используют JDK proxy или обычные fake classes. Сопоставление suite
counts подтвердило сохранение всех 814 исходных сценариев. Старый hamcrest-core 1.3
заменён уже используемым Hamcrest 3, чтобы исключить duplicate test classes.

Новые сценарии: 59 Jupiter codec/JNI tests, 5 Spock Confluent checks и один запрет
TWO_PHASE_COMMIT. Кодеки проверяют NONE/GZIP/SNAPPY/LZ4/ZSTD в Kafka record batches,
native arrays/direct buffers для LZ4/Snappy/Zstd, пустые/Unicode/повторяющиеся/случайные
payloads до 1 MiB и малые некорректные buffers. Эксплуатационные DoS/RCE PoC не запускались.

Загрузчик --command-config не переписан. Сохранились его unit tests и 49 CLI scenarios,
включая чтение явного файла, отсутствие/некорректность файла, JAAS redaction,
PLAIN/SCRAM/GSSAPI/OAUTHBEARER settings и verbose logging. Эти проверки не устанавливают
реальные SASL/SSL connections и не проверяют сертификаты/keytabs. Стандартные properties
передаются как прежде, без подмены настоящих credential values масками.

**Существующее ограничение приоритета:** loader сначала читает файл, затем environment;
KAFKA_* при конфликте продолжает перекрывать file. Это исходная семантика, а не регрессия
обновления. Без KAFKA_* явный файл самодостаточен. Требование приоритета файла *над env*
не реализовано: его изменение затронуло бы существующий контракт и должно быть явно
согласовано в задании 2. Fallback localhost и новая диагностика соединений не менялись.

## 7. Совместимость Kafka Brokers — статическая оценка

Статусы разделены: VERIFIED_BY_TEST означает испытание соответствующего реального
брокера; SUPPORTED_BY_API — пересечение необходимых protocol versions и анализ
клиентского API; NOT_TESTED — фактических испытаний нет; UNSUPPORTED — путь намеренно
недоступен. **Ни одному брокеру не присвоен VERIFIED_BY_TEST.**

| Broker | Стандартные Admin operations | Реальное соединение | Production RF execution |
| --- | --- | --- | --- |
| Apache Kafka 3.2.x, анализ тега 3.2.3 | SUPPORTED_BY_API | NOT_TESTED | UNSUPPORTED текущим барьером приложения |
| Apache Kafka 3.7.1 | SUPPORTED_BY_API | NOT_TESTED | UNSUPPORTED текущим барьером приложения |
| Apache Kafka 3.9.x, анализ тега 3.9.2 | SUPPORTED_BY_API | NOT_TESTED | UNSUPPORTED текущим барьером приложения |
| Arenadata Streaming / Kafka 4.3.1 | SUPPORTED_BY_API для upstream 4.3.1; vendor deviations NOT_TESTED | NOT_TESTED | UNSUPPORTED текущим барьером приложения |
| Apache Kafka 4.4.x, анализ тега 4.4.0 | SUPPORTED_BY_API | NOT_TESTED | UNSUPPORTED текущим барьером приложения |

Сопоставлены JSON schemas точных Apache tags. В таблице — validVersions requests;
для ApiVersions client fallback определяется дополнительно request builder-ом.
Пересечение диапазонов необходимо, но само по себе недостаточно для live support.

| Request | 3.2.3 | 3.7.1 | 3.9.2 | 4.3.1 | 4.4.0 |
| --- | --- | --- | --- | --- | --- |
| ApiVersions | 0–3 | 0–3 | 0–4 | 0–4 | 0–5 |
| Metadata | 0–12 | 0–12 | 0–12 | 0–13 | 0–13 |
| DescribeCluster | 0 | 0–1 | 0–1 | 0–2 | 0–2 |
| DescribeConfigs | 0–4 | 0–4 | 0–4 | 1–4 | 1–4 |
| IncrementalAlterConfigs | 0–1 | 0–1 | 0–1 | 0–1 | 0–1 |
| CreateTopics | 0–7 | 0–7 | 0–7 | 2–7 | 2–7 |
| DeleteTopics | 0–6 | 0–6 | 0–6 | 1–6 | 1–6 |
| CreatePartitions | 0–3 | 0–3 | 0–3 | 0–3 | 0–3 |
| AlterPartitionReassignments | 0 | 0 | 0 | 0–1 | 0–1 |
| ListPartitionReassignments | 0 | 0 | 0 | 0 | 0 |
| Describe/Create/DeleteAcls | 0–2 | 0–3 | 0–3 | 1–3 | 1–3 |
| DescribeLogDirs | 0–3 | 0–4 | 0–4 | 1–5 | 1–5 |
| DescribeTopicPartitions | отсутствует | отсутствует | 0 | 0 | 0 |

Индивидуальные URL всех schemas:
[protocol-ranges.json](modernization-stage1-2026-11-11/protocol-ranges.json).
В [KafkaAdminClient 4.4.0](https://github.com/apache/kafka/blob/4.4.0/clients/src/main/java/org/apache/kafka/clients/admin/KafkaAdminClient.java)
проверен fallback describeTopics по именам: UnsupportedVersionException от
DescribeTopicPartitions переводит запрос на Metadata. Собственный код не вызывает
DescribeTopicPartitions напрямую и не требует topic-ID-only API. Topic UUID читается
из результата по имени; отсутствие корректной identity не разрешает RF.

DescribeFeatures использует feature metadata ApiVersions; недоступные данные остаются
UNKNOWN. Usable-bytes/rack/leader/ISR/ELR/min ISR/authorization могут отсутствовать
или различаться; такие наблюдения не превращаются в READY. Новые fencing options
не включались. ZooKeeper API напрямую не используется — нужен Kafka bootstrap/Admin.
Точная broker version по ApiVersions не угадывается. Vendor ADS patches, Ranger,
rolling upgrades и фактическая auth/ACL policy требуют отдельных испытаний.
Общий контекст: [официальная compatibility matrix](https://kafka.apache.org/44/getting-started/compatibility/).

## 8. Повторный аудит безопасности

Разрешены compile/runtime/test compile/test runtime/annotationProcessor/JaCoCo,
build/settings plugin classpaths и фактический Groovy compiler classpath. Проверены
350 JAR нового официального Gradle distribution, включая Maven metadata внутри
shaded JAR. Сравнение содержит **594 публичные coordinate/version пары** до/после;
для финального состояния идентифицировано 437 пар, из них 215 не относятся к
внутреннему group org.gradle. Это не число обновлённых библиотек и не полный SBOM
каждого native/shaded компонента.

OSV querybatch и полные records сопоставлены с GHSA, repository advisories Gradle,
Shadow, Jackson core/databind, Zstd, Snappy и другими доступными первоисточниками.
NVD JSON API вернул все пять отдельно запрошенных записей: Kafka 111551/114349,
Shadow 75598, Guava 102554, Jackson 110879. Для LZ4 исправления восьми advisories
сверены через [официальные HTML advisories](https://github.com/yawkat/lz4-java/security/advisories)
после ограничения GitHub API rate limit. Ошибочный первоначальный API URL yawk
исправлен по SCM из официального POM на yawkat; 404/403 не считаются нулевыми findings.
Публичным сервисам передавались только общедоступные координаты и IDs, не исходники
и не Kafka configuration.

| Срез | Runtime OSV active matches | Build/plugin OSV | Gradle bundled OSV |
| --- | ---: | ---: | ---: |
| Stage 3, исторический отчёт | 37 | считать отдельно по прежнему отчёту | отдельно |
| Stage 4 / исходное состояние этого задания | 0 | 8 | 2 |
| Финальное состояние | 0 | 0 | 1 |

0 runtime совпадений уже было до задания и не скрывает Kafka CVE из vendor source.
Один финальный OSV match — Guava. **Дополнительно vendor advisory оставляет Jackson
в Gradle**, поэтому итог — два известных version-affected компонента инструментов
сборки, а не «уязвимостей нет». Отозванные advisories не учитывались как активные.
Сырые координаты, matches, ranges и references сохранены в
[sca-results.json](modernization-stage1-2026-11-11/sca-results.json),
дистрибутивные JAR/hash — в [gradle-jars.csv](modernization-stage1-2026-11-11/gradle-jars.csv).

### 8.1. Поимённая сверка обнаруженных проблем

| CVE / GHSA | Затронутый исходный компонент | Исправление / финальное состояние | Применимость |
| --- | --- | --- | --- |
| [CVE-2026-111551](https://kafka.apache.org/community/cve-list/#cve-2026-111551) | Kafka Client 3.9.2, ListDeserializer | 4.4.0, исправлено; vendor fixes также 4.2.2/4.3.2 | Client class присутствовал, но GitOps не использует ListDeserializer/consumer/Streams |
| [CVE-2026-75598 / GHSA-rr8v-fxxf-c986](https://github.com/GradleUp/shadow/security/advisories/GHSA-rr8v-fxxf-c986) | Shadow 8.3.11 | fixed ≥9.6.0; итог 9.6.1 | Build-time path traversal через dependency ZIP; текущие archives также проверены |
| [CVE-2025-67030 / GHSA-6fmv-xxpf-w3cw](https://github.com/advisories/GHSA-6fmv-xxpf-w3cw) | Plexus Utils 4.0.2 | fixed ≥4.0.3; итог 4.1.0 | Build extraction path, не runtime CLI |
| [CVE-2026-34477 / GHSA-6hg6-v5c8-fphq](https://github.com/advisories/GHSA-6hg6-v5c8-fphq) | Log4j Core 2.25.3 | fixed 2.25.4; итог 2.26.1 | TLS logging configuration, соответствующий appender не настроен |
| [CVE-2026-34478 / GHSA-445c-vh5m-36rj](https://github.com/advisories/GHSA-445c-vh5m-36rj) | Log4j Core 2.25.3 | fixed 2.25.4; итог 2.26.1 | Rfc5424Layout, build dependency |
| [CVE-2026-34480 / GHSA-3pxv-7cmr-fjr4](https://github.com/advisories/GHSA-3pxv-7cmr-fjr4) | Log4j Core 2.25.3 | fixed 2.25.4; итог 2.26.1 | XmlLayout, build dependency |
| [CVE-2026-49844 / GHSA-qv9r-c865-cp47](https://github.com/advisories/GHSA-qv9r-c865-cp47) | Log4j API 2.25.3 | fixed 2.25.5 / 2.26.1; итог 2.26.1 | MapMessage JSON, build dependency |
| [CVE-2026-34479 / GHSA-h383-gmxw-35v2](https://github.com/advisories/GHSA-h383-gmxw-35v2) | Scanner match Log4j Core 2.25.3 | итог 2.26.1; исходный bridge отсутствовал | Не считать доказанной исходной уязвимостью: требовался log4j-1.2-api |
| [CVE-2021-0341 / GHSA-3cqm-mf7h-prrj](https://github.com/advisories/GHSA-3cqm-mf7h-prrj) | OkHttp 3.14.2, embedded старого Sonar | Старый scanner-api удалён; текущий shaded scanner 4.3.0.11766 вне полученных matches | Условная применимость к JDK scanner; Sonar не запускался |
| [CVE-2023-3635 / GHSA-w33c-445m-f8w7](https://github.com/advisories/GHSA-w33c-445m-f8w7) | Okio 1.17.2, embedded старого Sonar | fixed 1.17.6 / 3.4.0; старая embedded copy удалена | Scanner network GZIP response; не приложение |
| [CVE-2025-48924 / GHSA-j288-q9x7-2f5v](https://github.com/advisories/GHSA-j288-q9x7-2f5v) | Commons Lang 2.6 в Gradle 8.14.6 | legacy Lang 2 отсутствует в Gradle 9.9.1; Lang3 3.20.0 | Recursive class-name parsing; bundled build component |
| [GHSA-3cfg-jpcr-gfp3](https://github.com/gradle/gradle/security/advisories/GHSA-3cfg-jpcr-gfp3) | Gradle 8.14.6, version catalog accessors | fixed 8.14.7 / 9.9.1; итог 9.9.1 | Недоверенные catalog values; catalog в проекте отсутствует |
| [GHSA-gqrr-4j89-5267](https://github.com/gradle/gradle/security/advisories/GHSA-gqrr-4j89-5267) | Gradle 8.14.6, daemon registry | fixed 8.14.7 / 9.9.1; итог 9.9.1 | Local daemon registry; --no-daemon сам по себе не заменяет fix |
| [GHSA-pcp8-98xm-c6pr](https://github.com/gradle/gradle/security/advisories/GHSA-pcp8-98xm-c6pr) | Gradle 8.14.6, stale registry/port | fixed 8.14.7 / 9.9.1; итог 9.9.1 | Local daemon communication |
| [CVE-2026-54516](https://github.com/FasterXML/jackson-databind/security/advisories/GHSA-9fxm-vc8v-hj55) | Jackson 2.18.11 в старом Gradle по vendor range | исправлено в текущих Gradle 2.22.3 / compiler 2.23 / app 3.3 | Mutator inference; путь в Gradle не был доказан |
| [CVE-2026-102554 / GHSA-xxph-c9ww-hj94](https://github.com/advisories/GHSA-xxph-c9ww-hj94) | Guava 33.4.6-jre в обоих Gradle | **ОСТАЁТСЯ**, fixed ≥33.7.2-jre | Подробности ниже |
| [CVE-2026-110879 / GHSA-m8xf-2cr7-77ph](https://github.com/FasterXML/jackson-databind/security/advisories/GHSA-m8xf-2cr7-77ph) | Gradle Jackson 2.18.11 → 2.22.3 | **ОСТАЁТСЯ в Gradle**; fixed 2.22.4; app 3.3/compiler 2.23 исправлены | Подробности ниже |

Последние три Gradle advisories опубликованы после исторического Stage 4 и
проверены заново. Предыдущие core fixes Gradle 8.14.6 не выдаются за устранение
этих новых проблем.

### 8.2. Остаточные риски инструментов сборки

**Guava 33.4.6-jre / CVE-2026-102554:** NVD CNA CVSS 4.0 **8.2**, GHSA CVSS 3.x
**5.9**. Уязвимость требует недоверенной native Java deserialization Guava collections.
Наличие затронутого кода подтверждено Maven metadata и фактическим JAR внутри Gradle.
Достижимость через конкретный Gradle input не доказана; это не основание снять finding.
Исправленная Guava — 33.7.2-jre. Обычный dependency constraint приложения не заменяет
библиотеку самого Gradle. [GHSA](https://github.com/advisories/GHSA-xxph-c9ww-hj94),
[NVD](https://nvd.nist.gov/vuln/detail/CVE-2026-102554).

**Jackson Databind 2.22.3 / CVE-2026-110879:** CVSS 3.1 **9.8**, fix **2.22.4**.
Условие — class-name/default polymorphism с недоверенным JSON и обход validator
через generic/array types. Версия в Gradle попадает в vendor affected range,
даже при нуле OSV matches для этого artifact. Опасная конфигурация/достижимость
именно в Gradle не подтверждены. Runtime GitOps 3.3.0 и compiler marker 2.23.0
вне диапазона. [Vendor advisory](https://github.com/FasterXML/jackson-databind/security/advisories/GHSA-m8xf-2cr7-77ph),
[NVD](https://nvd.nist.gov/vuln/detail/CVE-2026-110879).

На дату проверки 9.9.1 — последний стабильный Gradle. Его внутренние JAR не
перепаковывались и не подменялись вручную: это нарушило бы проверенный официальный
дистрибутив и не дало бы поддерживаемого исправления. До production build pipeline
нужны исправленный Gradle либо отдельно рассмотренное исключение с проверкой путей,
доверенных inputs и изоляцией workspace/cache/daemon registry. Это рекомендации,
исключение безопасности в рамках задания не выдавалось.

### 8.3. Compression и broker vulnerabilities

Текущие at.yawk.lz4 1.14.0, Snappy 1.1.10.12, Zstd JNI 1.5.7-26 не имеют matches
в полученных OSV/vendor диапазонах. Восемь LZ4 fixes: 1.12.1 для
[GHSA-48mj-8f7c-7qfm](https://github.com/yawkat/lz4-java/security/advisories/GHSA-48mj-8f7c-7qfm),
1.11.4 для native extraction/recursion/frame buffer issues,
1.11.2 для двух length-allocation issues, 1.11.1 для XXHash ranges,
1.10.1 для disclosure. Все ниже сохранённой 1.14.0.
13 Zstd и 11 Snappy repository advisories также сверены: их fixes не выше
сохранённых версий. Это проверка известных диапазонов, не полный аудит C/C++.
Источники: [LZ4](https://github.com/yawkat/lz4-java/security/advisories),
[Zstd](https://github.com/luben/zstd-jni/security/advisories),
[Snappy](https://github.com/xerial/snappy-java/security/advisories).

**Broker CVE не исправлялись обновлением клиента.** В частности,
[CVE-2026-114349](https://kafka.apache.org/community/cve-list/#cve-2026-114349),
CVSS 3.1 6.5, касается server compressed-batch decoding; upstream 4.3.1 требует
vendor backport/исправления, fixed upstream 4.3.2/4.4.0. Аналогично необходима
отдельная проверка server CVE-2026-112375, 111115, 111108 и OAuth defaults.
ADS edition/build/backport status неизвестны. Уязвимости Streams/Connect относятся
к их отдельным модулям, которых нет в JAR GitOps. [Apache security list](https://kafka.apache.org/community/cve-list/).

Пределы аудита: неизвестные exact versions некоторых repackaged/shaded компонентов
FreeBuilder/плагинов, native ABI/C libraries, JRE/OS и контейнеры. Docker base image
остаётся прежним и не был просканирован. Нельзя переносить 0 Maven runtime matches
на безопасность брокеров, ОС, JDK, внешних Logback configs или SASL callbacks.

## 9. Сборка, регрессия и JAR

| Контрольная точка | Результат |
| --- | --- |
| A: исходная сборка, принудительный rerun безопасных тестов, dependency graph | SUCCESS, 814/814 |
| B: Gradle/Shadow/settings/DSL | SUCCESS, 814/814 |
| C: Kafka 4.4/API fixtures/ACL guard | SUCCESS, 815/815 |
| D: Jackson/Groovy/JUnit/все существенные transitive dependencies | SUCCESS |
| E: повторный OSV/GHSA/vendor/NVD audit | выполнен, два остаточных Gradle findings |
| F: clean/classes/testClasses/test/jacocoTestReport/shadowJar/buildExecutableJar/buildRelease | SUCCESS, 879/879 |
| Configuration cache store + reuse | SUCCESS |
| Принудительная повторная компиляция и упаковка | SUCCESS, SHA-256 JAR совпал |

При переносе Kafka сначала выявлены obsolete TopicListing constructors и новый
AclOperation; при Jackson — changed unknown-property defaults и core overflow
exception path. Новые Confluent tests выявили игнорирование interface-only
JsonIgnoreProperties при builder mapping. Эти дефекты исправлены в production/API
адаптации. Ожидаемые результаты прежних бизнес-тестов не ослаблялись.

Финальный вызов через Windows Wrapper (обычные unit tests безопасны без init script):

```powershell
.\gradlew.bat --no-daemon --no-scan --console plain --warning-mode all clean classes testClasses test jacocoTestReport shadowJar buildExecutableJar buildRelease
```

Локальные audit вызовы дополнительно указывали существующий Gradle user home и
read-only inventory init script. Никакие задачи sonar, внешние integration tests
или Kafka write API не запускались.

| Характеристика артефакта | Результат |
| --- | --- |
| Основной JAR | build/libs/kafka-gitops-all.jar |
| Размер | 25 790 267 bytes, 24,60 MiB |
| SHA-256 | a93fe70ae9b5920150895c8237d9ffb1f7cff2eb9d5736ae53bd825bb18973c8 |
| Main-Class | com.devshawn.kafka.gitops.MainCommand |
| Manifest | Multi-Release: true; внешний Class-Path отсутствует |
| Runtime JAR components | 13, один kafka-clients 4.4.0 |
| ZIP entries / classes | 8019 / 7271 |
| Native resources / service descriptors | 53 / 5 |
| Conflicting/duplicate runtime classes | 0 |
| Java | source/target/release 21, effective maximum class major 65 |
| MR versions выше 21 | 22 и 23; JVM 21 их игнорирует |
| Исключённые компоненты | Kafka 3.x, Jackson 2 core/databind, FreeBuilder, тестовые движки |

Все runtime classes/native resources/pom.properties сопоставлены побайтово с
разрешёнными исходными JAR; module-info намеренно исключены. Providers всех пяти
service descriptors совпадают с объединением runtime dependencies. Проверены
отсутствие ZIP traversal/signatures/duplicate entries и корректность release ZIP
с POSIX launcher. MainCommand major 65 и фактический запуск на Java 21 подтверждают
минимум 21; требований Java 25 у исполняемой части нет.

CLI smoke: --help/--version, help подкоманд, valid/invalid local YAML, legacy/new/unknown
JSON plans, RF/decrease/no-delete guards, command-config read failures и verbose
secret logging — **49 checks**, ожидаемые exit codes 0/2 сохранены. Это не тест
сетевой аутентификации. Внутренний verifier разрешал только validate/help,
plan/apply с обеими ресурсными категориями skipped либо доказанный локальный
RF permission barrier до создания Admin.

Windows JNI непосредственно из fat JAR: **60 array + 60 direct-buffer + 36 Kafka
record-batch roundtrips**, LZ4 safe/fast Java/native, Snappy native, Zstd JNI,
12 payloads, включая пустые/Unicode/случайные/повторяющиеся до 1 MiB.
Linux/macOS native-файлы сохранены побайтово, но не исполнялись.
Полные suite counts, CLI labels и свойства JAR:
[verification.json](modernization-stage1-2026-11-11/verification.json).

## 10. Ограничения, следующие задания и контроль изменений

1. Два известных version-affected компонента остаются внутри последнего Gradle;
   исправления/исключения для production CI остаются открытыми, см. §8.2.
2. Нужен официальный Jackson-3-aware FreeBuilder, чтобы удалить compiler marker
   Jackson 2.23. Сейчас это безопасный compileOnly мост, проверенный чистой сборкой.
3. Старая system-rules требует двух --add-opens на Java 21. Миграция Rules/env injection
   в этой задаче не требовалась для обновления Groovy/JUnit и не выполнялась.
4. Приоритет command-config/env, localhost fallback, новая диагностика, RF defaults,
   WARN/BLOCK UX и допуск брокеров — обсуждение и работа задания 2, без изменений здесь.
5. Для задания 3 нужны отдельно разрешённые auth/Admin испытания целевых брокеров
   и целевых ОС. Ни один broker не VERIFIED_BY_TEST, production RF по-прежнему закрыт.
6. ADS backports/server CVE, JDK/OS/container scan и external configs требуют отдельной
   проверки; обновление клиента их не исправляет.

Бизнес-политики RF/ACL, YAML/JSON fixtures, действующие CLI flags и защита секретов
сохранены. Git commit/staging/смена ветки не выполнялись. git diff --check проходит.
Полный git status --short и git diff --stat проверены после подготовки отчёта;
итоговые изменения перечислены ниже. Аудитные временные scripts/logs находятся в
игнорируемом `.gradle/modernization-stage1/`; основные доказательства сохранены в
репозитории рядом с этим отчётом и не требуют существования локального кеша.

### Изменённые и новые файлы

- `build.gradle`
- `docs/audit/modernization-stage1-2026-11-11.md`
- `docs/audit/modernization-stage1-2026-11-11/dependencies.csv`
- `docs/audit/modernization-stage1-2026-11-11/dependency-inventory.json`
- `docs/audit/modernization-stage1-2026-11-11/gradle-jars.csv`
- `docs/audit/modernization-stage1-2026-11-11/protocol-ranges.json`
- `docs/audit/modernization-stage1-2026-11-11/sca-results.json`
- `docs/audit/modernization-stage1-2026-11-11/verification.json`
- `gradle.properties`
- `gradle/wrapper/gradle-wrapper.jar`
- `gradle/wrapper/gradle-wrapper.properties`
- `gradlew`
- `gradlew.bat`
- `settings.gradle`
- `src/main/java/com/devshawn/kafka/gitops/StateManager.java`
- `src/main/java/com/devshawn/kafka/gitops/config/KafkaGitopsConfig.java`
- `src/main/java/com/devshawn/kafka/gitops/config/ManagerConfig.java`
- `src/main/java/com/devshawn/kafka/gitops/domain/confluent/ServiceAccount.java`
- `src/main/java/com/devshawn/kafka/gitops/domain/options/GetAclOptions.java`
- `src/main/java/com/devshawn/kafka/gitops/domain/plan/AclPlan.java`
- `src/main/java/com/devshawn/kafka/gitops/domain/plan/BrokerSnapshot.java`
- `src/main/java/com/devshawn/kafka/gitops/domain/plan/DesiredPlan.java`
- `src/main/java/com/devshawn/kafka/gitops/domain/plan/PartitionReplicaAssignment.java`
- `src/main/java/com/devshawn/kafka/gitops/domain/plan/PlanOverview.java`
- `src/main/java/com/devshawn/kafka/gitops/domain/plan/TopicConfigPlan.java`
- `src/main/java/com/devshawn/kafka/gitops/domain/plan/TopicPartitionPlan.java`
- `src/main/java/com/devshawn/kafka/gitops/domain/plan/TopicPlan.java`
- `src/main/java/com/devshawn/kafka/gitops/domain/plan/TopicReplicationPlan.java`
- `src/main/java/com/devshawn/kafka/gitops/domain/plan/serialization/TopicConfigPlanSerializer.java`
- `src/main/java/com/devshawn/kafka/gitops/domain/plan/serialization/TopicPlanSerializer.java`
- `src/main/java/com/devshawn/kafka/gitops/domain/plan/serialization/TopicReplicationPlanSerializer.java`
- `src/main/java/com/devshawn/kafka/gitops/domain/state/AclDetails.java`
- `src/main/java/com/devshawn/kafka/gitops/domain/state/CustomAclDetails.java`
- `src/main/java/com/devshawn/kafka/gitops/domain/state/DesiredState.java`
- `src/main/java/com/devshawn/kafka/gitops/domain/state/DesiredStateFile.java`
- `src/main/java/com/devshawn/kafka/gitops/domain/state/TopicDetails.java`
- `src/main/java/com/devshawn/kafka/gitops/domain/state/UserDetails.java`
- `src/main/java/com/devshawn/kafka/gitops/domain/state/service/ApplicationService.java`
- `src/main/java/com/devshawn/kafka/gitops/domain/state/service/KafkaConnectService.java`
- `src/main/java/com/devshawn/kafka/gitops/domain/state/service/KafkaConnectStorageTopics.java`
- `src/main/java/com/devshawn/kafka/gitops/domain/state/service/KafkaConnectorDetails.java`
- `src/main/java/com/devshawn/kafka/gitops/domain/state/service/KafkaStreamsService.java`
- `src/main/java/com/devshawn/kafka/gitops/domain/state/settings/Settings.java`
- `src/main/java/com/devshawn/kafka/gitops/domain/state/settings/SettingsCCloud.java`
- `src/main/java/com/devshawn/kafka/gitops/domain/state/settings/SettingsFiles.java`
- `src/main/java/com/devshawn/kafka/gitops/domain/state/settings/SettingsServices.java`
- `src/main/java/com/devshawn/kafka/gitops/domain/state/settings/SettingsServicesAcls.java`
- `src/main/java/com/devshawn/kafka/gitops/domain/state/settings/SettingsTopics.java`
- `src/main/java/com/devshawn/kafka/gitops/domain/state/settings/SettingsTopicsDefaults.java`
- `src/main/java/com/devshawn/kafka/gitops/domain/state/settings/SettingsTopicsList.java`
- `src/main/java/com/devshawn/kafka/gitops/manager/PlanManager.java`
- `src/main/java/com/devshawn/kafka/gitops/service/ConfluentCloudService.java`
- `src/main/java/com/devshawn/kafka/gitops/service/ParserService.java`
- `src/main/java/com/devshawn/kafka/gitops/service/ReplicationMetadataReader.java`
- `src/main/java/com/devshawn/kafka/gitops/util/TopicConfigSecurity.java`
- `src/test/groovy/com/devshawn/kafka/gitops/PlanTestSupport.groovy`
- `src/test/groovy/com/devshawn/kafka/gitops/ReplicationApplicationWorkflowSpec.groovy`
- `src/test/groovy/com/devshawn/kafka/gitops/domain/state/CustomAclDetailsSpec.groovy`
- `src/test/groovy/com/devshawn/kafka/gitops/manager/PartitionApplySpec.groovy`
- `src/test/groovy/com/devshawn/kafka/gitops/manager/PlanFixturesSpec.groovy`
- `src/test/groovy/com/devshawn/kafka/gitops/manager/PlanJsonSpec.groovy`
- `src/test/groovy/com/devshawn/kafka/gitops/manager/PlanManagerSpec.groovy`
- `src/test/groovy/com/devshawn/kafka/gitops/manager/ReplicationPlanningSpec.groovy`
- `src/test/groovy/com/devshawn/kafka/gitops/service/ConfluentCloudServiceSpec.groovy`
- `src/test/groovy/com/devshawn/kafka/gitops/service/ReplicationMetadataSpec.groovy`
- `src/test/java/com/devshawn/kafka/gitops/CompressionCompatibilityTest.java`
