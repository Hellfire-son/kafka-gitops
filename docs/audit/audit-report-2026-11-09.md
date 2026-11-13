<!-- SPDX-License-Identifier: Apache-2.0 -->
> Historical stage evidence: the dates, versions, hashes and test outcomes below describe that stage and artifact. Personal machine paths were anonymized for publication. See [0.4.0 preparation](../release/v0.4.0-release-preparation.md) for the current release status.

# Аудит зависимостей Kafka GitOps для ADS

Дата проверки: **9 ноября 2026 года**. Репозиторий `joschi/kafka-gitops`, основа 0.3.1; рабочая ветка `feature/ads-gitops`, HEAD `349df9be1a497f93ee40e4dd4d94fe70bf5aad5e`. Учтены уже существующие изменения задач №1 и №2. Среда: Windows 11, Eclipse Temurin JDK 21.0.13.

**Вывод.** Обновление требуется и для runtime, и для сборки. Приоритеты: Gradle 8.7 и устаревшие плагины; Jackson 2.17.1; Logback 1.5.6; Kafka Client 3.7.0 и его библиотеки сжатия. Наличие уязвимой версии не означает, что все описанные атаки доступны через текущий CLI: приложение создаёт AdminClient, не Producer/Consumer, а многие advisories требуют отдельных parser/appender/JNI API. Обоснование применимости приведено ниже и в приложении.

Окончательно выбрать kafka-clients без версии ADS/брокеров нельзя. **3.9.2 — условная промежуточная цель, а не полностью исправленная ветка**: CVE-2026-111551 затрагивает также 3.9.2, хотя уязвимый ListDeserializer текущим приложением не используется. **4.4.0 — актуальная стабильная версия, требующая отдельной проверки совместимости с ADS**, не автоматическая рекомендация к установке. [Apache Kafka security](https://kafka.apache.org/community/cve-list/), [CVE-2026-111551](https://www.cve.org/CVERecord?id=CVE-2026-111551).

## Объём проверки и воспроизводимость

Прочитаны `build.gradle`, `settings.gradle`, Wrapper properties, Dockerfile, build scripts, места создания Kafka AdminClient, JSON/YAML-модели и настройки логирования. Разрешены локальные `compileClasspath`, `runtimeClasspath`, `testCompileClasspath`, `testRuntimeClasspath`, annotationProcessor, classpaths плагинов проекта/settings. Дополнительно проверены библиотеки дистрибутива Gradle и фактическое содержимое fat JAR.

Выполнены безопасные команды Gradle **8.7**, с `--offline --no-daemon --no-scan`: `dependencies`, `buildEnvironment`, `shadowJar -x test`, временная задача инвентаризации через init script в `build/audit/`, `help --warning-mode all -Dorg.gradle.deprecation.trace=true`. `shadowJar` завершился успешно; `compileJava` был `UP-TO-DATE`, чистая пересборка не выполнялась. Полные журналы: [деревья и сборка](gradle-dependencies-and-build.txt), [инвентаризация](resolved-inventory-command.txt), [deprecation stack traces](gradle-deprecation-traces.txt).

Тесты, Sonar analysis, build scans, интеграционные задачи и подключения к Kafka не запускались. `jacocoTestReport` зависит от `test`, поэтому его намеренно не запускали. Публичным OSV, Maven Central, GitHub и CVE/NVD передавались только общедоступные координаты/версии пакетов и идентификаторы advisories; исходники, connection properties, секреты и внутренние конфигурации не отправлялись.

Использованы OSV batch API и GitHub advisories API, записи CNA/CVE, NVD, security/release notes производителей, Maven metadata и Gradle Plugin Portal. Первичная проверка **55 версий Maven-компонентов дала 44 уникальных advisories**; анализ идентифицируемых библиотек Gradle добавил ещё 15. Итого **59 совпадений OSV по версиям**, включая спорное сопоставление BC-PGP; это не число доказанных уязвимостей приложения. Ручная проверка первоисточников выявила дополнительные записи, ещё отсутствовавшие в ответе OSV. [Полный реестр OSV с диапазонами, CVSS, fixes и применимостью](vulnerabilities-osv.md), [CSV](vulnerabilities-osv.csv).

Проверка имеет документированные пробелы: `jacocoAnt:0.8.11` не разрешён из offline cache; точные версии shaded компонентов FreeBuilder/cglib не восстановлены; не каждый внутренний JAR Gradle имеет Maven metadata; native C/C++ и контейнерные пакеты требуют отдельного анализа. **Полный аудит всех встроенных компонентов не завершён**; результат нельзя обозначать «все зависимости безопасны» или «полный SBOM проверен». Базы публичных уязвимостей доступны, их проверка для идентифицированных координат выполнена.

## Инвентаризация

| Конфигурация | Физические artifacts | Результат |
| --- | --- | --- |
| compileClasspath | 12 | Полностью разрешена; включает compileOnly FreeBuilder, не включает runtime-only compression JAR |
| runtimeClasspath | 14 | Полностью разрешена; все 14 библиотек найдены в fat JAR |
| testCompileClasspath | 28 | Полностью разрешена |
| testRuntimeClasspath | 32 | Полностью разрешена; включает весь runtime приложения |
| annotationProcessor | 1 | FreeBuilder 2.8.0 |
| jacocoAgent | 1 | org.jacoco.agent 0.8.11 |
| jacocoAnt | 0 разрешённых | org.jacoco.ant 0.8.11 отсутствует в offline cache, транзитивное дерево этой опциональной конфигурации не подтверждено |
| buildClasspath | 16 | Плагины проекта и их библиотеки |
| settingsClasspath | 4 | Enterprise/Develocity 3.17.2, Foojay 0.8.0, Gson 2.10.1 |
| Дистрибутив Gradle 8.7 | 262 JAR | Список файлов сохранён; 77 Maven metadata координат + 9 проверок по именам файлов |

Все координаты и версии по конфигурациям приведены в [dependency-inventory.md](dependency-inventory.md), объединённый список — [dependency-inventory.csv](dependency-inventory.csv); полный список JAR дистрибутива — [gradle-distribution-jars.csv](gradle-distribution-jars.csv). Platform/BOM не являются физическими JAR, их версии отражены отдельно в таблице А и дереве Gradle.

**Runtime, полный список:** kafka-clients 3.7.0; jackson-annotations/core/databind/dataformat-yaml/datatype-jdk8 2.17.1; logback-classic/core 1.5.6; slf4j-api 2.0.13; picocli 4.7.5; zstd-jni 1.5.5-6; org.lz4:lz4-java 1.8.0; snappy-java 1.1.10.5; snakeyaml 2.2.

**Дополнительно только для тестов:** Groovy 4.0.21; Spock core/junit4 2.3-groovy-4.0; JUnit Jupiter aggregate/api/params/engine 5.10.2; Vintage 5.10.2; JUnit Platform commons/engine 1.10.2; JUnit 4.13.2; cglib-nodep 3.3.0; system-rules 1.19.0; JSONassert 1.5.1; android-json 0.0.20131108.vaadin1; Hamcrest 2.2 и hamcrest-core 1.3; apiguardian-api 1.1.2; opentest4j 1.3.0. Одновременное присутствие старого hamcrest-core и нового hamcrest требует проверки дублирующихся классов при модернизации тестового стека.

**Build-time:** FreeBuilder 2.8.0; Shadow 8.1.1 → Commons IO 2.11.0, Ant/launcher 1.10.13, ASM/commons/tree 9.4, JDOM 2.0.6.1, Plexus Utils 3.5.1, jdependency 2.8.0, Log4j API/core 2.20.0 и groovy-backports-compat23 2.3.5; Gradle processors 3.7.0; Sonar plugin 5.0.0.4638/scanner-api 2.16.2.588; settings plugins и Gson. Это отдельные classloaders сборки: **Log4j 2.20.0 не включён в приложение и не равен Log4Shell**. Собственный Groovy Gradle 8.7 имеет версию 3.0.17 и обновляется с Gradle, отдельно от test Groovy 4.0.21.

В FreeBuilder найдены shaded Guava/google-java-format и связанные классы (3405 совпавших entries), хотя POM не объявляет транзитивных зависимостей. В cglib-nodep найдены repackaged ASM entries. Точные версии встроенного кода не подтверждены: «нет CVE для Maven artifact» не равно «проверены все встроенные библиотеки».

**Проверенный исполняемый Java-артефакт:** `build/libs/kafka-gitops-all.jar`, 21 458 844 bytes, SHA-256 `CA41461F438CE383F6F2CA2A7361E074FC4ED77DECC1DD5DBA5CBDC1FD9D4FB2`; 6788 ZIP entries, 6141 classes, 46 native libraries. Все runtime artifacts обнаружены по фактическим классам, FreeBuilder/test/build plugin классы в fat JAR не включены. [Результат проверки JAR](fat-jar-summary.json), [компоненты](fat-jar-runtime-components.csv), [все entries](fat-jar-entries.txt).

Manifest содержит правильный Main-Class и избыточный Class-Path с именами уже встроенных библиотек. Есть 12 entries с versioned classes в `META-INF/versions/`, но нет `Multi-Release: true`: JVM не использует их как MR-JAR implementations. Это отдельная проблема упаковки для проверки в задаче №4, не доказанная CVE. При обновлении Shadow нужно сохранить service providers Jackson/SLF4J и native resources. `build.sh` добавляет shell stub к тому же JAR; получаемый shell executable отдельно не собирался. `buildExecutableJar` требует `sh`: для Windows нужен Git Bash/WSL либо Linux CI.

## А. Прямые зависимости

«Безопасная цель» ниже означает закрытие выявленных advisories и разумную совместимость, а не гарантию отсутствия неизвестных дефектов. Новые версии **не устанавливались**. В колонке «последняя стабильная» RC, alpha, beta и snapshot исключены; в metadata поле `release` иногда указывает prerelease, поэтому оно не принималось автоматически за стабильный релиз.

| Компонент / область | Сейчас | Последняя стабильная на 09.11.2026 | Совместимая целевая версия | CVE / риск | Java 21, breaking changes и рекомендация |
| --- | --- | --- | --- | --- | --- |
| org.apache.kafka:kafka-clients / Runtime | 3.7.0 | 4.4.0 | **Условно 3.9.2**; окончательная версия после ADS inventory | CVE-2024-31141, 2025-27817, 2026-33558, 2026-35554; 2026-111551 остаётся в 3.x | На Java 21 возможны обе ветки; проверить SSL/SASL callbacks и брокерные API. 3.7.2 — последний 3.7.x, но недостаточный security target. 4.x удаляет старые API и поддержку брокеров <2.1. См. раздел Г. |
| com.fasterxml.jackson:jackson-bom / Runtime platform | 2.17.1 | 2.23.0 в 2.x; семейство 3.x — 3.3.0 | **2.21.8 LTS**, согласованно для модулей | Собственного исполняемого кода нет; управляет уязвимыми core/databind | Java 21 подходит. Последний 2.17.x — 2.17.3 — не закрывает новые CVE. Нужны JSON/YAML regression tests. 3.x требует смены координат/packages. |
| jackson-databind / Runtime | 2.17.1 (BOM) | 2.23.0 в текущих координатах | **2.21.8** через BOM | 12 совпадений CVE; в том числе 2026-54512/54513, 2026-91776/91777 | Приоритетное обновление; многие validator/type CVE требуют неиспользуемых режимов. Id.NAME используется, его нельзя объявлять отсутствующим. |
| jackson-dataformat-yaml / Runtime | 2.17.1 (BOM) | 2.23.0 | **2.21.8** через BOM | Отдельной записи для этого artifact не найдено; общий parser/core и SnakeYAML | Проверить YAML типизацию, duplicates, null, ограничения размеров; Java 21 подходит. Не обновлять модуль отдельно от BOM. |
| jackson-datatype-jdk8 / Runtime | 2.17.1 (BOM) | 2.23.0 | **2.21.8** через BOM | Отдельного совпадения не найдено; зависит от Jackson core/databind | Optional и совместимость старых планов проверить. В Jackson 3 часть поддержки встроена, drop-in обновления нет. |
| info.picocli:picocli / Runtime | 4.7.5 | 4.7.7 | **4.7.7** | Совпадений CVE в выполненной проверке нет; низкий риск | Java 21 совместим; проверить parsing/help/exit codes и флаги skip/no-delete/prefixes. Рекомендуется. |
| org.slf4j:slf4j-api / Runtime | 2.0.13 | **2.0.20** | **2.0.20** | Совпадений не найдено; не путать с Logback advisories | Остаётся ветка 2.0; Java 21 подходит. 2.1.0-alpha1 исключён. Проверить единственный provider в fat JAR. |
| ch.qos.logback:logback-classic / Runtime | 1.5.6 | **1.6.6** | **1.6.6** | CVE-2026-19880 и 2026-104721, conditional MDC/SiftingAppender | Latest 1.5.x = 1.5.38, но по диапазонам CNA не закрывает все classic CVE. 1.6.x требует Java 11+, совместим с Java 21/SLF4J 2.0. Проверить custom XML. |
| ch.qos.logback:logback-core / Runtime | 1.5.6 | **1.6.6** | **1.6.6** синхронно с classic | CVE-2024-12798/12801, 2025-11226, 2026-1225/9828/10532/13006 | Обновить; многие сценарии требуют Janino/config control/socket receiver. Janino conditionals удалены в новых версиях; custom config может потребовать переписывания. |
| org.inferred:freebuilder / Build-time, compileOnly + processor | 2.8.0 | **2.8.0** | Пока **2.8.0**, security полнота shaded кода не подтверждена | Прямого совпадения нет; неизвестны точные версии embedded Guava/formatter | Сборка использует JDK 21, но свежая чистая генерация в аудите не проверялась. Удалить устаревший processors plugin, сохранив native annotationProcessor/compileOnly и IDE generation. |
| org.junit.jupiter:junit-jupiter / Test-only | 5.10.2 | **6.1.3** | **5.14.4** в согласованном JUnit BOM | Прямых совпадений нет; средний риск миграции engines | Java 21 подходит. Не смешивать platform 1.10 с engines 5.14. JUnit 6 требует Java 17+ и отдельной проверки Spock API/engines. |
| junit:junit / Test-only | 4.13.2 | **4.13.2** | **4.13.2**, затем оценить сокращение использования | Старые CVE JUnit до 4.13.1 не переносятся на 4.13.2 | На Java 21 сохраняется legacy/reflection риск Rules. Нужен spock-junit4 для существующих @Rule; не удалять автоматически. |
| org.junit.vintage:junit-vintage-engine / Test-only | 5.10.2 (JUnit alignment) | **6.1.3** | **5.14.4**, если engine нужен | Прямых совпадений нет | Проверить discovery: поддержка JUnit4 Rules в Spock и запуск JUnit4 тестов Vintage — разные задачи. При отсутствии JUnit4 tests engine может оказаться лишним. |
| org.apache.groovy:groovy / Test-only | 4.0.21 | **6.0.0** | **4.0.33** | Прямых совпадений для этой версии в выполненных запросах нет | Остаться в 4.x, поддерживающей Java 21; переход 5/6 отдельно. Не принуждать Groovy 4 вместо внутреннего Groovy Gradle. |
| org.spockframework:spock-core / Test-only | 2.3-groovy-4.0 | **2.5**, варианты Groovy 2.5/3/4/5/6 | **2.5-groovy-4.0** | Прямых совпадений нет; discovery/mocks regression | Java 21 подходит при совместимом Groovy. Опубликованный POM 2.5 импортирует JUnit BOM 5.14.4 и Groovy 4.0.32; целевой 4.0.33 — patch upgrade. |
| org.spockframework:spock-junit4 / Test-only | 2.3-groovy-4.0 | **2.5** | **2.5-groovy-4.0**, вместе с core | Прямых совпадений нет | Сохранить поддержку Rules, проверить EnvironmentVariables и restore после failed tests. |
| cglib:cglib-nodep / Test-only | 3.3.0 | **3.3.0**, новых стабильных нет | Безопасная новая версия не установлена; **оценить удаление** | Неизвестная полнота shaded ASM; проект unmaintained | Upstream предупреждает о проблемах JDK 17+. Java 21 нельзя считать надёжно поддерживаемой. В найденных тестах нет явных concrete-class Mock/Stub/Spy; удаление всё равно требует проверки engines/mocks. |
| com.github.stefanbirkner:system-rules / Test-only | 1.19.0 | **1.19.0** | Пока оставить; запланировать замену/изоляцию env access | Совпадений нет; старый reflection-based механизм | Требует --add-opens для изменения environment на Java 21; это не нативная совместимость. Замена требует переписывания тестовой инфраструктуры, не случайного version bump. |
| org.skyscreamer:jsonassert / Test-only | 1.5.1 | **1.5.3** | **1.5.3** | Прямых совпадений нет; android-json legacy | 2.0-rc1 исключён. Проверить lenient/strict semantics; android-json — другой Maven artifact, CVE org.json нельзя переносить без анализа кода. |

Версии проверены по [Maven Central metadata Kafka](https://repo.maven.apache.org/maven2/org/apache/kafka/kafka-clients/maven-metadata.xml), [Jackson BOM](https://repo.maven.apache.org/maven2/com/fasterxml/jackson/jackson-bom/maven-metadata.xml), [JUnit](https://repo.maven.apache.org/maven2/org/junit/jupiter/junit-jupiter/maven-metadata.xml), [Groovy](https://repo.maven.apache.org/maven2/org/apache/groovy/groovy/maven-metadata.xml), [Spock](https://repo.maven.apache.org/maven2/org/spockframework/spock-core/maven-metadata.xml); адреса metadata остальных пакетов сохранены в [maven-metadata.json](maven-metadata.json). Использовать версии из Plugin Portal для Gradle plugins: старые Maven Central координаты некоторых плагинов неполны.

Jackson 2.21 — LTS; на дату проверки выпущена **2.21.8**, а **2.21.9 ещё не выпущена**, хотя упоминается в release notes. 2.23.0 — свежий стабильный minor, но не необходимая цель первого обновления. [Release 2.21](https://github.com/FasterXML/jackson/wiki/Jackson-Release-2.21), [Release 2.23](https://github.com/FasterXML/jackson/wiki/Jackson-Release-2.23). Подтверждения Logback/SLF4J: [Logback news](https://logback.qos.ch/news.html), [SLF4J news](https://www.slf4j.org/news.html). Для тестового стека: [Spock 2.5](https://spockframework.org/spock/docs/2.5/all_in_one.html), [POM Spock](https://repo.maven.apache.org/maven2/org/spockframework/spock-core/2.5-groovy-4.0/spock-core-2.5-groovy-4.0.pom), [JUnit 5.14.4](https://docs.junit.org/5.14.4/overview.html), [cglib status](https://github.com/cglib/cglib).

## Б. Уязвимые транзитивные зависимости

Сводная таблица ниже отделяет runtime от среды сборки. **Полные строки каждой CVE/GHSA — диапазон, числовой CVSS с версией, исправление и применимость — находятся в [реестре](vulnerabilities-osv.md).** Таблица не заменяет оценку достижимости. CVSS v3.1 и v4.0 напрямую не сравниваются как одна шкала угрозы конкретному приложению.

| Компонент / область | Обнаружено | CVE/GHSA и воздействие | Исправление / цель | Применимость и риск |
| --- | --- | --- | --- | --- |
| jackson-core / Runtime через BOM/YAML/databind | 2.17.1 | CVE-2026-18401, 68494 — async parser DoS; 89425 — DataInput DoS; 89407 — NumberInput ReDoS | Для поздних записей 2.18.11 / 2.21.7 / 2.22.3; цель **2.21.8** | Async/DataInput не используются. Numeric conversion недоверенных файлов требует внимания. До 8.7 CVSS4 по отдельным записям. |
| org.lz4:lz4-java / Runtime через Kafka | 1.8.0 | CVE-2025-12183/66566; 2026-106449/106450/106451/106452/106453/59949; дополнительно GHSA-48mj-8f7c-7qfm | Старый artifact 1.8.1 закрывает только часть; перейти на **at.yawk.lz4:lz4-java:1.14.0**, один provider | До 8.8 CVSS4. Record decompression в AdminClient не используется; temp/JNI — отдельный локальный сценарий. Смена координат требует constraints/module replacement, исключения дублирующего net.jpountz. |
| snappy-java / Runtime через Kafka | 1.1.10.5 | CVE-2026-93451/108106 — native bounds/OOM; 116590 — crash при диапазонах | Для этих CVE **1.1.10.9**, цель **1.1.10.12** | 7.5/7.5/5.9 CVSS3.1. Уязвимые API не найдены в текущем AdminClient; библиотека упакована. |
| zstd-jni / Runtime через Kafka | 1.5.5-6 | 8 CVE и новые GHSA: native OOB, UAF, OOM/DoS; подробная отдельная таблица ниже | Старые CVE: **1.5.7-14**; полный выбранный target **1.5.7-26** | До 8.8 CVSS4 CNA; требуется конкретный JNI API/недоверенные входы. Нет доказательства удалённой атаки через текущий AdminClient. |
| Commons IO / Build-time через Shadow | 2.11.0 | CVE-2024-47554, XmlStreamReader DoS | **2.14.0** для CVE; обновить через Shadow | 7.5 CVSS3.1 / 8.7 CVSS4. Нужен обработчик недоверенного XML в build process. |
| Plexus Utils / Build-time через Shadow | 3.5.1 | CVE-2025-67030, Zip Slip | **3.6.1 / 4.0.3** | Числовой CVSS не опубликован в проверенных записях. Вызов уязвимого extractFile в текущей сборке не доказан. |
| ASM / Build-time через Shadow | 9.4 | В release notes 9.11 перечислены security hardening/fixes parser/recursion/resource limits, без CVE для этих пунктов | Актуальная **9.11.1**, согласовать все ASM modules через поддерживаемый plugin graph | ASM9.4 ориентирован на bytecode до Java20; Java21 появился в9.5. Текущий обычный shadowJar проходит, но relocation/minimize/bytecode analysis на Java21 отдельно не проверены. |
| Log4j API/core / Build-time через Shadow | 2.20.0 | CVE-2025-68161, 2026-34477 — TLS checks; 34479/34480/49844 — XML/JSON log integrity | **2.25.5 / 2.26.1** закрывают перечисленные записи, через plugin upgrade | CVSS4 6.3–6.9. Не runtime приложения; требуемые appenders/layouts не настроены. Log4j1 bridge отсутствует. |
| Commons Compress / Gradle distribution | 1.25.0 | CVE-2024-26308 OOM Pack200; 2024-25710 DUMP loop | **1.26.0** для этих CVE | Только build; форматы Pack200/DUMP в проекте не используются явно. |
| Apache MINA SSHD / Gradle distribution | 2.10.0 | CVE-2024-41909, integrity check bypass | **2.12.0** для CVE | Только build; SSH/SFTP repositories не настроены. |
| jsoup / Gradle distribution | 1.15.3 | CVE-2026-75140 XML namespace OOM; 2026-71497 Cleaner markup | **1.23.2** закрывает обе | Build-time; соответствующий XML/HTML sanitization path не установлен. |
| Commons Lang / Gradle distribution | 2.6 | CVE-2025-48924, ClassUtils uncontrolled recursion | Исправления в legacy artifact нет; **commons-lang3 >=3.18.0** в поддерживаемом Gradle | Нужен API с недоверенным class name. Обновлять Gradle целиком; не заменять lib вручную. |
| eddsa / Gradle distribution | 0.3.0 | CVE-2020-36843, Ed25519 signature malleability | Исправленного релиза net.i2p.crypto:eddsa в записи нет | Build-time SSH path; не относится автоматически к JDK TLS/Kerberos. |
| JGit / Gradle distribution | 5.13.3.202401111512-r | CVE-2025-4949, XXE | **5.13.4.202507202350-r**, исправления других веток в приложении | CVSS4 CNA 6.8; нужен недоверенный XML/JGit путь. |
| Guava / Gradle distribution | 32.1.2-jre | CVE-2026-102554, collection deserialization OOM | **33.7.2** для CVE | CVSS4 CNA 8.2. Учитывать вместе с Gradle daemon deserialization до auth. Shaded Guava FreeBuilder отдельно не идентифицирована. |
| Bouncy Castle bcprov-jdk15on / Gradle distribution | 1.68 | CVE-2023-33201/33202, 2024-29857/30171/34447: LDAP/PEM/certificate/TLS | Исправления 1.73 / 1.74 / 1.78 **в поддерживаемых artifacts**, legacy jdk15on заканчивается 1.70 | Build-time; использование конкретных BC API/provider не доказано. Простого bump jdk15on до 1.78 не существует. |
| Bouncy Castle bcpg-jdk15on / Gradle distribution | 1.68 | OSV совпадение CVE-2026-3505 | **Не считать подтверждённой для 1.68**: диапазон производителя начинается с 1.74 | Расхождение scanner metadata с CNA/производителем. Не включать в подтверждённый vulnerability count. |

Оценка Snappy: [CVE-2026-93451](https://github.com/advisories/GHSA-gwq9-9ffj-mvrv), [108106](https://github.com/advisories/GHSA-q233-g92q-qxp6), [116590](https://github.com/advisories/GHSA-xqrx-qh46-34m7). LZ4: [advisories fork](https://github.com/yawkat/lz4-java/security/advisories), [drop-in replacement](https://lz4-java.yawk.at/v1.11.4/). BC-PGP: [объяснение производителя CVE-2026-3505](https://github.com/bcgit/bc-java/wiki/CVE%E2%80%902026%E2%80%903505).

SnakeYAML 2.2 не попадает в старый диапазон CVE-2022-1471 до 2.0; такой флаг для текущего дерева был бы ошибочным. Актуальная стабильная версия 2.7; предпочтительно брать согласованную версию через обновлённый Jackson YAML и проверять эффективное дерево. Отдельных совпадений для snakeyaml 2.2, jackson-annotations 2.17.1 и остальных test-only artifacts в выполненных OSV запросах нет. Это ограниченный отрицательный результат, а не сертификация безопасности.

Дополнительный источник для ASM: [официальные версии и security fixes](https://asm.ow2.io/versions.html). Обновление Shadow до выбранного релиза не доказывает автоматически, что его дерево включает ASM9.11.1: новое effective дерево и bytecode paths нужно проверить отдельно, не присваивая несуществующий CVE номер.

### Дополнительно найденные записи, отсутствовавшие в ответах OSV

| Идентификатор / компонент | Затронутые версии | CVSS | Исправление | Воздействие и применимость |
| --- | --- | --- | --- | --- |
| [CVE-2026-13006, logback-core](https://www.cve.org/CVERecord?id=CVE-2026-13006) | 0.9.20–1.5.36 | **7.0 v4.0, CNA** | **1.5.37** | Неполное исправление Janino/config code execution. Нужен контроль logging configuration, штатный JAR Janino не содержит. |
| [CVE-2026-19880, logback-classic](https://www.cve.org/CVERecord?id=CVE-2026-19880) | 0.9.14–1.6.2 | **6.3 v4.0, CNA** | **1.6.3** | MDC/SiftingAppender могут использовать недоверенные значения в путях. Штатного соответствующего appender/MDC path не найдено. |
| [CVE-2026-104721, logback-classic](https://www.cve.org/CVERecord?id=CVE-2026-104721) | 0.9.14–1.6.4 | **6.3 v4.0, CNA** | **1.6.5** | Неполное исправление предыдущей проблемы; обосновывает цель 1.6.6 вместо остановки на 1.5.38. |
| [CVE-2026-111551, kafka-clients](https://nvd.nist.gov/vuln/detail/CVE-2026-111551) | 0.10.0.0–4.1.2; 4.2.0–4.2.1; 4.3.0–4.3.1 | **7.5 v3.1, NVD/CISA** | **4.2.2 / 4.3.2 / 4.4.0**; 3.x fix не опубликован | ListDeserializer/Streams — неконтролируемая аллокация. Текущий AdminClient не вызывает ListDeserializer и не использует kafka-streams. Версия 3.9.2 остаётся version-affected. |
| [GHSA-48mj-8f7c-7qfm, LZ4](https://github.com/yawkat/lz4-java/security/advisories/GHSA-48mj-8f7c-7qfm) | org.lz4 <=1.8.1; fork <=1.12.0 | Upstream **Low**, числовой CVSS не опубликован | Fork **1.12.1**, цель 1.14.0 | Header-only input может вызвать большие аллокации до проверки payload. Текущий AdminClient record stream не декодирует. CVE не назначен — не придумывать номер. |

Для zstd-jni диапазоны ниже взяты из CNA, а не только из поля «tested vulnerable version» GitHub repository advisory. Несколько repository записей указывают лишь 1.5.7-13, тогда как CNA подтверждает более ранние диапазоны, включающие текущую **1.5.5-6**. Оценки CNA и автора advisory также различаются из-за модели атаки; это сохранено явно.

| CVE / zstd-jni | Затронутые версии | CVSS CNA | Исправление | Воздействие / применимость |
| --- | --- | --- | --- | --- |
| [CVE-2026-87823](https://www.cve.org/CVERecord?id=CVE-2026-87823) | >=1.1.1, <1.5.7-14 | **8.2 v3.1 / 8.8 v4.0** | 1.5.7-14 | Direct ByteBuffer offset OOB read/crash. Нужен вызов конкретного JNI API; автор GHSA оценивает локальную модель в 5.7 v3.1. |
| [CVE-2026-87795](https://www.cve.org/CVERecord?id=CVE-2026-87795) | >=1.2.0, <1.5.7-14 | **8.2 v3.1 / 8.8 v4.0** | 1.5.7-14 | Ошибка диапазонов ZstdDictCompress; AdminClient словари не создаёт. |
| [CVE-2026-90560](https://www.cve.org/CVERecord?id=CVE-2026-90560) | >=1.2.0, <1.5.7-14 | **8.2 v3.1 / 8.8 v4.0** | 1.5.7-14 | Ошибка диапазонов ZstdDictDecompress; API не найден в приложении. |
| [CVE-2026-89045](https://www.cve.org/CVERecord?id=CVE-2026-89045) | >=1.4.8-4, <1.5.7-14 | **4.0 v3.1 / 5.1 v4.0** | 1.5.7-14 | Negative length вызывает цикл в InputStream wrapper. Требует вызова с некорректными аргументами. |
| [CVE-2026-89046](https://www.cve.org/CVERecord?id=CVE-2026-89046) | >=1.5.5-6, <1.5.7-14 | **8.2 v3.1 / 8.8 v4.0** | 1.5.7-14 | Negative offset в getFrameContentSize; у автора GHSA 5.7 v3.1. Совпадение с baseline включительно. |
| [CVE-2026-87824](https://www.cve.org/CVERecord?id=CVE-2026-87824) | >=1.3.3-1, <1.5.7-14 | **7.5 v3.1 / 8.7 v4.0** | 1.5.7-14 | Неконтролируемая аллокация при training samples; training приложением не выполняется. |
| [CVE-2026-87877](https://www.cve.org/CVERecord?id=CVE-2026-87877) | >=1.3.8-4, <1.5.7-14 | **7.7 v3.1 / 7.0 v4.0** | 1.5.7-14 | Use-after-free при вызове setters после close; жизненный цикл JNI context не используется приложением. |
| [CVE-2026-87825](https://www.cve.org/CVERecord?id=CVE-2026-87825) | >=1.3.8-4, <1.5.7-14 | **7.7 v3.1 / 7.0 v4.0** | 1.5.7-14 | Use-after-free dictionary objects; соответствующий API не найден. |

Следующие более свежие repository GHSA не имеют назначенного CVE на дату проверки:

| GHSA / zstd-jni | Диапазон / исправление | CVSS автора | Оценка для текущего 1.5.5-6 |
| --- | --- | --- | --- |
| [GHSA-8hfr-vm4w-9gfg](https://github.com/luben/zstd-jni/security/advisories/GHSA-8hfr-vm4w-9gfg) | <=1.5.7-25; **fix 1.5.7-26** | **5.5 v3.1** | FASTCOVER dictionary training integer underflow; API не используется. Опубликовано 08.11.2026: цель 1.5.7-14 уже недостаточна для всех записей. |
| [GHSA-h5m7-4hv9-cvp3](https://github.com/luben/zstd-jni/security/advisories/GHSA-h5m7-4hv9-cvp3) | <=1.5.7-24; **fix 1.5.7-25** | **8.1 v3.1** | Multi-dictionary UAF. JNI вариант требует опасного lifecycle; FFM-вариант Java 22+ к текущей Java 21 неприменим. |
| [GHSA-5p5w-hg9r-gx2p](https://github.com/luben/zstd-jni/security/advisories/GHSA-5p5w-hg9r-gx2p) | <1.5.7-25; **fix 1.5.7-25** | **7.4 v3.1** | JNI training TOCTOU; нет dictionary training в приложении. |
| [GHSA-8w96-r9r5-4j3r](https://github.com/luben/zstd-jni/security/advisories/GHSA-8w96-r9r5-4j3r) | <=1.5.7-16; **fix 1.5.7-17** | **6.8 v3.1** | Deprecated decompressedSize offset OOB; вызов API не найден. |
| [GHSA-8v5f-66rc-52f5](https://github.com/luben/zstd-jni/security/advisories/GHSA-8v5f-66rc-52f5) | >=1.5.7-12, <1.5.7-25; fix 1.5.7-25 | **5.1 v3.1** | **Baseline не затронут**, но важно не попасть в уязвимую промежуточную версию. |
| [GHSA-5p47-q5r2-3396](https://github.com/luben/zstd-jni/security/advisories/GHSA-5p47-q5r2-3396) | Только 1.5.7-15; fix 1.5.7-16 | Числового CVSS нет | **Baseline не затронут**; forged frame size regression. |

Для CVE-2026-87823 дополнительно проверена [NVD](https://nvd.nist.gov/vuln/detail/CVE-2026-87823). Все исходные CNA/GHSA сохранены локально: `additional-cve-records.json`, `final-supplement-cve-records.json`, `zstd-first-party-advisories.json`. Запрос OSV для выбранных новых targets вернул пустые списки, но **это не доказательство безопасности**: Kafka 3.9.2 всё ещё затронут CVE-2026-111551, а новые zstd GHSA отсутствовали в OSV.

### Kafka: применимость отдельных advisories

| Запись | Диапазон / CVSS | Исправление | Оценка для Kafka GitOps |
| --- | --- | --- | --- |
| [CVE-2024-31141](https://github.com/advisories/GHSA-2x2g-32r7-p4x8) | Kafka Client >=2.3.0, <3.7.1; **6.5 v3.1 / 6.8 v4.0** | 3.7.1 | ConfigProvider file read требует контроля connection properties и провайдеров. Не считать произвольный topic config автоматически исполнением ConfigProvider. |
| [CVE-2025-27817](https://github.com/advisories/GHSA-vgq5-3255-v292) | >=3.1.0, <3.9.1; **7.5 v3.1 / 6.2 v4.0** | 3.9.1 | OAuth endpoint/file access. Для GSSAPI самого по себе не применимо. При OAuth нужен явный allowlist URL: в исправленном 3.x default разрешает все URL, в 4.x default строже. |
| [CVE-2026-33558](https://github.com/advisories/GHSA-wf66-mphr-4c4r) | >=0.11.0, <3.9.2; 4.0.0; **5.3 v3.1** | 3.9.2 / 4.0.1; 4.1+ исправлены | NetworkClient DEBUG может раскрывать secrets. AdminClient использует этот компонент. Штатный verbose приложения ставит Kafka logger WARN; внешний DEBUG override возвращает риск. Безопасное логирование задачи №1 не исправляет чужой код. |
| [CVE-2026-35554](https://github.com/advisories/GHSA-5qcv-4rpc-jp93) | >=2.8.0, <3.9.2; 4.0.0–4.0.1; 4.1.0–4.1.1; **8.7 v3.1** | 3.9.2 / 4.0.2 / 4.1.2 | Producer buffer pool data misrouting; Producer здесь не создаётся. Не считать подтверждённой утечкой через AdminClient. |
| [CVE-2026-111551](https://www.cve.org/CVERecord?id=CVE-2026-111551) | <=4.1.2 и отдельные 4.2/4.3 patches, см. выше; **7.5 v3.1 NVD** | 4.2.2 / 4.3.2 / 4.4.0 | ListDeserializer не вызывается. Обоснованное временное исключение для 3.9.2 возможно только с документированием и повторной оценкой при появлении Consumer/Streams. |

**Исключённые автоматические флаги:** CVE-2026-114349 относится по CNA к broker artifact `kafka_2.xx`, не к найденному kafka-clients; CVE-2024-56128 исправляет SCRAM nonce validation на брокере, поэтому обновление клиента не исправляет уязвимый ADS broker. LDAP/JNDI advisories Kafka Connect нельзя автоматически считать RCE текущего AdminClient. При инвентаризации ADS их следует проверить отдельно. [Apache Kafka security advisories](https://kafka.apache.org/community/cve-list/), [CVE-2026-114349](https://www.cve.org/CVERecord?id=CVE-2026-114349).

### Gradle core и Shadow: отдельные build-time риски

| Advisory | Затронутые версии | CVSS производителя | Исправление | Применимость |
| --- | --- | --- | --- | --- |
| [GHSA-j5m7-59rp-24f5](https://github.com/gradle/gradle/security/advisories/GHSA-j5m7-59rp-24f5) | <=8.14.5; 9.0–9.8.0 | **8.6 v4.0** | **8.14.6 / 9.8.1** | SSL failure repository fallback может разрешить подмену зависимости. Один mavenCentral в проекте уменьшает сценарий fallback, но settings/plugin resolution и CI нужно проверять отдельно. |
| [GHSA-xwqc-3h47-hg64](https://github.com/gradle/gradle/security/advisories/GHSA-xwqc-3h47-hg64) | <=8.14.5; 9.0–9.8.0 | **8.6 v4.0** | **8.14.6 / 9.8.1** | Gradle client десериализует daemon response до аутентификации; локальный/shared-host build risk. --no-daemon может создавать single-use daemon и не является полным исправлением. |
| [GHSA-mvvg-497x-hmj8](https://github.com/gradle/gradle/security/advisories/GHSA-mvvg-497x-hmj8) | <=8.14.5; 9.0–9.8.0 | **7.3 v4.0** | **8.14.6 / 9.8.1** | Worker daemon unauthenticated deserialization, локальная гонка во время сборки. Не runtime Kafka GitOps. |
| [CVE-2026-22865 / GHSA-mqwm-5m85-gmcv](https://github.com/gradle/gradle/security/advisories/GHSA-mqwm-5m85-gmcv) | <8.14.4; 9.0–9.2.1 | **8.6 v4.0** | **8.14.4 / 9.3.0** | Repository fallback при transient errors; supply-chain риск сборки. |
| [CVE-2026-22816 / GHSA-w78c-w6vf-rw82](https://github.com/gradle/gradle/security/advisories/GHSA-w78c-w6vf-rw82) | <8.14.4; 9.0–9.2.1 | **8.6 v4.0** | **8.14.4 / 9.3.0** | Repository fallback при unknown host; тот же класс риска. |
| [CVE-2026-75598 / GHSA-rr8v-fxxf-c986](https://github.com/GradleUp/shadow/security/advisories/GHSA-rr8v-fxxf-c986) | Shadow family **<9.6.0** по CNA; GitHub package — новый plugin ID | **6.3 v3.1** | **9.6.0**, цель **9.6.1** | Zip Slip entries при ShadowCopyAction. Для старого ID 8.1.1 требуется сверка family/code, а не только Maven coordinate match. Gradle 8.7 уже содержит sanitization, уменьшающую прямой build exploit; downstream небезопасная распаковка остаётся условием риска. Промежуточный Shadow 8.3/9.2 не считать полным исправлением. |

У первых трёх Gradle GHSA CVE на дату проверки не назначены. CVE-2025-27148 / [GHSA-465q-w4mf-4f4r](https://github.com/gradle/gradle/security/advisories/GHSA-465q-w4mf-4f4r) затрагивает **только Gradle 8.12**, не обнаруженный 8.7; в vulnerability count текущего проекта его не включать.

## В. Gradle, плагины и Java 21

| Компонент | Сейчас | Актуальная стабильная / цель | Совместимость и действие |
| --- | --- | --- | --- |
| Gradle Wrapper/distribution | **8.7** | **9.9.0**; промежуточно **8.14.6** | Java 21 для запуска поддерживается с Gradle 8.5. 8.14.6 закрывает перечисленные core GHSA, но не делает автоматически безопасными все встроенные библиотеки/старый Shadow. 9.x требует предварительной миграции conventions/plugins. |
| java, groovy, application, idea, jacoco | Core Gradle 8.7 | Версия вместе с Gradle | Самостоятельных version bumps нет. Application convention заменить modern extension; проверить test engine/IDE/generated sources. |
| org.inferred.processors | **3.7.0**, последняя доступная | **Удалить plugin**, сохранить FreeBuilder dependencies | Использует удаляемые в Gradle 9 Convention/JavaPluginConvention. Native annotationProcessor уже присутствует и поддерживается Gradle; plugin не нужен для базовой компиляции. IDE source roots проверить отдельно. |
| com.github.johnrengelman.shadow | **8.1.1**, последний старый ID | Новый **com.gradleup.shadow:9.6.1** | Ветка 8.3 требует Gradle>=8.3; 9.2 — >=8.11/Java17; 9.3 — >=9.0; 9.5+ — >=9.2/Java17. Для цели 9.6.1 подходит Gradle9.9 + Java21. Миграция ID/API и packaging tests обязательны. |
| org.sonarqube | **5.0.0.4638** | **7.5.0.8588**; последняя 5.x **5.1.0.4882** не конечная цель | Новый plugin поддерживает Gradle9. Проверить требования установленного SonarQube/Cloud, Java scanner и JRE auto-provisioning policy. Не запускать внешнюю analysis без отдельной задачи. |
| com.gradle.enterprise | **3.17.2** | Старый ID максимум **3.19.2**, deprecated; целевой **com.gradle.develocity:4.6.1** | Требуется миграция settings DSL и контроль build scan publishing. CI publishAlways сейчас может отправлять build metadata наружу; сделать явным opt-in согласно политике организации. |
| org.gradle.toolchains.foojay-resolver-convention | **0.8.0** | **1.0.0** | Требует Java17+; совместим с Gradle9, устраняет обращение к удалённым JVM vendor constants. Роль — разрешение JDK, не runtime dependency. |
| JaCoCo tool | По умолчанию **0.8.11** | **0.8.16** | 0.8.11 уже поддерживает Java21. Обновление рекомендуется отдельно от runtime; agent/Ant/report согласовать. jacocoAnt transitive inventory не завершена offline. |

Источники версий и требований: [Gradle releases](https://gradle.org/releases/), [Java/Gradle compatibility](https://docs.gradle.org/9.9.0/userguide/compatibility.html), [Shadow compatibility matrix](https://github.com/GradleUp/shadow/blob/main/README.md), [Shadow Portal](https://plugins.gradle.org/plugin/com.gradleup.shadow), [Sonar Portal](https://plugins.gradle.org/plugin/org.sonarqube), [Sonar scanner requirements](https://docs.sonarsource.com/sonarqube-server/analyzing-source-code/scanners/sonarscanner-for-gradle/), [Develocity Portal](https://plugins.gradle.org/plugin/com.gradle.develocity), [Enterprise Portal](https://plugins.gradle.org/plugin/com.gradle.enterprise), [Foojay Portal](https://plugins.gradle.org/plugin/org.gradle.toolchains.foojay-resolver-convention), [processors Portal](https://plugins.gradle.org/plugin/org.inferred.processors), [FreeBuilder Gradle setup](https://github.com/inferred/FreeBuilder), [JaCoCo changes](https://www.jacoco.org/jacoco/trunk/doc/changes.html).

**Java 21 следует сохранить.** Это LTS и подходящая цель для ADS CLI. Текущий `build.gradle` задаёт `languageVersion=21` и **vendor=AZUL**, поэтому локальный Temurin не удовлетворяет toolchain: Gradle выбирает cached Zulu либо пытается скачать Azul через Foojay. Это мешает воспроизводимости Windows/CI/offline и создаёт скрытое отличие от заявленной среды. В задаче №4 следует либо убрать vendor constraint и документировать допустимый JDK21, либо осознанно задать принятого поставщика и provisioning policy. Версию JVM, запускающей Gradle, и toolchain компиляции необходимо проверять раздельно.

**Подтверждённые причины deprecation warnings:**

1. `org.inferred.gradle.ProcessorsPlugin.configureJavaCompilerTasks`, stack trace lines 81/84 и связанные closures обращаются к `Convention` и `JavaPluginConvention`. Они удаляются в Gradle9. Решение — убрать plugin, оставить native processor configurations.
2. `build.gradle:14`, `mainClassName = ...`, обращается к deprecated application convention. Решение — `application { mainClass = 'com.devshawn.kafka.gitops.MainCommand' }` в следующей задаче.
3. `settings.gradle`: старый `com.gradle.enterprise` ID сообщает о переименовании в Develocity. Решение — обновление ID и DSL с проверкой scan policy.

Использование `afterEvaluate`, старого `task ...` syntax и `${buildDir}` можно модернизировать через providers, `layout.buildDirectory`, `tasks.register/named` при подготовке Gradle9, но **не следует объявлять каждое из них отдельным доказанным источником текущих warnings**. Проверять результат `help --warning-mode all`, сборкой JAR и явно выбранными unit tasks. Обычный `build` и `jacocoTestReport` в этом проекте могут запустить нежелательные тесты.

В Wrapper properties отсутствует `distributionSha256Sum`; `validateDistributionUrl=true` не проверяет SHA-256 содержимого ZIP. Не обнаружены dependency locks/verification metadata. В следующем этапе добавить проверку Wrapper distribution и проверенную dependency verification/locking policy, включая plugins, без слепого доверия автоматически принятым checksum. Это снижает supply-chain риск и не заменяет обновление Gradle.

Dockerfile использует **eclipse-temurin:21.0.3_9-jre-jammy**, существенно старее локального 21.0.13. Задача №4 должна отдельно обновить JRE21 security patch/base image с фиксацией digest и OS scan. Образ в аудите не загружался и не сканировался, список контейнерных CVE не установлен.

## Г. Kafka Client и Arenadata Streaming

**Границы совместимости.** Kafka Java Client согласует версии протокола с брокером, но наличие метода в Java API не означает его поддержку брокером. Текущий `KafkaService.updateTopicConfig()` уже использует `incrementalAlterConfigs`, которому нужен broker **2.3+**. Для будущих задач `createPartitions()` требует broker **1.0+**, `alterPartitionReassignments()` и `listPartitionReassignments()` — **2.4+**. Нужны также ACL, поддержка API в конкретной ADS сборке и учёт rolling upgrade. [Admin API 3.9](https://kafka.apache.org/39/javadoc/org/apache/kafka/clients/admin/Admin.html), [KIP-455: Admin reassignment](https://cwiki.apache.org/confluence/display/KAFKA/KIP-455%3A+Create+an+Administrative+API+for+Replica+Reassignment).

| Возможность | Ограничение | Проверка до восстановления функциональности |
| --- | --- | --- |
| createPartitions | Только увеличение, уменьшение не поддерживается; меняется распределение key→partition | Idempotency, конкурентное изменение metadata, отсутствие уменьшения, ошибки authorization/timeouts, эффекты для порядка сообщений |
| alterPartitionReassignments | Меняет assignment существующих реплик; требует подходящих broker IDs и достаточного числа брокеров | Детерминированный assignment, broker/rack awareness, ISR, throttling, частичные ошибки, retries/cancellation и конфликты существующего reassignment |
| listPartitionReassignments | Показывает текущие перемещения; операция асинхронная | Не считать успешное принятие запроса завершением; polling с timeout, затем describeTopics/ISR, восстановление после прерывания |
| incrementalAlterConfigs | Уже используется; broker>=2.3 | Сохранить SET/DELETE semantics, null и sensitive handling; не менять topic semantics вместе с dependency bump |
| SSL | JSSE/JDK trust/key stores, TLS versions/cipher/SAN | JKS/PKCS12, сертификаты/SAN, TLS1.2/1.3, mTLS и безопасные diagnostics без password/JAAS |
| SASL/GSSAPI | Поддерживается 3.x/4.x; зависит от JDK/KDC и окружения | serviceName, principal/realm, DNS/SPN, ticket cache/keytab и renew, KDC algorithms, права файлов. Не включать устаревшие криптоалгоритмы ради совместимости без отдельного решения |
| PLAIN/SCRAM/OAUTHBEARER | Callback/login modules и defaults могут отличаться | SASL_SSL, callbacks, OAuth URL allowlist, отсутствие DEBUG секретов и конфиденциальных callback properties |

`AlterPartitionReassignmentsResult.all()` означает, что переназначения **инициированы**, а не завершены. [Javadoc](https://kafka.apache.org/39/javadoc/org/apache/kafka/clients/admin/AlterPartitionReassignmentsResult.html). Это следует заложить в будущую реализацию изменения replication factor, а не переносить PR #52 целиком вместе с обновлением клиента.

**Переход Client 3.x→4.x:**

- Java Client 4.x требует Java11+, поэтому Java21 достаточна; это не означает, что сервер ADS должен использовать тот же JDK.
- Минимальная версия брокера для Java Client4 — **2.1**. Если ADS старее, 4.x исключается. В конкретных Admin operations действуют более высокие требования API.
- Удаление ZooKeeper из Kafka server4 не означает обязательный KRaft для Client4: клиент работает по broker protocol. ZK-based broker>=2.1 теоретически совместим по протоколу; поддержка в ADS должна быть подтверждена отдельно.
- Удалены deprecated Java APIs и изменены отдельные defaults/security constraints. Нужны компиляция против выбранной версии, contract tests Admin calls и auth tests. Код из старого PR может ссылаться на уже удалённые методы — перенос требует проверки.
- Kafka server4.4 и client4.4 — разные решения об обновлении; нельзя обновлять кластер как побочный эффект замены библиотеки CLI.

Источники: [Kafka4.4 upgrade guide](https://kafka.apache.org/44/getting-started/upgrade/), [compatibility](https://kafka.apache.org/44/getting-started/compatibility/). Документация ADS показывает несколько существенно разных линий поставки, поэтому версия «ADS» без build number недостаточна: [ADS release notes](https://docs.arenadata.io/en/ADStreaming/current/release-notes/release-notes.html), [ADS documentation](https://docs.arenadata.io/en/landing-ads/index.html).

**Что получить до выбора Kafka Client:**

1. Точные ADS edition/version/build, версия kafka на каждом broker/controller, vendor patches/backports и официальная поддерживаемая матрица клиентов Arenadata.
2. Наличие rolling/mixed-version cluster, `inter.broker.protocol.version` для старых линий либо `metadata.version`, KRaft/ZooKeeper, поддерживаемые Admin API и ограничения дистрибутива.
3. Активные SSL/SASL mechanisms, callback/login modules и их версии; требования Kerberos/KDC, trust/key store formats, сертификаты/TLS и политика JDK. Нужны названия/версии и требования, **не секретные значения**.
4. Механизм авторизации: Kafka ACL/Ranger и точные права для describe/config/partition/reassignment operations, service principal, naming restrictions.
5. Число брокеров, rack labels, требования RF/min.insync.replicas/ISR, ограничения переназначения, throttles, ongoing reassignments, допустимые maintenance windows.
6. Целевые OS/architecture и JVM CLI/контейнера, политика native library temp directory, доверие к connection configuration и возможность внешних logging overrides.

До этих сведений можно подготовить runtime/parser/logging/tooling updates, но **нельзя объявить 4.4.0 безопасной для данного ADS**. Для 3.9.2 также нужны supported matrix и auth/API smoke tests. Если выбран временный 3.x путь, остаточную ListDeserializer CVE нужно записать как «не используется текущим Admin-only CLI», с условием пересмотра при расширении функциональности.

**Важный транзитивный эффект:** опубликованный kafka-clients **3.9.2 POM** всё ещё задаёт zstd-jni **1.5.6-4**, at.yawk.lz4:lz4-java **1.10.1**, snappy-java **1.1.10.5**. Поэтому один bump Kafka не исправит перечисленные compression advisories. Нужны явные согласованные constraints/исключения и повторное дерево; отсутствие старого org.lz4 и дублирующих `net.jpountz` классов проверяется по JAR. [POM 3.9.2](https://repo.maven.apache.org/maven2/org/apache/kafka/kafka-clients/3.9.2/kafka-clients-3.9.2.pom).

## Д. План обновления для задачи №4

Каждый этап — отдельный проверяемый набор изменений в будущей задаче. Ни один из них сейчас не применён. Первыми следует убрать риски среды сборки, затем обновлять runtime, затем тестовую инфраструктуру; функциональность партиций/RF восстанавливать после стабилизации зависимостей.

### Обязательные по безопасности

1. **Согласовать ADS compatibility gate.** Получить сведения из раздела Г, зафиксировать текущие JSON-планы, CLI flags и unit-test baseline. Разделить source/test/build/runtime SBOM; уточнить shaded FreeBuilder/cglib и jacocoAnt gap. Определить поддерживаемые JVM/OS для JNI.
2. **Подготовить Gradle9 migration:** убрать processors plugin с сохранением annotationProcessor и IDE generation, заменить mainClassName convention, мигрировать settings plugins. Добавить checksum Wrapper и контролируемую dependency verification/locking. В CI сделать build scan публикацию явным opt-in. Сначала перейти на **8.14.6** при необходимости короткого моста, затем на **9.9.0 + Shadow9.6.1 + Foojay1.0.0**. Develocity4.6.1 и Sonar7.5 согласовать с инфраструктурой. 8.14.6 — временный этап, не основание объявить старый Shadow полностью исправленным.
3. **Jackson:** BOM **2.21.8**, согласовать все модули и фактически выбранный SnakeYAML. Проверить старые/new JSON plans, previousValue, sensitive/null, строгий отказ для неизвестных операций/структур, YAML service types, malformed/oversized input. Гарантии задач №1/№2 должны сохраниться.
4. **Логирование:** SLF4J **2.0.20**, Logback classic/core **1.6.6**. Проверить обычный/verbose режим, JAAS PLAIN/SCRAM/GSSAPI, SSL passwords и OAuth secrets, exception messages; один provider, service metadata и custom logging XML. Если есть Janino-based deployment config, заранее мигрировать его. Не ослаблять безопасное логирование задачи №1.
5. **Compression:** согласовать **at.yawk.lz4:lz4-java1.14.0**, **zstd-jni1.5.7-26**, **snappy-java1.1.10.12** с выбранным Kafka Client. Удалить duplicate old LZ4 artifact через корректное module replacement/exclusion, проверить classpath и license/service/native resources. Изолированные Windows/Linux native smoke tests и roundtrip known payloads, включая zero/large/malformed buffers, без реального Kafka. Не применять blind minimize/relocation к SASL/reflection/native классовым именам.
6. **Kafka:** после ADS gate выбрать **3.9.2 как ограниченный мост** или **4.4.0 после подтверждения совместимости**. Тесты с fake/mock Admin и компиляция обязательны; auth/protocol integration — только отдельно разрешённый изолированный ADS-compatible test cluster. Зафиксировать временные CVE exceptions с условиями и сроком пересмотра. Никаких проверок на production в рамках dependency task.
7. **Runtime environment:** обновить старый Docker JRE21 base до поддерживаемого security patch, зафиксировать digest, выполнить container/OS scan отдельно. Не считать обновление Maven библиотек исправлением JRE/OS.

Для этих этапов риск совместимости средний/высокий: Gradle/plugin major, parser constraints, Logback XML и Kafka auth/protocol. Минимальный gate после каждого изменения — новое dependency tree/SCA, Java21 compilation, явно ограниченные unit tests, упаковка fat JAR и CLI `--help` без cluster configuration. По отдельному разрешению — isolated auth/Admin integration. Не запускать общий `test/build` без исключения реальных integration suites.

### Рекомендуемые

8. **Тестовый стек согласованным набором:** Groovy **4.0.33**, Spock core/junit4 **2.5-groovy-4.0**, JUnit BOM **5.14.4**, JaCoCo **0.8.16**, JSONassert **1.5.3**. Проверить discovery/count тестов, Rules/env cleanup, engine alignment, Hamcrest conflicts и отчёт coverage. Не удалять Vintage или spock-junit4 только из-за отсутствия прямых JUnit4 @Test: сначала проверить реальное назначение каждого компонента.
9. **Picocli4.7.7**, проверка `--skip-topics`, `--skip-acls`, `--no-delete`, include/exclude prefixes, exit codes/help и совместимости command-line arguments.
10. Убрать неиспользуемый cglib после доказательства отсутствия нужных mock paths; заменить system-rules и reflection env modification поддерживаемым решением/инъекцией окружения. Сократить --add-opens после прохождения тестов. Риск — незаметная потеря тестового покрытия и cleanup, поэтому считать это отдельной миграцией.
11. Проверить fat JAR manifest/Multi-Release/services и shell packaging на Windows/Linux, license notices, воспроизводимость. Повторить аудит shaded/build libraries и residual vulnerabilities нового Gradle distribution; результаты старого дерева не переносятся автоматически на новое.

### Необязательные major migrations

12. **Jackson3.3 / JUnit6.1.3 / Groovy6** — отдельные задачи после стабилизации. Они не нужны для первого закрытия известных CVE и повышают объём миграции package/API/test engine. Kafka4.x также не следует выбирать ради «самой новой версии» без ADS gate, хотя при подходящих брокерах это может быть нужная security цель.

**После dependency baseline:** восстанавливать увеличение партиций и RF отдельными изменениями. Для replica reassignment тестировать принятие и завершение раздельно, partial failures/timeout/cancel/retry, rack awareness и ISR; для partitions — увеличение/конкурентность/запрет уменьшения. Не менять план JSON и применение topic configs одновременно с обновлением dependency stack. Регрессии флагов joschi0.3.1 и безопасного логирования обязательны на каждом затрагивающем их этапе.

## Е. Что нельзя безопасно обновить без дополнительных сведений

| Компонент / решение | Чего не хватает |
| --- | --- |
| kafka-clients4.x и окончательная 3.x цель | Версия/патчи ADS, все broker versions, API/auth/Ranger support matrix, rolling state |
| Утверждение «3.9.2 полностью безопасен» | Отсутствует исправление ListDeserializer CVE в 3.x; нужна обоснованная ограниченная exception либо поддерживаемый исправленный client |
| Sonar plugin/server integration и Develocity publishing | Версия/политика сервера, разрешённая JRE provisioning, требования CI privacy; новый plugin version сам по себе не даёт разрешения отправлять исходники/metadata |
| Миграция Logback deployment configurations | Существуют ли внешние XML, Janino evaluators, MDC/SiftingAppender, socket receivers и DEBUG overrides |
| Обновление compression/native providers | Целевые OS/architecture, temp policy, callback/packaging usage и compatibility tests выбранного Kafka Client |
| FreeBuilder/shaded dependencies | Точные embedded versions, generated-source/IDE требования; версия artifact2.8.0 не даёт полного SBOM |
| Удаление cglib/system-rules/Vintage/adapters | Фактические engine/mock/Rule usages и coverage; Java21 reflection constraints |
| Jackson3, Groovy5/6, JUnit6 | Совместимость моделей/аннотаций/Spock и migration scope; эти major upgrades пока необязательны |
| Требование остаться на Gradle8.x | Допустима ли Gradle9 migration; Shadow<9.6 не считать полностью исправленным без отдельного backport/mitigation анализа |

## Ж. Источники, артефакты и оставшиеся ограничения

Источники актуальности: публичные Maven metadata и Gradle Plugin Portal, официальные release/security notes; дата выборки **09.11.2026**. Источники уязвимостей: [OSV API](https://google.github.io/osv.dev/api/), [GitHub Security Advisories](https://github.com/advisories), [NVD](https://nvd.nist.gov/), записи CVE/CNA и ссылки на advisories рядом с каждой записью. Для версий используются первичные репозитории производителей; для конфликтов диапазонов приоритет имеют подтверждённые данные CNA/производителя с явным сохранением расхождения.

Артефакты для повторной проверки:

- [Полная инвентаризация по конфигурациям](dependency-inventory.md), [CSV координат/областей/OSV matches](dependency-inventory.csv), [Gradle distribution JAR list](gradle-distribution-jars.csv).
- [Полный OSV реестр CVE/GHSA, 59 записей](vulnerabilities-osv.md), [CSV с диапазонами, CVSS/vector, применимостью](vulnerabilities-osv.csv).
- [Сырые разрешённые конфигурации](resolved-inventory.json), [первичный OSV batch](osv-batch-response.json), [44 полных GitHub records](github-advisories.json), [новые Gradle advisories](gradle-first-party-advisories.json), [новые zstd advisories](zstd-first-party-advisories.json), [CNA Shadow](shadow-cve-record.json).
- [Snapshot metadata и URL для 59 пакетов](maven-metadata.json), [проверка предложенных targets OSV](proposed-targets-osv.json), [JAR summary/hash](fat-jar-summary.json).

Остаётся проверить exact SBOM shaded FreeBuilder/cglib, неразрешённый jacocoAnt, неидентифицированные внутренние/native компоненты, контейнер/JRE/OS и фактические ADS auth/protocol scenarios. Эксплуатационные PoC не запускались. Отсутствие найденного вызова уязвимого API — обоснование текущей ограниченной достижимости, а не гарантия для будущих расширений CLI.

**Контроль изменений:** исходный код, build.gradle, settings.gradle, Gradle Wrapper и dependency versions не изменены; коммитов нет. Отчёт, сырые результаты и вспомогательные audit scripts находятся только в игнорируемом `build/audit/`; Gradle пересоздал игнорируемый shadow JAR. `git diff --check` проходит, `git diff --stat` для отслеживаемых файлов пуст. Существующий untracked `.vscode/` не изменялся инструментами аудита. Выполнение задачи №4 не начиналось.
