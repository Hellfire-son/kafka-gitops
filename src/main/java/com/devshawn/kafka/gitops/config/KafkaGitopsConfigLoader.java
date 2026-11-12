package com.devshawn.kafka.gitops.config;

import com.devshawn.kafka.gitops.exception.MissingConfigurationException;
import org.apache.kafka.clients.CommonClientConfigs;
import org.apache.kafka.common.config.SaslConfigs;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.File;
import java.io.FileInputStream;
import java.io.IOException;
import java.io.InputStream;
import java.util.Collections;
import java.util.HashMap;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Locale;
import java.util.Map;
import java.util.Properties;

public class KafkaGitopsConfigLoader {

    private static final Logger LOG = LoggerFactory.getLogger(KafkaGitopsConfigLoader.class);

    // Everything outside this explicit allowlist is omitted, including unknown plugin settings.
    private static final List<String> SAFE_DIAGNOSTIC_CONFIG_KEYS = List.of(
            CommonClientConfigs.BOOTSTRAP_SERVERS_CONFIG,
            CommonClientConfigs.CLIENT_ID_CONFIG,
            CommonClientConfigs.SECURITY_PROTOCOL_CONFIG,
            SaslConfigs.SASL_MECHANISM
    );

    private KafkaGitopsConfigLoader() {
    }

    public static KafkaGitopsConfig load() {
        return load(null);
    }

    public static KafkaGitopsConfig load(File configFile) {
        KafkaGitopsConfig.Builder builder = new KafkaGitopsConfig.Builder();
        setConfigFromEnvironment(builder);
        setConfigFromFile(configFile, builder);
        Map<String, Object> effective = builder.getConfig();
        String protocol = String.valueOf(effective.getOrDefault(CommonClientConfigs.SECURITY_PROTOCOL_CONFIG, ""));
        // GSSAPI can use the JVM JAAS file/keytab; username/password shortcuts do not apply.
        if (!protocol.equals("PLAINTEXT") && !protocol.equals("SSL")
                && !"GSSAPI".equals(effective.get(SaslConfigs.SASL_MECHANISM))
                && !effective.containsKey(SaslConfigs.SASL_JAAS_CONFIG)) {
            builder.putAllConfig(handleAuthentication(effective.get(SaslConfigs.SASL_MECHANISM),
                    System.getenv("KAFKA_SASL_JAAS_USERNAME"), System.getenv("KAFKA_SASL_JAAS_PASSWORD")));
        }
        handleDefaultConfig(builder);
        KafkaGitopsConfig config = builder.build();
        LOG.info("Kafka Config (safe diagnostic fields only): {}", configurationForLogging(config.getConfig()));
        return config;
    }

    private static void setConfigFromFile(File configFile, KafkaGitopsConfig.Builder builder) {
        if (configFile == null) {
            return;
        }
        try (InputStream inputStream = new FileInputStream(configFile)) {
            Properties properties = new Properties();
            properties.load(inputStream);
            properties.forEach( (k, v) -> builder.putConfig(k.toString(), v));
        } catch (IOException | IllegalArgumentException ex) {
            // Never continue with defaults after an explicitly requested file failed to load.
            throw new MissingConfigurationException("COMMAND_CONFIG_UNREADABLE; expected a readable Kafka properties file with valid syntax. Check --command-config.");
        }
    }

    private static void setConfigFromEnvironment(KafkaGitopsConfig.Builder builder) {
        Map<String, Object> config = new HashMap<>();
        for (Map.Entry<String, String> entry : System.getenv().entrySet()) {
            String key = entry.getKey();
            String value = entry.getValue();
            if (key.equals("KAFKA_SASL_JAAS_USERNAME") || key.equals("KAFKA_SASL_JAAS_PASSWORD")) {
                continue; // Shortcuts are evaluated only after file properties have taken precedence.
            } else if (key.startsWith("KAFKA_")) {
                String newKey = key.substring(6).replace("_", ".").toLowerCase(Locale.ROOT);
                config.put(newKey, value);
            }
        }

        builder.putAllConfig(config);
    }

    private static Map<String, Object> configurationForLogging(Map<String, Object> config) {
        Map<String, Object> diagnosticConfig = new LinkedHashMap<>();
        for (String key : SAFE_DIAGNOSTIC_CONFIG_KEYS) {
            if (config.containsKey(key)) {
                diagnosticConfig.put(key, key.equals(CommonClientConfigs.BOOTSTRAP_SERVERS_CONFIG)
                        ? com.devshawn.kafka.gitops.util.KafkaDiagnostics.bootstrap(config.get(key)) : config.get(key));
            }
        }
        return diagnosticConfig;
    }

    private static void handleDefaultConfig(KafkaGitopsConfig.Builder builder) {
        if (!builder.getConfig().containsKey(CommonClientConfigs.CLIENT_ID_CONFIG)) {
            builder.putConfig(CommonClientConfigs.CLIENT_ID_CONFIG, "kafka-gitops");
        }
    }

    private static Map<String, Object> handleAuthentication(Object saslMechanism, String username, String password) {
        if (username != null && password != null) {
            // Do we need the Plain or SCRAM module?
            String loginModule;
            if ("PLAIN".equals(saslMechanism)) {
                loginModule = "org.apache.kafka.common.security.plain.PlainLoginModule";
            } else if ("SCRAM-SHA-256".equals(saslMechanism) || "SCRAM-SHA-512".equals(saslMechanism)) {
                loginModule = "org.apache.kafka.common.security.scram.ScramLoginModule";
            } else {
                throw new MissingConfigurationException("KAFKA_SASL_MECHANISM");
            }

            String value = String.format("%s required username=\"%s\" password=\"%s\";",
                    loginModule, escape(username), escape(password));
            return Map.of(SaslConfigs.SASL_JAAS_CONFIG, value);
        } else if (username != null) {
            throw new MissingConfigurationException("KAFKA_SASL_JAAS_PASSWORD");
        } else if (password != null) {
            throw new MissingConfigurationException("KAFKA_SASL_JAAS_USERNAME");
        }

        return Collections.emptyMap();
    }

    private static String escape(String value) {
        return value.replace("\\", "\\\\").replace("\"", "\\\"");
    }
}
