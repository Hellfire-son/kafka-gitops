package com.devshawn.kafka.gitops.util;

import com.devshawn.kafka.gitops.exception.KafkaExecutionException;
import com.devshawn.kafka.gitops.exception.KafkaExecutionException.Code;
import org.apache.kafka.common.errors.*;

import javax.net.ssl.SSLHandshakeException;
import java.net.ConnectException;
import java.net.UnknownHostException;
import java.util.Collections;
import java.util.IdentityHashMap;
import java.util.Set;

/** Classify exception types only. Raw server/login/configuration messages never leave this boundary. */
public final class KafkaDiagnostics {
    private KafkaDiagnostics() { }

    public static KafkaExecutionException failure(String operation, Throwable error) {
        Code code = Thread.currentThread().isInterrupted() ? Code.INTERRUPTED : Code.KAFKA_ERROR;
        Set<Throwable> seen = Collections.newSetFromMap(new IdentityHashMap<>());
        Throwable current = error;
        for (int depth = 0; current != null && depth < 16 && seen.add(current); depth++, current = current.getCause()) {
            if (current instanceof KafkaExecutionException known && known.getCode() != Code.KAFKA_ERROR) return known;
            if (current instanceof InterruptedException) { code = Code.INTERRUPTED; Thread.currentThread().interrupt(); break; }
            if (current instanceof SslAuthenticationException || current instanceof SSLHandshakeException) { code = Code.SSL_HANDSHAKE_FAILED; break; }
            if (current instanceof AuthenticationException) { code = Code.AUTHENTICATION_FAILED; break; }
            if (current instanceof AuthorizationException) { code = Code.AUTHORIZATION_FAILED; break; }
            if (current instanceof UnknownHostException) { code = Code.DNS_FAILURE; break; }
            if (current instanceof ConnectException) { code = Code.CONNECTION_REFUSED; break; }
            if (current instanceof org.apache.kafka.common.errors.TimeoutException || current instanceof java.util.concurrent.TimeoutException) code = Code.TIMEOUT;
            else if (code == Code.KAFKA_ERROR && (current instanceof NetworkException || current instanceof BrokerNotAvailableException)) code = Code.KAFKA_UNAVAILABLE;
        }
        String action = switch (code) {
            case AUTHENTICATION_FAILED -> "Check security.protocol, sasl.mechanism and the configured login credentials.";
            case SSL_HANDSHAKE_FAILED -> "Check TLS certificates, truststore, keystore and host names.";
            case AUTHORIZATION_FAILED -> "Check the ACLs required for this operation; no rights were added automatically.";
            case DNS_FAILURE -> "Check bootstrap host names and DNS resolution.";
            case CONNECTION_REFUSED, KAFKA_UNAVAILABLE, TIMEOUT -> "Check bootstrap.servers, listeners, network access and configured timeouts.";
            case INTERRUPTED -> "Operation interrupted; the thread interrupt flag has been restored.";
            default -> "Check Kafka configuration and broker state.";
        };
        String type = error instanceof java.util.concurrent.ExecutionException && error.getCause() != null
                ? error.getCause().getClass().getSimpleName() : error.getClass().getSimpleName();
        return new KafkaExecutionException(operation, "State: " + code + ". Failure type: " + type + ". " + action
                + " External exception text omitted. Verify Kafka state before retrying; changes may be partially applied.", code);
    }
}
