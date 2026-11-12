package com.devshawn.kafka.gitops.exception;

public class KafkaExecutionException extends RuntimeException {

    private final String exceptionMessage;
    private final Code code;

    public enum Code { KAFKA_ERROR, BOOTSTRAP_MISSING, INVALID_TIMEOUT, DNS_FAILURE, CONNECTION_REFUSED,
        KAFKA_UNAVAILABLE, AUTHENTICATION_FAILED, SSL_HANDSHAKE_FAILED, AUTHORIZATION_FAILED, TIMEOUT, INTERRUPTED, METADATA_INVALID }

    public KafkaExecutionException(String message, String exceptionMessage) {
        this(message, exceptionMessage, Code.KAFKA_ERROR);
    }

    public KafkaExecutionException(String message, String exceptionMessage, Code code) {
        super(message);
        this.exceptionMessage = exceptionMessage;
        this.code = code;
    }

    public Code getCode() { return code; }

    public String getExceptionMessage() {
        return exceptionMessage;
    }
}
