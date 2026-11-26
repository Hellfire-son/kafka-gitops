// Modified for the independent Kafka GitOps 0.4.0 fork; see CHANGES-FROM-UPSTREAM.md.
package com.devshawn.kafka.gitops.exception;

public class KafkaExecutionException extends RuntimeException {

    private final String exceptionMessage;
    private final Code code;
    private final WriteOutcome writeOutcome;

    public enum Code { KAFKA_ERROR, BOOTSTRAP_MISSING, INVALID_TIMEOUT, DNS_FAILURE, CONNECTION_REFUSED,
        KAFKA_UNAVAILABLE, AUTHENTICATION_FAILED, SSL_HANDSHAKE_FAILED, AUTHORIZATION_FAILED, TIMEOUT, INTERRUPTED,
        METADATA_INVALID, PARTITION_PLAN_STALE }

    /** Evidence available when reporting a failed command after a resource operation. */
    public enum WriteOutcome { UNKNOWN, NOT_SENT, REJECTED, CONFIRMED, NO_OP, PARTIAL }

    public KafkaExecutionException(String message, String exceptionMessage) {
        this(message, exceptionMessage, Code.KAFKA_ERROR);
    }

    public KafkaExecutionException(String message, String exceptionMessage, Code code) {
        this(message, exceptionMessage, code, WriteOutcome.UNKNOWN);
    }

    public KafkaExecutionException(String message, String exceptionMessage, Code code, WriteOutcome writeOutcome) {
        super(message);
        this.exceptionMessage = exceptionMessage;
        this.code = code;
        this.writeOutcome = writeOutcome;
    }

    public Code getCode() { return code; }

    public WriteOutcome getWriteOutcome() { return writeOutcome; }

    public String getExceptionMessage() {
        return exceptionMessage;
    }
}
