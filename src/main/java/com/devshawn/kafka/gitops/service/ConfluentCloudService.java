package com.devshawn.kafka.gitops.service;

import com.devshawn.kafka.gitops.domain.confluent.ServiceAccount;
import com.devshawn.kafka.gitops.exception.ConfluentCloudException;
import tools.jackson.core.type.TypeReference;
import tools.jackson.core.JacksonException;
import tools.jackson.databind.ObjectMapper;
import org.slf4j.LoggerFactory;

import java.io.IOException;
import java.nio.charset.StandardCharsets;
import java.time.Duration;
import java.util.List;
import java.util.concurrent.FutureTask;
import java.util.concurrent.TimeUnit;

public class ConfluentCloudService {

    private static final org.slf4j.Logger LOG = LoggerFactory.getLogger(ConfluentCloudService.class);

    private final ObjectMapper objectMapper;
    private final CommandRunner commands;
    private static final String ccloudExecutable;

    public ConfluentCloudService(ObjectMapper objectMapper) {
        this(objectMapper, ConfluentCloudService::execCmd);
    }

    @FunctionalInterface
    interface CommandRunner {
        String run(String[] command) throws IOException;
    }

    ConfluentCloudService(ObjectMapper objectMapper, CommandRunner commands) {
        this.objectMapper = objectMapper;
        this.commands = commands;
    }

    public List<ServiceAccount> getServiceAccounts() {
        LOG.info("Fetching service account list from Confluent Cloud via ccloud tool.");
        try {
            String result = commands.run(new String[]{ccloudExecutable, "service-account", "list", "-o", "json"});
            return objectMapper.readValue(result, new TypeReference<>() {
            });
        } catch (IOException | JacksonException ex) {
            throw new ConfluentCloudException("There was an error listing Confluent Cloud service accounts. Are you logged in?");
        }
    }

    public ServiceAccount createServiceAccount(String name, boolean isUser) {
        LOG.info("Creating service account {} in Confluent Cloud via ccloud tool.", name);
        try {
            String serviceName = isUser ? String.format("user-%s", name) : name;
            String description = isUser ? String.format("User: %s", name) : String.format("Service account: %s", name);
            String result = commands.run(new String[]{ccloudExecutable, "service-account", "create", serviceName, "--description", description, "-o", "json"});
            return objectMapper.readValue(result, ServiceAccount.class);
        } catch (IOException | JacksonException ex) {
            throw new ConfluentCloudException(String.format("There was an error creating Confluent Cloud service account: %s.", name));
        }
    }

    public static String execCmd(String[] cmd) throws java.io.IOException {
        return execCmd(cmd, Duration.ofSeconds(30));
    }

    // Package-local bounded seam for isolated subprocess tests; never configurable through JSON.
    static String execCmd(String[] cmd, Duration timeout) throws IOException {
        if (Thread.currentThread().isInterrupted()) throw new IOException("External command interrupted.");
        if (timeout == null || timeout.isNegative() || timeout.isZero() || timeout.compareTo(Duration.ofMinutes(1)) > 0)
            throw new IOException("Invalid external command timeout.");
        long deadline = System.nanoTime() + timeout.toNanos();
        Process process;
        try { process = new ProcessBuilder(cmd).redirectError(ProcessBuilder.Redirect.DISCARD).start(); }
        catch (IOException | RuntimeException ex) { throw new IOException("External command could not be started."); }
        FutureTask<byte[]> output = new FutureTask<>(() -> {
            byte[] bytes = process.getInputStream().readNBytes(1_048_577);
            if (bytes.length > 1_048_576) throw new IOException("External command output exceeded its limit.");
            return bytes;
        });
        Thread.ofVirtual().start(output);
        try {
            byte[] bytes = output.get(Math.max(1, deadline - System.nanoTime()), TimeUnit.NANOSECONDS);
            long remaining = deadline - System.nanoTime();
            if (remaining <= 0 || !process.waitFor(remaining, TimeUnit.NANOSECONDS)) throw new IOException("External command timed out.");
            if (process.exitValue() != 0) throw new IOException("External command exited unsuccessfully.");
            return new String(bytes, StandardCharsets.UTF_8);
        } catch (InterruptedException ex) {
            Thread.currentThread().interrupt();
            throw new IOException("External command interrupted.");
        } catch (java.util.concurrent.ExecutionException | java.util.concurrent.TimeoutException ex) {
            throw new IOException("External command failed, timed out or exceeded its output limit.");
        } finally {
            boolean interrupted = Thread.currentThread().isInterrupted();
            try {
                if (process.isAlive()) process.destroyForcibly();
                output.cancel(true);
                try { process.getInputStream().close(); } catch (IOException ignored) { }
                try { process.getErrorStream().close(); } catch (IOException ignored) { }
                try { process.getOutputStream().close(); } catch (IOException ignored) { }
            } finally { if (interrupted) Thread.currentThread().interrupt(); }
        }
    }

    static {
        ccloudExecutable = System.getenv("CCLOUD_EXECUTABLE_PATH") != null ? System.getenv("CCLOUD_EXECUTABLE_PATH") : "ccloud";
        LOG.info("Using ccloud executable at: {}", ccloudExecutable);
    }
}
