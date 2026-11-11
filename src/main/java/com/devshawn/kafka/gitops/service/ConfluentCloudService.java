package com.devshawn.kafka.gitops.service;

import com.devshawn.kafka.gitops.domain.confluent.ServiceAccount;
import com.devshawn.kafka.gitops.exception.ConfluentCloudException;
import tools.jackson.core.type.TypeReference;
import tools.jackson.core.JacksonException;
import tools.jackson.databind.ObjectMapper;
import org.slf4j.LoggerFactory;

import java.io.IOException;
import java.util.List;

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
        java.util.Scanner s = new java.util.Scanner(Runtime.getRuntime().exec(cmd).getInputStream()).useDelimiter("\\A");
        return s.hasNext() ? s.next() : "";
    }

    static {
        ccloudExecutable = System.getenv("CCLOUD_EXECUTABLE_PATH") != null ? System.getenv("CCLOUD_EXECUTABLE_PATH") : "ccloud";
        LOG.info("Using ccloud executable at: {}", ccloudExecutable);
    }
}
