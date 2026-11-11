package com.devshawn.kafka.gitops.service;

import com.devshawn.kafka.gitops.domain.state.DesiredStateFile;
import com.devshawn.kafka.gitops.domain.state.settings.Settings;
import com.devshawn.kafka.gitops.exception.ValidationException;
import com.fasterxml.jackson.core.JsonParser;
import com.fasterxml.jackson.databind.DeserializationFeature;
import com.fasterxml.jackson.databind.JsonMappingException;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.fasterxml.jackson.databind.exc.InvalidFormatException;
import com.fasterxml.jackson.databind.exc.UnrecognizedPropertyException;
import com.fasterxml.jackson.databind.exc.ValueInstantiationException;
import com.fasterxml.jackson.dataformat.yaml.YAMLFactory;
import com.fasterxml.jackson.datatype.jdk8.Jdk8Module;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.File;
import java.io.FileNotFoundException;
import java.io.IOException;
import java.nio.file.Paths;

public class ParserService {

    private static final Logger LOG = LoggerFactory.getLogger(ParserService.class);

    private final ObjectMapper objectMapper;

    private final File file;

    public ParserService(File file) {
        this.objectMapper = new ObjectMapper(new YAMLFactory());
        objectMapper.enable(DeserializationFeature.FAIL_ON_READING_DUP_TREE_KEY);
        objectMapper.enable(JsonParser.Feature.STRICT_DUPLICATE_DETECTION);
        objectMapper.registerModule(new Jdk8Module());
        this.file = file;
    }

    public DesiredStateFile parseStateFile() {
        DesiredStateFile desiredStateFile = parseStateFile(file);
        return desiredStateFile.getSettings().flatMap(Settings::getFiles).map(settingsFiles -> {
            DesiredStateFile.Builder builder = new DesiredStateFile.Builder().mergeFrom(desiredStateFile);
            settingsFiles.getServices()
                    .map(services -> loadExternalFile(services, "Services"))
                    .ifPresent(servicesFile -> builder.putAllServices(servicesFile.getServices()));

            settingsFiles.getTopics()
                    .map(topics -> loadExternalFile(topics, "Topics"))
                    .ifPresent(topicsFile -> builder.putAllTopics(topicsFile.getTopics()));

            settingsFiles.getUsers()
                    .map(users -> loadExternalFile(users, "Users"))
                    .ifPresent(usersFile -> builder.putAllUsers(usersFile.getUsers()));

            return builder.build();
        }).orElse(desiredStateFile);
    }

    public DesiredStateFile parseStateFile(File stateFile) {
        LOG.info("Parsing desired state file...");

        try {
            return objectMapper.readValue(stateFile, DesiredStateFile.class);
        } catch (ValueInstantiationException ex) {
            throw new ValidationException("Missing or invalid required fields in state file.");
        } catch (UnrecognizedPropertyException ex) {
            throw new ValidationException("Unrecognized field in state file.");
        } catch (InvalidFormatException ex) {
            throw new ValidationException("Invalid field format in state file.");
        } catch (JsonMappingException ex) {
            throw new ValidationException("Invalid state file structure or field type.");
        } catch (FileNotFoundException ex) {
            throw new ValidationException("The specified state file could not be found.");
        } catch (IOException ex) {
            throw new ValidationException("Invalid state file. Check syntax and file access.");
        }
    }

    private DesiredStateFile loadExternalFile(String fileName, String type) {
        File externalFile = getAdditionalFile(fileName);
        if (!externalFile.exists()) {
            throw new ValidationException(String.format("%s file could not be found.", type));
        }
        return parseStateFile(externalFile);
    }

    private File getAdditionalFile(String fileName) {
        return new File(Paths.get(file.getAbsoluteFile().getParent(), fileName).toString());
    }

}
