// Modified for the independent Kafka GitOps 0.4.0 fork; see CHANGES-FROM-UPSTREAM.md.
package com.devshawn.kafka.gitops.service;

import com.devshawn.kafka.gitops.domain.state.DesiredStateFile;
import com.devshawn.kafka.gitops.domain.state.settings.Settings;
import com.devshawn.kafka.gitops.exception.ValidationException;
import tools.jackson.core.StreamReadFeature;
import tools.jackson.core.JacksonException;
import tools.jackson.databind.DeserializationFeature;
import tools.jackson.databind.DatabindException;
import tools.jackson.databind.ObjectMapper;
import tools.jackson.databind.exc.InvalidFormatException;
import tools.jackson.databind.exc.UnrecognizedPropertyException;
import tools.jackson.databind.exc.ValueInstantiationException;
import tools.jackson.dataformat.yaml.YAMLMapper;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.File;
import java.io.FileNotFoundException;
import java.nio.file.Paths;

public class ParserService {

    private static final Logger LOG = LoggerFactory.getLogger(ParserService.class);

    private final ObjectMapper objectMapper;

    private final File file;

    public ParserService(File file) {
        this.objectMapper = YAMLMapper.builder()
                .enable(DeserializationFeature.FAIL_ON_UNKNOWN_PROPERTIES)
                .disable(DeserializationFeature.FAIL_ON_NULL_FOR_PRIMITIVES)
                .disable(DeserializationFeature.FAIL_ON_TRAILING_TOKENS)
                .enable(DeserializationFeature.FAIL_ON_READING_DUP_TREE_KEY)
                .enable(StreamReadFeature.STRICT_DUPLICATE_DETECTION)
                .build();
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
            DesiredStateFile parsed = objectMapper.readValue(stateFile, DesiredStateFile.class);
            if (parsed == null) throw new ValidationException("The state file must contain a YAML object.");
            return parsed;
        } catch (ValueInstantiationException ex) {
            throw new ValidationException("Missing or invalid required fields in state file.");
        } catch (UnrecognizedPropertyException ex) {
            throw new ValidationException("Unrecognized field in state file.");
        } catch (InvalidFormatException ex) {
            throw new ValidationException("Invalid field format in state file.");
        } catch (DatabindException ex) {
            throw new ValidationException("Invalid state file structure or field type.");
        } catch (JacksonException ex) {
            if (ex.getCause() instanceof FileNotFoundException) {
                throw new ValidationException("The specified state file could not be found.");
            }
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
