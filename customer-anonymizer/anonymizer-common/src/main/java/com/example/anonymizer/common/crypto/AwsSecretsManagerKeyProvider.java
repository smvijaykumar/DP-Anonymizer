package com.example.anonymizer.common.crypto;

import com.fasterxml.jackson.databind.JsonNode;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.typesafe.config.Config;
import software.amazon.awssdk.http.urlconnection.UrlConnectionHttpClient;
import software.amazon.awssdk.regions.Region;
import software.amazon.awssdk.services.secretsmanager.SecretsManagerClient;
import software.amazon.awssdk.services.secretsmanager.model.GetSecretValueRequest;

import java.io.IOException;

/**
 * Reads keys from an AWS Secrets Manager secret whose SecretString is JSON keyed by version, e.g.
 * {@code {"v1": "<base64>"}}. Credentials come from the default provider chain (IRSA on EKS).
 */
public final class AwsSecretsManagerKeyProvider implements KeyProvider {
    private static final ObjectMapper MAPPER = new ObjectMapper();
    private final String secretId;
    private final String region;

    public AwsSecretsManagerKeyProvider(Config c) {
        this.secretId = c.getString("secret-id");
        this.region = c.getString("region");
    }

    @Override
    public byte[] keyBytes(String version) {
        try (SecretsManagerClient client = SecretsManagerClient.builder()
                .region(Region.of(region))
                .httpClientBuilder(UrlConnectionHttpClient.builder())
                .build()) {
            String json = client.getSecretValue(GetSecretValueRequest.builder().secretId(secretId).build()).secretString();
            JsonNode value = MAPPER.readTree(json).path(version);
            if (value.isMissingNode() || value.asText().isBlank()) {
                throw new KeyUnavailableException("HMAC key version " + version + " is not present in the secret");
            }
            return KeyProviders.decode(value.asText(), version);
        } catch (KeyUnavailableException e) {
            throw e;
        } catch (IOException e) {
            throw new KeyUnavailableException("HMAC key secret is not valid JSON", e);
        } catch (RuntimeException e) {
            throw new KeyUnavailableException("Could not read HMAC key from AWS Secrets Manager", e);
        }
    }
}
