package com.rd.retry.dynamo;

import com.rd.retry.config.ApplicationProperties;
import com.rd.retry.domain.Profile;
import java.io.IOException;
import java.time.Duration;
import java.util.Set;
import lombok.RequiredArgsConstructor;
import org.springframework.context.annotation.Bean;
import org.springframework.context.annotation.Configuration;
import software.amazon.awssdk.auth.credentials.EnvironmentVariableCredentialsProvider;
import software.amazon.awssdk.core.client.config.ClientOverrideConfiguration;
import software.amazon.awssdk.core.exception.ApiCallAttemptTimeoutException;
import software.amazon.awssdk.core.exception.ApiCallTimeoutException;
import software.amazon.awssdk.core.exception.RetryableException;
import software.amazon.awssdk.core.retry.RetryPolicy;
import software.amazon.awssdk.core.retry.backoff.BackoffStrategy;
import software.amazon.awssdk.core.retry.backoff.FullJitterBackoffStrategy;
import software.amazon.awssdk.core.retry.conditions.OrRetryCondition;
import software.amazon.awssdk.core.retry.conditions.RetryCondition;
import software.amazon.awssdk.core.retry.conditions.RetryOnClockSkewCondition;
import software.amazon.awssdk.core.retry.conditions.RetryOnExceptionsCondition;
import software.amazon.awssdk.core.retry.conditions.RetryOnStatusCodeCondition;
import software.amazon.awssdk.core.retry.conditions.RetryOnThrottlingCondition;
import software.amazon.awssdk.enhanced.dynamodb.DynamoDbAsyncTable;
import software.amazon.awssdk.enhanced.dynamodb.DynamoDbEnhancedAsyncClient;
import software.amazon.awssdk.enhanced.dynamodb.TableSchema;
import software.amazon.awssdk.http.HttpStatusCode;
import software.amazon.awssdk.http.async.SdkAsyncHttpClient;
import software.amazon.awssdk.http.crt.AwsCrtAsyncHttpClient;
import software.amazon.awssdk.regions.Region;
import software.amazon.awssdk.services.dynamodb.DynamoDbAsyncClient;
import software.amazon.awssdk.services.dynamodb.model.TransactionInProgressException;

@RequiredArgsConstructor
@Configuration
public class DynamoDbConfig {

    private final ApplicationProperties applicationProperties;

    @Bean
    ClientOverrideConfiguration clientConfig() {
        final var configBuilder = ClientOverrideConfiguration.builder();
        return configBuilder
                .retryPolicy(dynamoClientRetryConfig())
                .apiCallTimeout(Duration.ofMillis(applicationProperties.getTimeout()))
                .build();
    }

    @Bean
    DynamoDbAsyncClient dynamoDbClient(final ClientOverrideConfiguration clientConfiguration) {
        return DynamoDbAsyncClient.builder()
                .credentialsProvider(EnvironmentVariableCredentialsProvider.create())
                .region(Region.US_EAST_1)
                .overrideConfiguration(clientConfiguration)
                .httpClient(getCrtClient())
                .build();
    }

    @Bean
    DynamoDbEnhancedAsyncClient dynamoDbEnhancedAsyncClient(final DynamoDbAsyncClient dynamoDbClient) {
        return DynamoDbEnhancedAsyncClient
                .builder()
                .dynamoDbClient(dynamoDbClient)
                .build();
    }

    @Bean
    DynamoDbAsyncTable<Profile> profileDynamoDbAsyncTable(final DynamoDbEnhancedAsyncClient dynamoDbEnhancedClient) {
        return dynamoDbEnhancedClient.table(applicationProperties.getTable(), TableSchema.fromBean(Profile.class));
    }

    RetryPolicy dynamoClientRetryConfig() {
        return RetryPolicy.builder()
                .numRetries(applicationProperties.getNumberOfRetries())
                .retryCondition(getRetryCondition())
                .backoffStrategy(getBackoffStrategy())
                .retryCapacityCondition(null)
                .build();
    }

    private BackoffStrategy getBackoffStrategy() {
        return FullJitterBackoffStrategy.builder()
                .baseDelay(Duration.ofMillis(applicationProperties.getBaseDelay()))
                .maxBackoffTime(Duration.ofMillis(applicationProperties.getMaxBackOff()))
                .build();
    }

    private RetryCondition getRetryCondition() {
        return OrRetryCondition.create(
                RetryOnStatusCodeCondition.create(retryStatusCode()),
                RetryOnExceptionsCondition.create(retryExceptions()),
                RetryOnClockSkewCondition.create(),
                RetryOnThrottlingCondition.create()
        );
    }

    private Set<Integer> retryStatusCode() {
        return Set.of(
                HttpStatusCode.INTERNAL_SERVER_ERROR,
                HttpStatusCode.BAD_GATEWAY,
                HttpStatusCode.SERVICE_UNAVAILABLE,
                HttpStatusCode.GATEWAY_TIMEOUT);
    }

    private Set<Class<? extends Exception>> retryExceptions() {
        return Set.of(
                RetryableException.class,
                IOException.class,
                TransactionInProgressException.class,
                ApiCallAttemptTimeoutException.class,
                ApiCallTimeoutException.class
        );
    }

    private SdkAsyncHttpClient getCrtClient() {
        return AwsCrtAsyncHttpClient.builder()
                .maxConcurrency(50)
                .build();
    }

}
