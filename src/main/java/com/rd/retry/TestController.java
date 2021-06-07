package com.rd.retry;

import com.rd.retry.config.ApplicationProperties;
import com.rd.retry.domain.Profile;
import java.util.List;
import lombok.RequiredArgsConstructor;
import lombok.extern.slf4j.Slf4j;
import org.springframework.web.bind.annotation.GetMapping;
import org.springframework.web.bind.annotation.RequestMapping;
import org.springframework.web.bind.annotation.RestController;
import reactor.core.publisher.Flux;
import reactor.core.publisher.Mono;
import software.amazon.awssdk.enhanced.dynamodb.DynamoDbAsyncTable;
import software.amazon.awssdk.enhanced.dynamodb.DynamoDbEnhancedAsyncClient;
import software.amazon.awssdk.enhanced.dynamodb.Key;
import software.amazon.awssdk.enhanced.dynamodb.model.BatchGetItemEnhancedRequest;
import software.amazon.awssdk.enhanced.dynamodb.model.BatchGetResultPage;
import software.amazon.awssdk.enhanced.dynamodb.model.ReadBatch;

@Slf4j
@RestController
@RequiredArgsConstructor
@RequestMapping("/test")
public class TestController {

    private final DynamoDbEnhancedAsyncClient asyncClient;
    private final DynamoDbAsyncTable<Profile> profileDynamoDbAsyncTable;
    private final ApplicationProperties applicationProperties;

    @GetMapping("/profile")
    public Mono<Profile> testDynamo() {
        log.info("Received request");
        Mono<Profile> map = Flux.from(asyncClient
                .batchGetItem(BatchGetItemEnhancedRequest.builder().addReadBatch(createReadBatch()).build()))
                .collectList()
                .map(pages -> handleBatchItems(pages));
        log.info("Response send");
        return map;
    }

    private Profile handleBatchItems(List<BatchGetResultPage> pages) {
        return pages.stream()
                .flatMap(page -> page.resultsForTable(profileDynamoDbAsyncTable).stream())
                .filter(pr -> pr.getPk().equals(applicationProperties.getKey()))
                .findFirst()
                .orElseThrow(() -> new IllegalArgumentException("Invalid id"));

    }

    private ReadBatch createReadBatch() {
        return ReadBatch.builder(Profile.class)
                .mappedTableResource(profileDynamoDbAsyncTable)
                .addGetItem(Key.builder().partitionValue(applicationProperties.getKey()).build())
                .build();
    }


}
