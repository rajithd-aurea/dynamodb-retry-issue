package com.rd.retry.config;

import lombok.Data;
import org.springframework.beans.factory.annotation.Value;
import org.springframework.context.annotation.Configuration;

@Configuration
@Data
public class ApplicationProperties {

    @Value("${application.dynamdb.table}")
    private String table;

    @Value("${application.dynamdb.numberOfRetries}")
    private int numberOfRetries;

    @Value("${application.dynamdb.timeout}")
    private int timeout;

    @Value("${application.dynamdb.baseDelay}")
    private int baseDelay;

    @Value("${application.dynamdb.maxBackOff}")
    private int maxBackOff;

    @Value("${application.key}")
    private String key;

}
