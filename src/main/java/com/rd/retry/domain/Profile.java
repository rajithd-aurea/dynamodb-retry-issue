package com.rd.retry.domain;

import lombok.Data;
import software.amazon.awssdk.enhanced.dynamodb.mapper.annotations.DynamoDbBean;
import software.amazon.awssdk.enhanced.dynamodb.mapper.annotations.DynamoDbPartitionKey;

@Data
@DynamoDbBean
public class Profile {

    private String pk;
    private String name;

    @DynamoDbPartitionKey
    public String getPk() {
        return pk;
    }
}
