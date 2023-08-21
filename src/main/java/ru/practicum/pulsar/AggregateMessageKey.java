package ru.practicum.pulsar;

import com.fasterxml.jackson.annotation.JsonCreator;
import com.fasterxml.jackson.annotation.JsonProperty;
import lombok.Data;

import java.util.UUID;

@Data
public class AggregateMessageKey {
    @JsonProperty("aggregate_name")
    private String aggregateName;
    @JsonProperty("aggregate_id")
    private String aggregateId;

    @JsonCreator
    public AggregateMessageKey(@JsonProperty("aggregate_name") String aggregateName, @JsonProperty("aggregate_id") String aggregateId) {
        this.aggregateName = aggregateName;
        this.aggregateId = aggregateId;
    }

    public String toJsonString() {
        String localAggregateName = this.getAggregateName();
        return "{\"aggregate_id\":\"" + this.aggregateId + "\",\"aggregate_name\":\"" + localAggregateName + "\"}";
    }
}
