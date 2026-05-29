package com.credtrack.ai_agent.model;

import com.fasterxml.jackson.annotation.JsonIgnoreProperties;
import com.fasterxml.jackson.annotation.JsonProperty;

import java.time.LocalDateTime;

@JsonIgnoreProperties(ignoreUnknown = true)
public record CardProductToScrape(
        @JsonProperty("card_product_id") Long cardProductId,
        @JsonProperty("bank_key") String bankKey,
        @JsonProperty("product_name") String productName,
        @JsonProperty("official_name") String officialName,
        @JsonProperty("terms_url") String termsUrl,
        @JsonProperty("last_content_hash") String lastContentHash,
        @JsonProperty("last_fetched_at") LocalDateTime lastFetchedAt
) {}
