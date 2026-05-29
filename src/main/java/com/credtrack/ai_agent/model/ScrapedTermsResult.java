package com.credtrack.ai_agent.model;

import lombok.Builder;
import lombok.Value;

import java.util.List;

/**
 * Result of one card_product scrape cycle. Surfaced via the trigger endpoint
 * for manual inspection. Backend writeback is a separate future step.
 */
@Value
@Builder
public class ScrapedTermsResult {
    Long cardProductId;
    String bankKey;
    String productName;
    String sourceUrl;
    int httpStatus;
    String contentHash;
    boolean unchanged;
    int cleanedTextChars;
    int chunkCount;
    int embeddingDim;
    List<ExtractedRule> rules;
    String error;

    @Value
    @Builder
    public static class ExtractedRule {
        String canonicalCategory;
        Integer rateBps;
        Integer baseRateBps;
        String capPeriod;
        java.math.BigDecimal capAmount;
        String capGroupKey;
        Boolean requiresUserChoice;
        String channelRestriction;
        List<String> exclusions;
        String notes;
        Double confidence;
    }
}
