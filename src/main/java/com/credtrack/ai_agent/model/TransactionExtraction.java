package com.credtrack.ai_agent.model;

import java.time.LocalDate;

/**
 * Result of LLM extraction from a bank transaction alert email.
 * Produced by {@link com.credtrack.ai_agent.service.ExtractionService#extractTransaction}.
 */
public record TransactionExtraction(
        boolean   isTransaction,
        String    merchantName,
        String    merchantCategory,
        double    amount,
        String    currency,
        LocalDate transactionDate,
        String    transactionType,   // "DEBIT" or "CREDIT"
        String    description,
        double    confidence
) {}
