package com.credtrack.ai_agent.model;

/**
 * Result of LLM extraction from a utility bill email (Eversource / National Grid).
 * Produced by {@link com.credtrack.ai_agent.service.ExtractionService#extractUtilityBill}.
 */
public record UtilityBillExtraction(
        boolean isBill,
        String  accountLastFour,   // last 4 digits shown in the email
        double  amountDue,
        String  dueDate,           // YYYY-MM-DD string from LLM
        String  billDate,          // YYYY-MM-DD
        String  billingPeriodStart,// YYYY-MM-DD
        String  billingPeriodEnd,  // YYYY-MM-DD
        double  confidence
) {}
