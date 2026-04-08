package com.credtrack.ai_agent.model;

import java.time.LocalDate;

/**
 * A single Gmail message after fetching + decoding.
 * senderDomain  — extracted from From header, used to identify the bank.
 * sentDate      — Gmail internalDate, used as statement closing date.
 * viewStatementUrl / makePaymentUrl — extracted directly from HTML <a href> tags
 *   by GmailService (bypasses LLM to avoid token-limit URL truncation).
 */
public record EmailMessage(
        String    messageId,
        String    subject,
        String    body,
        String    snippet,
        String    senderDomain,
        LocalDate sentDate,
        String    viewStatementUrl,
        String    makePaymentUrl
) {}
