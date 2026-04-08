package com.credtrack.ai_agent.model;

/**
 * A single registered card returned inside GmailUserCredential.
 * Sourced from the backend — bank key matches CardProduct.bankKey exactly.
 */
public record CardInfo(
        Long   cardId,
        String lastFour,
        String bankKey    // CHASE | AMEX | BOA | DISCOVER | CITI | CAPITAL_ONE | WELLS_FARGO | US_BANK
) {}
