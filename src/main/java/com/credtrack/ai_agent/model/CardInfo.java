package com.credtrack.ai_agent.model;

import java.time.LocalDate;

/**
 * A single registered card returned inside GmailUserCredential.
 * Sourced from the backend — bank key matches CardProduct.bankKey exactly.
 */
public record CardInfo(
        Long      cardId,
        String    lastFour,
        String    bankKey,              // CHASE | AMEX | BOA | DISCOVER | CITI | CAPITAL_ONE | WELLS_FARGO | US_BANK
        boolean   gmailScanComplete,   // false = historical scan not yet done for this card
        LocalDate lastStatementDate,   // date of most recent saved statement (null if none)
        boolean   hasUnpaidStatements  // true = at least one statement for this card is unpaid
) {}
