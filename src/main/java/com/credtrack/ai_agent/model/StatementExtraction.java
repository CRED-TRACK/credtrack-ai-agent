package com.credtrack.ai_agent.model;

import com.fasterxml.jackson.annotation.JsonProperty;
import lombok.Data;
import lombok.NoArgsConstructor;

/**
 * Structured output from the LLM (manual Jackson deserialization).
 *
 * bank and cardId are NOT extracted by the LLM — they are resolved before
 * the LLM call using the sender domain + registered cards from the backend.
 */
@Data
@NoArgsConstructor
public class StatementExtraction {
    // @JsonProperty needed: Lombok generates isStatement()/setStatement() for boolean fields
    // with 'is' prefix, which Jackson maps to property "statement" — not "isStatement".
    @JsonProperty("isStatement")
    private boolean isStatement;
    private String  cardDigits;          // digits shown in email — we take last 4
    private Double  statementBalance;
    private Double  minimumPaymentDue;
    private String  dueDate;             // YYYY-MM-DD
    private String  statementDate;       // YYYY-MM-DD
    private String  viewStatementUrl;
    private String  makePaymentUrl;
    private Double  confidence;          // 0.0 – 1.0
}
