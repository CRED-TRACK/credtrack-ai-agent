package com.credtrack.ai_agent.model;

import com.fasterxml.jackson.annotation.JsonIgnoreProperties;
import lombok.Data;
import lombok.NoArgsConstructor;

import java.util.List;

/**
 * Returned by GET /internal/gmail-credentials.
 * Contains OAuth tokens + the user's registered cards so the AI agent
 * never needs to ask the LLM which bank or card an email belongs to.
 */
@Data
@NoArgsConstructor
@JsonIgnoreProperties(ignoreUnknown = true)
public class GmailUserCredential {
    private String       userId;
    private String       accessToken;
    private String       tokenExpiryUtc;   // ISO-8601 e.g. "2026-04-08T23:00:00"
    private String       gmailAddress;
    private Long         historyId;         // null on first poll
    private List<CardInfo> cards;           // registered active cards for this user
}
