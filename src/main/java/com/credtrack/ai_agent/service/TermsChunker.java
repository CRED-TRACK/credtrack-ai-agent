package com.credtrack.ai_agent.service;

import java.util.ArrayList;
import java.util.List;

/**
 * Static utility — splits cleaned T&C text into ~400-token chunks for embedding.
 * Uses a sliding char window with sentence-boundary preference. No tokenizer dep;
 * uses char count as proxy (≈ 4 chars/token English avg). 50-char overlap between
 * adjacent chunks helps embedding recall.
 */
public final class TermsChunker {

    public record Chunk(int index, String headingPath, String text, int tokenCount) {}

    private static final int TARGET_TOKENS = 400;
    private static final int CHARS_PER_TOKEN = 4;
    private static final int TARGET_CHARS = TARGET_TOKENS * CHARS_PER_TOKEN;     // 1600
    private static final int OVERLAP_CHARS = 50;

    private TermsChunker() {}

    public static List<Chunk> chunk(String cleanedText) {
        List<Chunk> out = new ArrayList<>();
        if (cleanedText == null || cleanedText.isBlank()) return out;

        String text = cleanedText;
        int len = text.length();
        int start = 0;
        int index = 0;
        while (start < len) {
            int end = Math.min(start + TARGET_CHARS, len);
            // Prefer ending at the last sentence boundary (. ? !) before `end`.
            if (end < len) {
                int dotPos = Math.max(
                    text.lastIndexOf('.', end),
                    Math.max(text.lastIndexOf('?', end), text.lastIndexOf('!', end))
                );
                if (dotPos > start + (TARGET_CHARS / 2)) {
                    end = dotPos + 1;
                } else {
                    // Fall back to last space to avoid mid-word breaks.
                    int spacePos = text.lastIndexOf(' ', end);
                    if (spacePos > start + (TARGET_CHARS / 2)) {
                        end = spacePos + 1;
                    }
                }
            }
            String slice = text.substring(start, end).trim();
            if (!slice.isEmpty()) {
                int tokens = Math.max(1, slice.length() / CHARS_PER_TOKEN);
                out.add(new Chunk(index++, null, slice, tokens));
            }
            if (end >= len) break;
            start = Math.max(end - OVERLAP_CHARS, start + 1);
        }
        return out;
    }
}
