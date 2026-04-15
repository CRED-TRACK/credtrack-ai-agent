package com.credtrack.ai_agent.actor;

import akka.actor.typed.Behavior;
import akka.actor.typed.javadsl.AbstractBehavior;
import akka.actor.typed.javadsl.ActorContext;
import akka.actor.typed.javadsl.Behaviors;
import akka.actor.typed.javadsl.Receive;
import com.credtrack.ai_agent.model.CardInfo;
import com.credtrack.ai_agent.model.EmailMessage;
import com.credtrack.ai_agent.service.BackendApiClient;
import org.jsoup.Jsoup;

import java.math.BigDecimal;
import java.time.LocalDate;
import java.time.format.DateTimeFormatter;
import java.util.List;
import java.util.Locale;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.Executor;
import java.util.regex.Matcher;
import java.util.regex.Pattern;

/**
 * Spawned once per bank payment confirmation email.
 * No LLM needed — these emails are highly structured so regex parsing is sufficient.
 *
 * Chase — subject "Your credit card payment is scheduled":
 *   Account   Chase Freedom Unlimited Visa(...5058)
 *   Amount    $92.91
 *   Payment authorized on   Mar 21, 2026
 *   Effective date          Mar 21, 2026
 *
 * BOA — subject "We received your credit card payment":
 *   Business Credit Card  ending in 5135
 *   Payment amount        $120.51
 *   Date posted           February 12, 2026
 */
public class PaymentExtractorActor extends AbstractBehavior<PaymentExtractorActor.Command> {

    // ── Chase patterns ────────────────────────────────────────────────────────
    private static final Pattern CHASE_CARD_DIGITS = Pattern.compile("\\(\\.\\.\\.([0-9]+)\\)");
    private static final Pattern CHASE_PAYMENT_DATE = Pattern.compile(
            "Payment authorized on\\s+([A-Za-z]+ \\d{1,2},?\\s*\\d{4})", Pattern.CASE_INSENSITIVE);
    private static final Pattern CHASE_EFFECTIVE_DATE = Pattern.compile(
            "Effective date\\s+([A-Za-z]+ \\d{1,2},?\\s*\\d{4})", Pattern.CASE_INSENSITIVE);

    // ── BOA patterns ──────────────────────────────────────────────────────────
    // Primary: "ending in 5135" (newer emails)
    private static final Pattern BOA_CARD_DIGITS = Pattern.compile(
            "ending in ([0-9]+)", Pattern.CASE_INSENSITIVE);
    // Fallback: "ending 5135" (older emails that omit "in")
    private static final Pattern BOA_CARD_DIGITS_ALT = Pattern.compile(
            "ending\\s+([0-9]+)", Pattern.CASE_INSENSITIVE);
    private static final Pattern BOA_PAYMENT_DATE = Pattern.compile(
            "Date posted\\s+([A-Za-z]+ \\d{1,2},?\\s*\\d{4})", Pattern.CASE_INSENSITIVE);

    // ── Discover patterns ─────────────────────────────────────────────────────
    private static final Pattern DISCOVER_CARD_DIGITS = Pattern.compile(
            "Last 4 #:\\s*([0-9]+)", Pattern.CASE_INSENSITIVE);
    private static final Pattern DISCOVER_PAYMENT_DATE = Pattern.compile(
            "Payment Post Date:\\s+([A-Za-z]+ \\d{1,2},?\\s*\\d{4})", Pattern.CASE_INSENSITIVE);

    // ── Amex patterns ─────────────────────────────────────────────────────────
    // "Account Ending: 51006"
    private static final Pattern AMEX_CARD_DIGITS = Pattern.compile(
            "Account Ending:\\s*([0-9]+)", Pattern.CASE_INSENSITIVE);
    // "Processed on: Apr 8, 2026"
    private static final Pattern AMEX_PAYMENT_DATE = Pattern.compile(
            "Processed on:\\s+([A-Za-z]+ \\d{1,2},?\\s*\\d{4})", Pattern.CASE_INSENSITIVE);

    // ── Shared patterns ───────────────────────────────────────────────────────
    private static final Pattern AMOUNT = Pattern.compile("\\$([0-9,]+\\.[0-9]{2})");

    // Chase uses abbreviated months (Mar), BOA uses full names (February)
    private static final DateTimeFormatter DATE_FMT_SHORT =
            DateTimeFormatter.ofPattern("MMM d yyyy", Locale.US);
    private static final DateTimeFormatter DATE_FMT_LONG =
            DateTimeFormatter.ofPattern("MMMM d yyyy", Locale.US);

    // ── Messages ──────────────────────────────────────────────────────────────

    public sealed interface Command {}

    public record ExtractPayment(
            String         userId,
            String         bankKey,
            List<CardInfo> registeredCards,
            EmailMessage   email
    ) implements Command {}

    private record ParseDone(
            String     cardLastFour,
            BigDecimal amount,
            LocalDate  paymentDate,
            LocalDate  effectiveDate,
            ExtractPayment original
    ) implements Command {}

    private record ParseFailed(String messageId, String reason) implements Command {}

    // ── Factory ───────────────────────────────────────────────────────────────

    public static Behavior<Command> create(BackendApiClient backendApiClient, Executor executor) {
        return Behaviors.setup(ctx -> new PaymentExtractorActor(ctx, backendApiClient, executor));
    }

    // ── State ─────────────────────────────────────────────────────────────────

    private final BackendApiClient backendApiClient;
    private final Executor         executor;

    private PaymentExtractorActor(ActorContext<Command> context,
                                  BackendApiClient backendApiClient,
                                  Executor executor) {
        super(context);
        this.backendApiClient = backendApiClient;
        this.executor         = executor;
    }

    // ── Behavior ──────────────────────────────────────────────────────────────

    @Override
    public Receive<Command> createReceive() {
        return newReceiveBuilder()
                .onMessage(ExtractPayment.class, this::onExtract)
                .onMessage(ParseDone.class,      this::onDone)
                .onMessage(ParseFailed.class,    this::onFailed)
                .build();
    }

    private Behavior<Command> onExtract(ExtractPayment msg) {
        getContext().pipeToSelf(
                CompletableFuture.supplyAsync(() -> parsePayment(msg), executor),
                (result, ex) -> ex != null
                        ? new ParseFailed(msg.email().messageId(), ex.getMessage())
                        : result
        );
        return this;
    }

    private Command parsePayment(ExtractPayment msg) {
        String body    = toPlainText(msg.email().body());
        String bankKey = msg.bankKey();

        String cardDigits;
        String amountStr;
        String payDateStr;
        String effDateStr;

        if ("BOA".equals(bankKey)) {
            // Try "ending in XXXX" first, then "ending XXXX" (older templates),
            // then scan for any registered card's digits (Gmail search already filtered by them)
            cardDigits = extractGroup(BOA_CARD_DIGITS, body);
            if (cardDigits == null) cardDigits = extractGroup(BOA_CARD_DIGITS_ALT, body);
            if (cardDigits == null && msg.registeredCards() != null) {
                cardDigits = msg.registeredCards().stream()
                        .map(CardInfo::lastFour)
                        .filter(body::contains)
                        .findFirst().orElse(null);
            }
            amountStr  = extractGroup(AMOUNT, body);
            payDateStr = extractGroup(BOA_PAYMENT_DATE, body);
            effDateStr = null;
        } else if ("DISCOVER".equals(bankKey)) {
            cardDigits = extractGroup(DISCOVER_CARD_DIGITS, body);
            amountStr  = extractGroup(AMOUNT, body);
            payDateStr = extractGroup(DISCOVER_PAYMENT_DATE, body);
            effDateStr = null;
        } else if ("AMEX".equals(bankKey)) {
            cardDigits = extractGroup(AMEX_CARD_DIGITS, body);
            amountStr  = extractGroup(AMOUNT, body);
            payDateStr = extractGroup(AMEX_PAYMENT_DATE, body);
            effDateStr = null;
        } else {
            // Chase (default)
            cardDigits = extractGroup(CHASE_CARD_DIGITS, body);
            amountStr  = extractGroup(AMOUNT, body);
            payDateStr = extractGroup(CHASE_PAYMENT_DATE, body);
            effDateStr = extractGroup(CHASE_EFFECTIVE_DATE, body);
        }

        if (cardDigits == null || amountStr == null || payDateStr == null) {
            throw new RuntimeException("Missing required fields in " + bankKey + " payment email. " +
                    "cardDigits=" + cardDigits + " amount=" + amountStr + " paymentDate=" + payDateStr);
        }

        // Validate card digits against registered cards (suffix match)
        String lastFour = cardDigits.length() >= 4
                ? cardDigits.substring(cardDigits.length() - 4) : cardDigits;
        CardInfo matchedCard = msg.registeredCards() == null ? null :
                msg.registeredCards().stream()
                        .filter(c -> c.lastFour().endsWith(lastFour) || lastFour.endsWith(c.lastFour()))
                        .findFirst().orElse(null);

        if (matchedCard == null) {
            throw new RuntimeException("Card digits " + cardDigits +
                    " do not match any registered card — skipping payment email");
        }

        BigDecimal amount  = new BigDecimal(amountStr.replace(",", ""));
        LocalDate  payDate = parseDate(payDateStr);
        LocalDate  effDate = effDateStr != null ? parseDate(effDateStr) : payDate;

        return new ParseDone(matchedCard.lastFour(), amount, payDate, effDate, msg);
    }

    private Behavior<Command> onDone(ParseDone msg) {
        ExtractPayment src = msg.original();
        getContext().getLog().info(
                "Payment parsed from {} — card={} amount={} paymentDate={}",
                src.email().messageId(), msg.cardLastFour(), msg.amount(), msg.paymentDate());

        CompletableFuture.runAsync(() ->
                backendApiClient.postPayment(
                        src.userId(),
                        src.email().messageId(),
                        msg.cardLastFour(),
                        src.bankKey(),
                        msg.amount(),
                        msg.paymentDate(),
                        msg.effectiveDate()
                ), executor);

        return Behaviors.stopped();
    }

    private Behavior<Command> onFailed(ParseFailed msg) {
        getContext().getLog().warn("Payment parsing failed for {}: {}", msg.messageId(), msg.reason());
        return Behaviors.stopped();
    }

    // ── Helpers ───────────────────────────────────────────────────────────────

    private String toPlainText(String body) {
        if (body == null) return "";
        if (body.contains("<") && body.contains(">")) {
            return Jsoup.parse(body).text();
        }
        return body;
    }

    private String extractGroup(Pattern pattern, String text) {
        Matcher m = pattern.matcher(text);
        return m.find() ? m.group(1).trim() : null;
    }

    private LocalDate parseDate(String raw) {
        if (raw == null || raw.isBlank()) return null;
        // Normalize: remove comma, collapse spaces → "Mar 21 2026" or "February 12 2026"
        String normalized = raw.replace(",", "").replaceAll("\\s+", " ").trim();
        // Try abbreviated month (Chase) first, then full month name (BOA)
        for (DateTimeFormatter fmt : new DateTimeFormatter[]{DATE_FMT_SHORT, DATE_FMT_LONG}) {
            try {
                return LocalDate.parse(normalized, fmt);
            } catch (Exception ignored) {}
        }
        getContext().getLog().warn("Could not parse payment date '{}'", raw);
        return null;
    }
}
