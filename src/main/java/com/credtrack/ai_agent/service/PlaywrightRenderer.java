package com.credtrack.ai_agent.service;

import com.microsoft.playwright.Browser;
import com.microsoft.playwright.BrowserContext;
import com.microsoft.playwright.BrowserType;
import com.microsoft.playwright.Page;
import com.microsoft.playwright.Playwright;
import com.microsoft.playwright.options.WaitUntilState;
import jakarta.annotation.PreDestroy;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.beans.factory.annotation.Value;
import org.springframework.stereotype.Service;

import java.util.List;

/**
 * Lazy-init headless Chromium for JS-rendered issuer pages (Amex Cloudflare shell,
 * Discover quarterly calendar, etc). Only invoked by RewardTermsScraper when the
 * plain-HTTP fetch returns thin content.
 *
 * Single shared Browser instance; new BrowserContext per render call for isolation.
 * Disabled entirely when playwright.enabled=false (default true on prod, false in
 * tests/local runs where binaries are not installed).
 */
@Service
public class PlaywrightRenderer {

    private static final Logger log = LoggerFactory.getLogger(PlaywrightRenderer.class);

    private final boolean enabled;
    private final int navTimeoutMs;
    private final String userAgent;

    private volatile Playwright pw;
    private volatile Browser browser;

    public PlaywrightRenderer(
            @Value("${playwright.enabled:true}") boolean enabled,
            @Value("${playwright.nav-timeout-ms:30000}") int navTimeoutMs,
            @Value("${playwright.user-agent:Mozilla/5.0 (Macintosh; Intel Mac OS X 10_15_7) AppleWebKit/605.1.15 (KHTML, like Gecko) Version/17.0 Safari/605.1.15}")
            String userAgent) {
        this.enabled = enabled;
        this.navTimeoutMs = navTimeoutMs;
        this.userAgent = userAgent;
    }

    public boolean isEnabled() { return enabled; }

    /**
     * Returns rendered HTML (post-JS) or null if Playwright is disabled or the render fails.
     * Lazy-launches the browser on first call.
     */
    public synchronized String renderHtml(String url) {
        if (!enabled) {
            log.debug("playwright disabled — skipping render for {}", url);
            return null;
        }
        try {
            ensureBrowser();
        } catch (Throwable t) {
            log.error("playwright_event=init_failed error={}", t.getMessage());
            return null;
        }
        BrowserContext ctx = null;
        try {
            ctx = browser.newContext(new Browser.NewContextOptions()
                    .setUserAgent(userAgent)
                    .setViewportSize(1280, 800)
                    .setExtraHTTPHeaders(java.util.Map.of(
                            "Accept-Language", "en-US,en;q=0.9")));
            Page page = ctx.newPage();
            page.navigate(url, new Page.NavigateOptions()
                    .setWaitUntil(WaitUntilState.DOMCONTENTLOADED)
                    .setTimeout(navTimeoutMs));
            // Give async JS a chance to settle. NETWORKIDLE is often too strict for
            // analytics-heavy issuer pages — use a short fixed wait instead.
            try { Thread.sleep(2000); } catch (InterruptedException ignored) { Thread.currentThread().interrupt(); }
            return page.content();
        } catch (Exception e) {
            log.warn("playwright_event=render_failed url={} error={}", url, e.getMessage());
            return null;
        } finally {
            if (ctx != null) try { ctx.close(); } catch (Exception ignored) {}
        }
    }

    private void ensureBrowser() {
        if (browser != null) return;
        log.info("playwright_event=launching headless=true");
        pw = Playwright.create();
        browser = pw.chromium().launch(new BrowserType.LaunchOptions()
                .setHeadless(true)
                .setArgs(List.of("--no-sandbox", "--disable-dev-shm-usage")));
        log.info("playwright_event=launched version={}", browser.version());
    }

    @PreDestroy
    public void shutdown() {
        try { if (browser != null) browser.close(); } catch (Exception e) { log.warn("playwright close browser: {}", e.getMessage()); }
        try { if (pw != null) pw.close(); } catch (Exception e) { log.warn("playwright close runtime: {}", e.getMessage()); }
    }
}
