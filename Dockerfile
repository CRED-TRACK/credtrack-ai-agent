FROM --platform=linux/amd64 eclipse-temurin:21-jdk-jammy AS build
WORKDIR /app
COPY .mvn/ .mvn/
COPY mvnw pom.xml ./
RUN ./mvnw dependency:go-offline -B
COPY src ./src
RUN ./mvnw package -DskipTests -B

# Install Playwright Chromium into a known cache dir so the runtime image can copy it.
ENV PLAYWRIGHT_BROWSERS_PATH=/ms-playwright
RUN mkdir -p /ms-playwright \
    && PW_JAR=$(find /root/.m2/repository/com/microsoft/playwright -name 'playwright-*.jar' | grep -v javadoc | grep -v sources | head -1) \
    && echo "playwright jar: $PW_JAR" \
    && java -cp "$PW_JAR" com.microsoft.playwright.CLI install chromium

FROM --platform=linux/amd64 eclipse-temurin:21-jre-jammy
WORKDIR /app

# Chromium runtime system libs (mirror of Playwright's install-deps for chromium on jammy).
RUN apt-get update && apt-get install -y --no-install-recommends \
        libnss3 libnspr4 libatk1.0-0 libatk-bridge2.0-0 libcups2 libdrm2 \
        libxkbcommon0 libxcomposite1 libxdamage1 libxfixes3 libxrandr2 \
        libgbm1 libpango-1.0-0 libcairo2 libasound2 fonts-liberation \
        libxshmfence1 libx11-xcb1 \
    && apt-get clean && rm -rf /var/lib/apt/lists/*

COPY --from=build /app/target/*.jar app.jar
COPY --from=build /ms-playwright /ms-playwright
ENV PLAYWRIGHT_BROWSERS_PATH=/ms-playwright

RUN groupadd -r spring && useradd -r -g spring spring \
    && chown -R spring:spring /app /ms-playwright
USER spring
EXPOSE 8081
ENTRYPOINT ["java", "-jar", "app.jar"]
