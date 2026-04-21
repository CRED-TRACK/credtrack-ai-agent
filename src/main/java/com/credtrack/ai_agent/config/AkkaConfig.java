package com.credtrack.ai_agent.config;

import akka.actor.typed.ActorRef;
import akka.actor.typed.ActorSystem;
import akka.actor.typed.Props;
import akka.actor.typed.SpawnProtocol;
import akka.actor.typed.SupervisorStrategy;
import akka.actor.typed.javadsl.AskPattern;
import akka.actor.typed.javadsl.Behaviors;
import com.credtrack.ai_agent.actor.AnalyticsAgentHolder;
import com.credtrack.ai_agent.actor.AnalyticsAggregatorActor;
import com.credtrack.ai_agent.actor.EmailPipelineCoordinator;
import com.credtrack.ai_agent.actor.PersistentStatementLedgerActor;
import com.credtrack.ai_agent.service.BackendApiClient;
import com.credtrack.ai_agent.service.ExtractionService;
import com.credtrack.ai_agent.service.GmailService;
import org.springframework.beans.factory.annotation.Value;
import org.springframework.context.annotation.Bean;
import org.springframework.context.annotation.Configuration;

import java.time.Duration;
import java.util.concurrent.Executor;
import java.util.concurrent.Executors;

@Configuration
public class AkkaConfig {

    @Value("${transaction.scan.interval-minutes:1440}")
    private long transactionScanIntervalMinutes;

    /**
     * Virtual-thread-per-task executor used for all blocking I/O inside actors
     * (Gmail API, Ollama, backend REST calls).
     *
     * Java 21+ virtual threads are extremely cheap — each blocking call gets its
     * own virtual thread and the platform thread is never blocked, so we can have
     * hundreds of concurrent Ollama/Gmail calls without saturating the ForkJoinPool.
     */
    @Bean
    public Executor virtualThreadExecutor() {
        return Executors.newVirtualThreadPerTaskExecutor();
    }

    /**
     * Root ActorSystem using SpawnProtocol so we can spawn top-level actors
     * as Spring beans via AskPattern.
     */
    @Bean
    public ActorSystem<SpawnProtocol.Command> actorSystem() {
        return ActorSystem.create(SpawnProtocol.create(), "credtrack-ai");
    }

    /**
     * PersistentStatementLedgerActor — top-level so both the email pipeline
     * (writes) and AnalyticsAggregatorActor (reads via Ask) share the same instance.
     * Supervised with restartWithBackoff so it replays its journal on failure.
     */
    @Bean
    public ActorRef<PersistentStatementLedgerActor.Command> statementLedger(
            ActorSystem<SpawnProtocol.Command> system) {

        return AskPattern.<SpawnProtocol.Command, ActorRef<PersistentStatementLedgerActor.Command>>ask(
                system,
                replyTo -> new SpawnProtocol.Spawn<>(
                        Behaviors.supervise(PersistentStatementLedgerActor.create())
                                 .onFailure(SupervisorStrategy.restartWithBackoff(
                                         Duration.ofSeconds(2), Duration.ofSeconds(60), 0.1)),
                        "statement-ledger",
                        Props.empty(),
                        replyTo),
                Duration.ofSeconds(5),
                system.scheduler()
        ).toCompletableFuture().join();
    }

    /**
     * AnalyticsAgentHolder — AtomicReference-backed Akka Agents substitute.
     * Spawned before EmailPipelineCoordinator so its ref can be injected.
     */
    @Bean
    public ActorRef<AnalyticsAgentHolder.Command> analyticsAgentHolder(
            ActorSystem<SpawnProtocol.Command> system) {

        return AskPattern.<SpawnProtocol.Command, ActorRef<AnalyticsAgentHolder.Command>>ask(
                system,
                replyTo -> new SpawnProtocol.Spawn<>(
                        AnalyticsAgentHolder.create(),
                        "analytics-agent-holder",
                        Props.empty(),
                        replyTo),
                Duration.ofSeconds(5),
                system.scheduler()
        ).toCompletableFuture().join();
    }

    /**
     * EmailPipelineCoordinator exposed as a Spring bean so the scheduler
     * can inject it directly and send Poll messages.
     */
    @Bean
    public ActorRef<EmailPipelineCoordinator.Command> emailPipelineCoordinator(
            ActorSystem<SpawnProtocol.Command> system,
            BackendApiClient backendApiClient,
            GmailService gmailService,
            ExtractionService extractionService,
            Executor virtualThreadExecutor,
            ActorRef<PersistentStatementLedgerActor.Command> statementLedger,
            ActorRef<AnalyticsAgentHolder.Command> analyticsAgentHolder) {

        return AskPattern.<SpawnProtocol.Command, ActorRef<EmailPipelineCoordinator.Command>>ask(
                system,
                replyTo -> new SpawnProtocol.Spawn<>(
                        EmailPipelineCoordinator.create(
                                backendApiClient, gmailService, extractionService,
                                virtualThreadExecutor, statementLedger, analyticsAgentHolder,
                                transactionScanIntervalMinutes),
                        "email-pipeline-coordinator",
                        Props.empty(),
                        replyTo),
                Duration.ofSeconds(5),
                system.scheduler()
        ).toCompletableFuture().join();
    }

    /**
     * AnalyticsAggregatorActor — uses Ask pattern to query the ledger,
     * then calls the backend to sum unbilled transactions,
     * then updates AnalyticsAgentHolder with the result.
     */
    @Bean
    public ActorRef<AnalyticsAggregatorActor.Command> analyticsAggregator(
            ActorSystem<SpawnProtocol.Command> system,
            ActorRef<PersistentStatementLedgerActor.Command> statementLedger,
            BackendApiClient backendApiClient,
            Executor virtualThreadExecutor,
            ActorRef<AnalyticsAgentHolder.Command> analyticsAgentHolder) {

        return AskPattern.<SpawnProtocol.Command, ActorRef<AnalyticsAggregatorActor.Command>>ask(
                system,
                replyTo -> new SpawnProtocol.Spawn<>(
                        AnalyticsAggregatorActor.create(statementLedger, backendApiClient,
                                virtualThreadExecutor, analyticsAgentHolder),
                        "analytics-aggregator",
                        Props.empty(),
                        replyTo),
                Duration.ofSeconds(5),
                system.scheduler()
        ).toCompletableFuture().join();
    }
}
