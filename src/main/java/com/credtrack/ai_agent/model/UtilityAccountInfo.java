package com.credtrack.ai_agent.model;

/**
 * A user's registered utility account — returned by
 * GET /internal/utility-accounts and used by UtilityBillCoordinatorActor
 * to know which Gmail mailboxes to search and whether to run the init scan.
 */
public record UtilityAccountInfo(
        Long   id,                    // database PK — used when marking init complete
        String userId,
        String billerName,            // EVERSOURCE or NATIONAL_GRID
        String accountLastFour,
        boolean utilityInitComplete   // false → run init scan; true → normal incremental poll
) {}
