function objectValue(value) {
  return value && typeof value === "object" && !Array.isArray(value) ? value : null;
}

function arrayValue(value) {
  return Array.isArray(value) ? value : [];
}

function textValue(value) {
  return String(value ?? "").trim();
}

function labelize(value) {
  return textValue(value)
    .replaceAll("_", " ")
    .replaceAll("-", " ")
    .replace(/\s+/g, " ")
    .trim();
}

function toTitle(value) {
  const label = labelize(value);
  return label ? label.replace(/\b\w/g, (match) => match.toUpperCase()) : "";
}

function numberValue(value) {
  const parsed = Number(value);
  return Number.isFinite(parsed) ? parsed : null;
}

function compact(items) {
  return items.filter((item) => item !== null && item !== undefined && textValue(item));
}

function readExecution(diagnostics) {
  return objectValue(diagnostics?.execution);
}

function readAiRun(diagnostics) {
  return objectValue(diagnostics?.ai_run);
}

function readRouteDecision(diagnostics) {
  const aiRun = readAiRun(diagnostics);
  return (
    objectValue(aiRun?.diagnostics?.route_decision) ||
    objectValue(diagnostics?.route_decision) ||
    objectValue(readExecution(diagnostics)?.route_decision)
  );
}

function readIntentDecision(diagnostics) {
  const aiRun = readAiRun(diagnostics);
  return (
    objectValue(aiRun?.diagnostics?.intent) ||
    objectValue(diagnostics?.intent) ||
    objectValue(readExecution(diagnostics)?.intent)
  );
}

function readAgentSelection(diagnostics) {
  const aiRun = readAiRun(diagnostics);
  return (
    objectValue(diagnostics?.agent_selection) ||
    objectValue(readExecution(diagnostics)?.agent_selection) ||
    objectValue(aiRun?.diagnostics?.agent_selection)
  );
}

function readFinalReview(execution, diagnostics) {
  return (
    objectValue(execution?.reviews?.final_review) ||
    objectValue(readAiRun(diagnostics)?.diagnostics?.final_review) ||
    objectValue(diagnostics?.final_review)
  );
}

function readVerificationItems(execution, diagnostics) {
  const executionItems = arrayValue(execution?.reviews?.verification);
  return executionItems.length > 0 ? executionItems : arrayValue(readAiRun(diagnostics)?.verification);
}

function readPlanReviewItems(execution, diagnostics) {
  const executionItems = arrayValue(execution?.reviews?.plan_review_decisions);
  return executionItems.length > 0 ? executionItems : arrayValue(readAiRun(diagnostics)?.review_decisions);
}

function readPlanSteps(execution, diagnostics) {
  const executionItems = arrayValue(execution?.plan_steps);
  return executionItems.length > 0 ? executionItems : arrayValue(readAiRun(diagnostics)?.plan?.steps);
}

function readStepResults(execution, diagnostics) {
  const executionItems = arrayValue(execution?.step_results);
  return executionItems.length > 0 ? executionItems : arrayValue(readAiRun(diagnostics)?.step_results);
}

function readSqlItems(execution, diagnostics) {
  return arrayValue(execution?.sql).length > 0
    ? arrayValue(execution?.sql)
    : arrayValue(diagnostics?.sql);
}

function readInvestigationTrace(execution, diagnostics) {
  const directTrace = arrayValue(diagnostics?.investigation_trace);
  if (directTrace.length > 0) {
    return directTrace;
  }
  const items = [];
  readStepResults(execution, diagnostics).forEach((step) => {
    const stepTrace = arrayValue(step?.diagnostics?.investigation_trace);
    stepTrace.forEach((item) => items.push(item));
  });
  return items;
}

function statusTone(value, fallback = "neutral") {
  const status = textValue(value).toLowerCase();
  if (["success", "succeeded", "completed", "passed", "approve", "approved", "finalize"].includes(status)) {
    return "success";
  }
  if (["failed", "failure", "error", "abort", "aborted", "rejected"].includes(status)) {
    return "danger";
  }
  if (["clarification", "clarification_needed", "needs_clarification", "warning", "revise_plan", "replan"].includes(status)) {
    return "warning";
  }
  if (["running", "pending", "in_progress", "processing"].includes(status)) {
    return "active";
  }
  return fallback;
}

function buildEvidenceSummary(execution) {
  const evidence = objectValue(execution?.evidence);
  if (!evidence) {
    return "";
  }
  const parts = [];
  if (evidence.governed_attempted) {
    parts.push(`${numberValue(evidence.governed_rounds) ?? 0} governed round(s)`);
  }
  const externalSources = numberValue(evidence.external_sources);
  if (externalSources && externalSources > 0) {
    parts.push(`${externalSources} external source(s)`);
  }
  if (evidence.used_fallback) {
    parts.push("fallback used");
  }
  return parts.join(" | ");
}

function buildSummary({ diagnostics, execution, routeDecision, agentSelection, sqlItems }) {
  const aiRun = readAiRun(diagnostics);
  const finalReview = readFinalReview(execution, diagnostics);
  const verificationItems = readVerificationItems(execution, diagnostics);
  const failedVerification = verificationItems.some((item) => item?.passed === false);
  const passedVerification = verificationItems.length > 0 && !failedVerification;
  const rowCount =
    numberValue(execution?.rowcount) ??
    numberValue(diagnostics?.rowcount) ??
    numberValue(sqlItems[sqlItems.length - 1]?.rowcount);
  const sqlCount =
    numberValue(execution?.total_sql_queries) ??
    (sqlItems.length > 0 ? sqlItems.length : null);
  const finalReviewAction = textValue(finalReview?.action);
  const checksStatus = failedVerification
    ? "Verification failed"
    : passedVerification
      ? finalReviewAction
        ? `Verified, ${labelize(finalReviewAction)}`
        : "Verified"
      : finalReviewAction
        ? toTitle(finalReviewAction)
        : "";

  return {
    status: textValue(execution?.status || aiRun?.status || diagnostics?.status),
    route: textValue(execution?.route || aiRun?.route),
    executionMode: textValue(execution?.execution_mode || aiRun?.execution_mode),
    selectedAgent: textValue(
      execution?.selected_agent ||
        aiRun?.diagnostics?.selected_agent ||
        routeDecision?.agent_name ||
        agentSelection?.agent_name,
    ),
    stopReason: textValue(execution?.stop_reason || aiRun?.diagnostics?.stop_reason),
    iterations: numberValue(execution?.iterations || aiRun?.diagnostics?.iterations),
    replanCount: numberValue(execution?.replan_count || aiRun?.diagnostics?.replan_count),
    sqlCount,
    rowCount,
    queryScopes: arrayValue(execution?.query_scopes).map(textValue).filter(Boolean),
    evidence: buildEvidenceSummary(execution),
    checksStatus,
    tone: statusTone(execution?.status || aiRun?.status || diagnostics?.status),
  };
}

function buildTraceFlowItems(traceItems) {
  return traceItems
    .filter((item) => item && typeof item === "object")
    .map((item, index) => {
      const status = textValue(item.status);
      const rowCount = numberValue(item.rowcount);
      const meta = compact([
        item.type ? toTitle(item.type) : "",
        status ? toTitle(status) : "",
        item.query_scope,
        rowCount !== null ? `${rowCount.toLocaleString()} rows` : "",
        item.recommended_chart_type ? `${item.recommended_chart_type} chart` : "",
      ]).join(" | ");
      return {
        id: textValue(item.id) || `investigation-${index + 1}`,
        type: textValue(item.type) || "investigation",
        tone: statusTone(status, "neutral"),
        title: textValue(item.title) || `Investigation step ${index + 1}`,
        description: textValue(item.summary || item.rationale || item.evidence_goal),
        meta,
      };
    });
}

function buildFlowItems({ diagnostics, execution, routeDecision, intentDecision, agentSelection, investigationTrace }) {
  const items = [];

  if (agentSelection) {
    const action = textValue(agentSelection.action);
    const confidence = numberValue(agentSelection.confidence);
    const candidateCount = numberValue(agentSelection.candidate_count);
    items.push({
      id: "agent-selection",
      type: "agent-selection",
      tone: statusTone(action === "clarify" ? "clarification" : action || "completed", "active"),
      title:
        action === "select" && agentSelection.agent_name
          ? `Auto selected ${agentSelection.agent_name}`
          : action
            ? `Auto ${labelize(action)}`
            : "Auto agent selection",
      description: textValue(agentSelection.rationale || agentSelection.clarification_question),
      meta: compact([
        candidateCount !== null ? `${candidateCount} candidate(s)` : "",
        confidence !== null ? `${Math.round(confidence * 100)}% confidence` : "",
      ]).join(" | "),
    });
  }

  if (routeDecision) {
    const action = textValue(routeDecision.action);
    items.push({
      id: "route-decision",
      type: "route",
      tone: statusTone(action, "active"),
      title: routeDecision.agent_name
        ? `Routed to ${routeDecision.agent_name}`
        : action
          ? `Route: ${toTitle(action)}`
          : "Route selected",
      description: textValue(routeDecision.rationale || routeDecision.clarification_question),
      meta: compact([routeDecision.task_kind, routeDecision.input?.agent_mode ? `Mode ${routeDecision.input.agent_mode}` : ""]).join(" | "),
    });
  } else if (intentDecision) {
    const intent = textValue(intentDecision.intent);
    const action = textValue(intentDecision.action);
    const confidence = numberValue(intentDecision.confidence);
    items.push({
      id: "intent-decision",
      type: "intent",
      tone: statusTone(action === "clarify" ? "clarification" : "completed", "active"),
      title: intent ? `Intent: ${toTitle(intent)}` : "Intent classified",
      description: textValue(intentDecision.rationale || intentDecision.clarification_question),
      meta: compact([
        action ? toTitle(action) : "",
        confidence !== null ? `${Math.round(confidence * 100)}% confidence` : "",
      ]).join(" | "),
    });
  } else if (execution?.route || execution?.selected_agent) {
    items.push({
      id: "route",
      type: "route",
      tone: "active",
      title: execution.selected_agent ? `Routed to ${execution.selected_agent}` : `Route: ${execution.route}`,
      description: textValue(execution.summary),
      meta: compact([execution.execution_mode, execution.stop_reason]).join(" | "),
    });
  }

  const traceItems = buildTraceFlowItems(investigationTrace);
  if (traceItems.length > 0) {
    items.push(...traceItems);
    const finalReview = readFinalReview(execution, diagnostics);
    if (finalReview) {
      items.push({
        id: "final-review",
        type: "review",
        tone: statusTone(finalReview.action || finalReview.reason_code),
        title: finalReview.action ? `Final review: ${toTitle(finalReview.action)}` : "Final review",
        description: textValue(finalReview.rationale || finalReview.reason_code),
        meta: textValue(finalReview.reason_code),
      });
    }
    return items;
  }

  readPlanSteps(execution, diagnostics).forEach((step, index) => {
    items.push({
      id: `plan-${step.step_id || index}`,
      type: "plan",
      tone: "neutral",
      title: step.agent_name ? `Planned ${step.agent_name}` : `Plan step ${index + 1}`,
      description: textValue(step.question || step.task_kind),
      meta: compact([step.step_id, step.task_kind]).join(" | "),
    });
  });

  readStepResults(execution, diagnostics).forEach((step, index) => {
    const modeReason = textValue(step?.diagnostics?.mode_decision?.reason);
    const rowCount = numberValue(step.rowcount);
    items.push({
      id: `result-${step.task_id || step.step_id || index}`,
      type: "execute",
      tone: statusTone(step.status || step.outcome_status),
      title: step.agent_name ? `${step.agent_name} executed` : `Execution ${index + 1}`,
      description:
        modeReason ||
        textValue(step.outcome_message || step.error) ||
        (rowCount !== null ? `Returned ${rowCount.toLocaleString()} row(s).` : textValue(step.status)),
      meta: compact([step.query_scope, step.analysis_path, rowCount !== null ? `${rowCount.toLocaleString()} rows` : ""]).join(" | "),
    });
  });

  const finalReview = readFinalReview(execution, diagnostics);
  if (finalReview) {
    items.push({
      id: "final-review",
      type: "review",
      tone: statusTone(finalReview.action || finalReview.reason_code),
      title: finalReview.action ? `Final review: ${toTitle(finalReview.action)}` : "Final review",
      description: textValue(finalReview.rationale || finalReview.reason_code),
      meta: textValue(finalReview.reason_code),
    });
  }

  if (items.length === 0 && execution?.summary) {
    items.push({
      id: "summary",
      type: "summary",
      tone: "neutral",
      title: "Runtime summary",
      description: textValue(execution.summary),
      meta: "",
    });
  }

  return items;
}

function buildQueryItems(sqlItems) {
  return sqlItems
    .filter((item) => item && typeof item === "object")
    .map((item, index) => {
      const rowCount = numberValue(item.rowcount);
      const status = textValue(item.status);
      const canonicalSql = textValue(item.sql_canonical);
      const executableSql = textValue(item.sql_executable);
      return {
        id: textValue(item.task_id || item.step_id || `query-${index + 1}`),
        title: item.round_index ? `Round ${item.round_index}` : `Query ${index + 1}`,
        tone: statusTone(status),
        status,
        scope: textValue(item.query_scope),
        stage: textValue(item.stage),
        agentName: textValue(item.agent_name),
        toolName: textValue(item.tool_name || item.selected_tool),
        rowCount,
        message: textValue(item.message || item.error),
        roundQuestion: textValue(item.round_question),
        canonicalSql,
        executableSql,
        selectedDatasets: arrayValue(item.selected_datasets).map(textValue).filter(Boolean),
        selectedSemanticModels: arrayValue(item.selected_semantic_models).map(textValue).filter(Boolean),
        rowsSample: arrayValue(item.rows_sample),
        usedFallback: Boolean(item.used_fallback),
      };
    })
    .filter((item) => item.canonicalSql || item.executableSql || item.message);
}

function buildCheckItems({ diagnostics, execution }) {
  const checks = [];

  readVerificationItems(execution, diagnostics).forEach((item, index) => {
    checks.push({
      id: `verification-${item.step_id || index}`,
      group: "Verification",
      tone: item.passed === false ? "danger" : "success",
      title: item.passed === false ? "Verification failed" : "Verification passed",
      description: textValue(item.message || item.reason_code),
      meta: compact([item.step_id, item.agent_name, item.reason_code]).join(" | "),
    });
  });

  readPlanReviewItems(execution, diagnostics).forEach((item, index) => {
    checks.push({
      id: `plan-review-${index}`,
      group: "Plan review",
      tone: statusTone(item.action || item.reason_code),
      title: item.action ? toTitle(item.action) : "Plan review",
      description: textValue(item.rationale || item.reason_code),
      meta: textValue(item.reason_code),
    });
  });

  const finalReview = readFinalReview(execution, diagnostics);
  if (finalReview) {
    checks.push({
      id: "final-review",
      group: "Final review",
      tone: statusTone(finalReview.action || finalReview.reason_code),
      title: finalReview.action ? toTitle(finalReview.action) : "Final review",
      description: textValue(finalReview.rationale || finalReview.reason_code),
      meta: textValue(finalReview.reason_code),
    });
  }

  return checks;
}

export function buildRunInspectorModel(diagnostics) {
  const source = objectValue(diagnostics);
  if (!source) {
    return null;
  }

  const execution = readExecution(source);
  const intentDecision = readIntentDecision(source);
  const agentSelection = readAgentSelection(source);
  const routeDecisionRaw = readRouteDecision(source);
  const routeAction = textValue(routeDecisionRaw?.action).toLowerCase();
  const routeDecision =
    intentDecision && ["respond", "clarify"].includes(routeAction)
      ? null
      : routeDecisionRaw;
  const sqlItems = readSqlItems(execution, source);
  const investigationTrace = readInvestigationTrace(execution, source);

  return {
    summary: buildSummary({ diagnostics: source, execution, routeDecision, agentSelection, sqlItems }),
    flowItems: buildFlowItems({
      diagnostics: source,
      execution,
      routeDecision,
      intentDecision,
      agentSelection,
      investigationTrace,
    }),
    queryItems: buildQueryItems(sqlItems),
    checkItems: buildCheckItems({ diagnostics: source, execution }),
    raw: source,
  };
}

export function formatInspectorLabel(value) {
  return toTitle(value) || "n/a";
}
