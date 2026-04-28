# Runtime API Layering

These diagrams describe the current `langbridge/` runtime shape as implemented in the repo today.

## High-Level Layering

```mermaid
flowchart TD
    Client[SDK Client or HTTP Caller]

    subgraph Edge[Runtime Edge]
        FastAPI[FastAPI runtime host\nlangbridge.runtime.hosting.app]
        Auth[Auth resolver\nrequest-scoped RuntimeContext]
    end

    subgraph Host[Configured Runtime Host]
        ConfiguredHost[ConfiguredLocalRuntimeHost]
        Apps[Application layer\n datasets\n semantic\n sql\n agents\n threads\n connectors]
        UoW[Runtime operation scope\nUnit of Work]
    end

    subgraph Core[Runtime Core]
        RuntimeHost[RuntimeHost facade]
        Services[Service layer\n DatasetQueryService\n SemanticQueryExecutionService\n SemanticSqlQueryService\n SqlQueryService\n AgentExecutionService\n ConnectorSyncRuntime]
    end

    subgraph Infra[Providers and Persistence]
        Providers[Providers\n connector metadata\n dataset metadata\n semantic models\n credentials\n sync state]
        Persistence[Repositories and stores\n in-memory or SQL runtime metadata]
        Secrets[Secret provider registry]
    end

    subgraph Exec[Execution Plane]
        Resolver[DatasetExecutionResolver]
        FQ[FederatedQueryTool]
        Connectors[SQL connectors or file sources]
        Federation[Federation service]
    end

    Client --> FastAPI
    FastAPI --> Auth
    Auth --> ConfiguredHost
    ConfiguredHost --> Apps
    Apps --> UoW
    UoW --> RuntimeHost
    RuntimeHost --> Services
    Services --> Providers
    Services --> Persistence
    Services --> Secrets
    Services --> Resolver
    Resolver --> FQ
    FQ --> Connectors
    FQ --> Federation
```

## Bootstrap To Runtime Host

```mermaid
flowchart LR
    CLI[langbridge serve]
    Server[run_runtime_api]
    AppFactory[create_runtime_api_app]
    Config[load_runtime_config]
    Factory[ConfiguredLocalRuntimeHostFactory.build]
    RuntimeBuild[build RuntimeHost\nproviders plus services]
    Host[ConfiguredLocalRuntimeHost]

    CLI --> Server
    Server --> AppFactory
    AppFactory --> Factory
    Factory --> Config
    Factory --> RuntimeBuild
    RuntimeBuild --> Host
    Host --> AppFactory
```

## Request Path Through The API

```mermaid
sequenceDiagram
    participant C as Client
    participant A as FastAPI route
    participant R as RuntimeAuthResolver
    participant H as ConfiguredLocalRuntimeHost
    participant P as Domain application
    participant S as Runtime service
    participant X as Execution layer

    C->>A: HTTP request /api/runtime/v1/*
    A->>R: authenticate(request)
    R-->>A: RuntimeAuthPrincipal
    A->>R: build_context(...)
    R-->>A: RuntimeContext
    A->>H: with_context(context)
    A->>P: call application method
    P->>P: open runtime operation scope
    P->>S: delegate to service
    S->>X: execute query / sync / agent run
    X-->>S: rows / result / summary
    S-->>P: normalized payload
    P-->>A: response payload
    A-->>C: JSON response
```

## Dataset Preview Flow

```mermaid
flowchart TD
    Route[POST /api/runtime/v1/datasets/{dataset_ref}/preview]
    Resolve[Resolve dataset id and request context]
    Host[ConfiguredLocalRuntimeHost.query_dataset]
    App[DatasetApplication.query_dataset]
    Service[DatasetQueryService._run_preview]
    Bundle[Load dataset, columns, policy]
    Workflow[DatasetExecutionResolver.build_workflow_for_dataset]
    SQL[Build preview SQL with filters and row policy]
    Federated[FederatedQueryTool.execute_federated_query]
    Result[Apply redaction and shape preview response]

    Route --> Resolve
    Resolve --> Host
    Host --> App
    App --> Service
    Service --> Bundle
    Bundle --> Workflow
    Workflow --> SQL
    SQL --> Federated
    Federated --> Result
```

## Semantic Query Flow

```mermaid
flowchart TD
    Route[POST /api/runtime/v1/semantic/query]
    Host[ConfiguredLocalRuntimeHost.query_semantic_models]
    App[SemanticApplication.query_semantic_models]
    Normalize[Normalize members filters time dimensions order]
    Branch{Executable model or semantic graph}
    Standard[RuntimeHost.query_semantic]
    Graph[RuntimeHost.query_semantic_graph]
    StandardSvc[SemanticQueryExecutionService.execute_standard_query]
    GraphSvc[SemanticQueryExecutionService.execute_semantic_graph_query]
    GraphCompile[Compile semantic graph to executable SemanticModel]
    Compile[Compile semantic query to SQL]
    Workflow[Build semantic federation workflow]
    Execute[FederatedQueryTool.execute_federated_query]
    Response[Rows annotations metadata generated_sql]

    Route --> Host
    Host --> App
    App --> Normalize
    Normalize --> Branch
    Branch -->|executable SemanticModel| Standard
    Branch -->|configured graph or multi-model graph path| Graph
    Standard --> StandardSvc
    Graph --> GraphSvc
    StandardSvc --> Compile
    GraphSvc --> GraphCompile
    GraphCompile --> Compile
    StandardSvc --> Workflow
    GraphSvc --> Workflow
    Workflow --> Execute
    Compile --> Response
    Execute --> Response
```

## SQL Query Flow

```mermaid
flowchart TD
    Route[POST /api/runtime/v1/sql/query]
    Scope{query_scope}
    App[SqlApplication.query_sql]
    SemanticApp[SemanticApplication.query_semantic_sql]
    SemanticParse[SemanticSqlQueryService\nparse and build plan]
    SemanticExec[query_semantic_models existing path]
    JobPath[CreateSqlJobRequest]
    Host[RuntimeHost.execute_sql]
    Service[SqlQueryService.execute_sql]
    Branch{execution_mode}
    Single[SqlQueryService._execute_single]
    Federated[SqlQueryService._execute_federated]
    Connector[Connector SQL execution]
    Workflow[Resolve federated datasets and workflow]
    FQ[FederatedQueryTool.execute_federated_query]
    Response[Columns rows stats generated_sql]

    Route --> Scope
    Scope -->|semantic| App
    Scope -->|dataset| App
    Scope -->|source| App
    App -->|semantic| SemanticApp
    SemanticApp --> SemanticParse
    SemanticParse --> SemanticExec
    SemanticExec --> Response
    App -->|dataset or source| JobPath
    JobPath --> Host
    Host --> Service
    Service --> Branch
    Branch -->|single| Single
    Branch -->|federated| Federated
    Single --> Connector
    Federated --> Workflow
    Workflow --> FQ
    Connector --> Response
    FQ --> Response
```

## Agent Ask Flow

```mermaid
flowchart TD
    Route[POST /api/runtime/v1/agents/ask]
    Resolve[Resolve request context and target agent]
    Host[ConfiguredLocalRuntimeHost.ask_agent]
    App[AgentApplication.ask_agent]
    Thread[Create or load runtime thread]
    Message[Persist user message]
    Execute[RuntimeHost.create_agent]
    AgentSvc[AgentExecutionService.execute]
    Orchestrator[Orchestrator tools and agent runtime]
    Response[Thread id job id summary result visualization]

    Route --> Resolve
    Resolve --> Host
    Host --> App
    App --> Thread
    Thread --> Message
    Message --> Execute
    Execute --> AgentSvc
    AgentSvc --> Orchestrator
    Orchestrator --> Response
```

## SDK Access Modes

```mermaid
flowchart LR
    SDK[LangbridgeClient]
    Local[for_local_runtime / local]
    RuntimeHost[for_runtime_host / remote runtime-host autodetect]
    Legacy[for_remote_api]
    InProc[LocalRuntimeAdapter]
    HttpRuntime[RuntimeHostApiAdapter]
    HttpLegacy[RemoteApiAdapter]
    Runtime[ConfiguredLocalRuntimeHost]
    Api[/api/runtime/v1/*/]
    OlderApi[/api/v1/*/]

    SDK --> Local
    SDK --> RuntimeHost
    SDK --> Legacy
    Local --> InProc
    RuntimeHost --> HttpRuntime
    Legacy --> HttpLegacy
    InProc --> Runtime
    HttpRuntime --> Api
    HttpLegacy --> OlderApi
```
