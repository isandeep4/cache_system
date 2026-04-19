# cache_system
graph TD
    User((User/Client)) -->|1. Request Data| API[API Gateway / App Server]
    
    subgraph Cache_Layer [Cache System]
        API -->|2. Check Cache| Redis{Redis Cache}
        Redis -->|3. Cache Hit O:1| API
    end

    subgraph Persistence_Layer [Data Source]
        Redis -.->|4. Cache Miss| DB[(Primary Database)]
        DB -.->|5. Fetch Data| API
        API -.->|6. Update Cache| Redis
    end

    style Redis fill:#f96,stroke:#333,stroke-width:2px
    style DB fill:#69f,stroke:#333,stroke-width:2px
    style API fill:#fff,stroke:#333,stroke-width:2px
