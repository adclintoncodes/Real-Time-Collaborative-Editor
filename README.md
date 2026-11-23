# CollabDocs — Real-Time Collaborative Document Editor

**One document. Multiple minds. Server-authoritative OT.**

A full-stack collaborative rich-text editor inspired by Google Docs. Concurrent edits are ordered by a server-side Operational Transformation (OT) engine, streamed over WebSockets, published asynchronously to Apache Kafka, and persisted in Apache Cassandra — without blocking the real-time path.

### Live demo

**https://adclintoncodes.github.io/Real-Time-Collaborative-Editor/**

Open the URL in two browser tabs to collaborate instantly. When no backend WebSocket is configured (`VITE_WS_URL` unset), the client uses a `BroadcastChannel` multi-tab fallback so the GitHub Pages demo works without a hosted server.

> **One-time setup:** In [Settings → Pages](https://github.com/adclintoncodes/Real-Time-Collaborative-Editor/settings/pages), set **Source** to **GitHub Actions** (or Deploy from branch `gh-pages` / `/`). Then re-run the [Deploy workflow](https://github.com/adclintoncodes/Real-Time-Collaborative-Editor/actions/workflows/deploy.yml).

### Architecture

```
┌─────────────┐   WebSocket / BroadcastChannel   ┌──────────────────────┐
│ React +     │ ◄──────────────────────────────► │ Node.js WS Gateway   │
│ ProseMirror │   optimistic ops + presence      │ JWT · rooms · OT     │
└─────────────┘                                  └──────────┬───────────┘
                                                            │ async
                                                            ▼
                                                 ┌──────────────────────┐
                                                 │ Apache Kafka         │
                                                 │ topic: doc-mutations │
                                                 └──────────┬───────────┘
                                                            │ batch consume
                                                            ▼
                                                 ┌──────────────────────┐
                                                 │ Apache Cassandra     │
                                                 │ keyspace: collab_docs│
                                                 └──────────────────────┘
```

| Layer | Technology |
| --- | --- |
| Client | React, TypeScript, Vite, ProseMirror |
| Realtime | `ws` WebSockets, JWT auth, presence |
| Consistency | Server-authoritative OT (`server/src/ot/otEngine.ts`) |
| Streaming | Apache Kafka (KRaft, no Zookeeper) via `kafkajs` |
| Persistence | Apache Cassandra via `cassandra-driver` |
| Packaging | npm workspaces monorepo, Docker Compose, multi-stage server image |

**Not used:** Amazon S3, Firebase, Supabase, or other third-party BaaS.

### Key features

- Live multi-user editing with sub-50ms OT target latency on the hot path
- Room isolation per `documentId`, heartbeat pings, token-bucket rate limiting
- Optimistic local updates + offline operation queue with reconnect flush
- Remote collaborator cursors / selections (`PresenceBar`)
- Decoupled Kafka consumer writing revisions & snapshots to Cassandra
- REST API: `POST /api/docs`, `GET /api/docs/:id`
- GitHub Pages demo with BroadcastChannel multi-tab collaboration

### Local setup

**Prerequisites:** Node.js 20+, Docker / Docker Compose.

```bash
# 1. Install workspace dependencies
npm install

# 2. Start Kafka (KRaft) + Cassandra and apply schema
docker compose up -d kafka cassandra cassandra-init
./scripts/wait-for-infra.sh

# 3. Run the API + WebSocket gateway (also starts the metadata consumer)
npm run dev:server

# 4. In another terminal — run the Vite client
#    Point the client at the local gateway:
#    VITE_WS_URL=ws://localhost:3001/ws npm run dev:client
VITE_WS_URL=ws://localhost:3001/ws npm run dev:client
```

Optional: run the full stack including the server container:

```bash
docker compose --profile full up -d --build
```

### Tests

```bash
# OT race-condition unit tests
npm run test --workspace=server

# Integration tests against Docker Kafka + Cassandra
npm run test:integration --workspace=server

# Client production build (TypeScript + Vite)
npm run build --workspace=client
```

### Repository layout

```
/client   Vite + React + ProseMirror editor
/server   Express REST, WebSocket gateway, OT, Kafka, Cassandra
docker-compose.yml
.github/workflows/deploy.yml   → GitHub Pages
```

### Author

Clinton Adorsoo
