# CollabDocs

A real-time collaborative document editor. Multiple people can write in the same document at once, with changes synced live.

### How it was built

The client is a React + TypeScript app (Vite) with a ProseMirror rich-text editor. The server is Node.js + TypeScript. Live edits go over WebSockets (`ws`), ordered by a server-authoritative Operational Transformation engine. Mutation events are published asynchronously to Apache Kafka (`kafkajs`) and persisted in Apache Cassandra (`cassandra-driver`). Everything runs locally with Docker Compose. The GitHub Pages demo uses a BroadcastChannel multi-tab fallback so collaboration works without a hosted backend.

### Live demo

https://adclintoncodes.github.io/Real-Time-Collaborative-Editor/

Open the link in two browser tabs to edit together.

### How to run

You need Node.js 20+ and Docker.

```bash
npm install
docker compose up -d kafka cassandra cassandra-init
./scripts/wait-for-infra.sh
npm run dev:server
```

In another terminal:

```bash
VITE_WS_URL=ws://localhost:3001/ws npm run dev:client
```

Then open http://localhost:5173.
