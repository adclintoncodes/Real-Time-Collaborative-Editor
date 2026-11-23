import { useMemo, useState } from 'react';
import Editor from './editor/Editor';

function randomId(prefix: string): string {
  return `${prefix}-${Math.random().toString(36).slice(2, 10)}`;
}

export default function App() {
  const clientId = useMemo(() => randomId('client'), []);
  const [documentId] = useState(() => {
    const params = new URLSearchParams(window.location.search);
    return params.get('doc') ?? 'demo-doc';
  });
  const [name, setName] = useState(() => `Guest ${clientId.slice(-4)}`);

  return (
    <div className="app-shell">
      <header className="app-header">
        <div className="brand-block">
          <h1 className="brand">CollabDocs</h1>
          <p className="tagline">Real-time collaborative writing with operational transformation.</p>
        </div>
        <label className="name-field">
          <span>Your name</span>
          <input
            value={name}
            onChange={(e) => setName(e.target.value)}
            maxLength={40}
            aria-label="Display name"
          />
        </label>
      </header>
      <main className="app-main">
        <Editor documentId={documentId} clientId={clientId} displayName={name || 'Anonymous'} />
      </main>
      <footer className="app-footer">
        <span>Document: {documentId}</span>
        <span>Share this URL to collaborate</span>
      </footer>
    </div>
  );
}
