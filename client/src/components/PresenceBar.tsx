export interface Collaborator {
  clientId: string;
  userId: string;
  userName: string;
  color: string;
}

interface PresenceBarProps {
  peers: Collaborator[];
  connectionLabel: string;
  pendingOps: number;
}

const LABELS: Record<string, string> = {
  open: 'live',
  connecting: 'connecting',
  closed: 'offline',
  fallback: 'multi-tab demo',
};

export default function PresenceBar({ peers, connectionLabel, pendingOps }: PresenceBarProps) {
  return (
    <div className="presence-bar" aria-label="Collaborators">
      <div className="presence-status">
        <span className={`status-dot status-${connectionLabel}`} />
        <span className="status-text">{LABELS[connectionLabel] ?? connectionLabel}</span>
        {pendingOps > 0 ? <span className="pending-badge">{pendingOps} queued</span> : null}
      </div>
      <ul className="presence-avatars">
        {peers.map((p) => (
          <li key={p.clientId} title={p.userName} style={{ ['--peer-color' as string]: p.color }}>
            <span className="avatar-circle">{initials(p.userName)}</span>
            <span className="avatar-name">{p.userName}</span>
          </li>
        ))}
      </ul>
    </div>
  );
}

function initials(name: string): string {
  const parts = name.trim().split(/\s+/);
  const a = parts[0]?.[0] ?? '?';
  const b = parts.length > 1 ? parts[parts.length - 1]?.[0] ?? '' : '';
  return (a + b).toUpperCase();
}
