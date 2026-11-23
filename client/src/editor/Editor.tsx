import { useEffect, useRef, useState } from 'react';
import { EditorState, Plugin, type Transaction } from 'prosemirror-state';
import { EditorView, Decoration, DecorationSet } from 'prosemirror-view';
import { undo, redo, history } from 'prosemirror-history';
import { keymap } from 'prosemirror-keymap';
import { baseKeymap, toggleMark, setBlockType, wrapIn } from 'prosemirror-commands';
import { Step } from 'prosemirror-transform';
import { editorSchema } from './schema';
import { SocketClient, type ConnectionState } from '../network/SocketClient';
import PresenceBar, { type Collaborator } from '../components/PresenceBar';

interface RemotePresence {
  clientId: string;
  userId: string;
  userName: string;
  color: string;
  from: number;
  to: number;
}

interface EditorProps {
  documentId: string;
  clientId: string;
  displayName: string;
}

function createPresencePlugin(
  getPresence: () => Map<string, RemotePresence>,
): Plugin {
  return new Plugin({
    props: {
      decorations(state) {
        const decos: ReturnType<typeof Decoration.inline>[] = [];
        for (const p of getPresence().values()) {
          const from = Math.max(0, Math.min(p.from, state.doc.content.size));
          const to = Math.max(from, Math.min(p.to, state.doc.content.size));
          if (from === to) {
            decos.push(
              Decoration.widget(from, () => {
                const caret = document.createElement('span');
                caret.className = 'remote-caret';
                caret.style.borderColor = p.color;
                caret.title = p.userName;
                return caret;
              }, { side: -1 }),
            );
          } else {
            decos.push(
              Decoration.inline(from, to, {
                class: 'remote-selection',
                style: `background: ${p.color}33`,
              }),
            );
          }
        }
        return DecorationSet.create(state.doc, decos);
      },
    },
  });
}

export default function Editor({ documentId, clientId, displayName }: EditorProps) {
  const hostRef = useRef<HTMLDivElement>(null);
  const viewRef = useRef<EditorView | null>(null);
  const socketRef = useRef<SocketClient | null>(null);
  const revisionRef = useRef(0);
  const presenceRef = useRef(new Map<string, RemotePresence>());
  const applyingRemote = useRef(false);

  const [peers, setPeers] = useState<Collaborator[]>([]);
  const [conn, setConn] = useState<ConnectionState>('closed');
  const [pending, setPending] = useState(0);

  useEffect(() => {
    if (!hostRef.current) return;

    const presencePlugin = createPresencePlugin(() => presenceRef.current);

    const state = EditorState.create({
      schema: editorSchema,
      doc: editorSchema.node('doc', null, [
        editorSchema.node('paragraph', null, [
          editorSchema.text('Start writing — changes sync live with collaborators.'),
        ]),
      ]),
      plugins: [
        history(),
        keymap({
          'Mod-z': undo,
          'Shift-Mod-z': redo,
          'Mod-y': redo,
          'Mod-b': toggleMark(editorSchema.marks.strong!),
          'Mod-i': toggleMark(editorSchema.marks.em!),
        }),
        keymap(baseKeymap),
        presencePlugin,
      ],
    });

    const view = new EditorView(hostRef.current, {
      state,
      dispatchTransaction(tr: Transaction) {
        const newState = view.state.apply(tr);
        view.updateState(newState);

        if (applyingRemote.current) return;

        if (tr.docChanged && tr.steps.length > 0) {
          const steps = tr.steps.map((s) => {
            const json = s.toJSON() as Record<string, unknown>;
            // Annotate with insert length for server-side OT transforms
            const size = 'slice' in s ? (s as { slice: { size: number } }).slice?.size ?? 0 : 0;
            return { ...json, insertLength: size, sliceSize: size };
          });
          socketRef.current?.sendOp({
            baseRevision: revisionRef.current,
            steps,
            enqueuedAt: Date.now(),
          });
          setPending(socketRef.current?.pendingCount ?? 0);
        }

        if (tr.selectionSet) {
          const { from, to } = tr.selection;
          socketRef.current?.sendPresence({
            userId: clientId,
            userName: displayName,
            from,
            to,
            color: '#3d9b6a',
          });
        }
      },
    });
    viewRef.current = view;

    const socket = new SocketClient({
      documentId,
      clientId,
      onStateChange: (s) => {
        setConn(s);
        setPending(socket.pendingCount);
      },
      onMessage: (raw) => {
        const msg = raw as {
          type: string;
          revision?: number;
          clientId?: string;
          steps?: unknown[];
          peers?: Collaborator[];
          from?: number;
          to?: number;
          userId?: string;
          userName?: string;
          color?: string;
        };

        if (msg.type === 'ack' && typeof msg.revision === 'number') {
          revisionRef.current = msg.revision;
          setPending(socket.pendingCount);
          return;
        }

        if (msg.type === 'snapshot' && typeof msg.revision === 'number') {
          revisionRef.current = msg.revision;
          return;
        }

        if (msg.type === 'peers' && Array.isArray(msg.peers)) {
          setPeers(msg.peers.filter((p) => p.clientId !== clientId));
          return;
        }

        if (msg.type === 'presence' && msg.clientId && msg.clientId !== clientId) {
          presenceRef.current.set(msg.clientId, {
            clientId: msg.clientId,
            userId: msg.userId ?? msg.clientId,
            userName: msg.userName ?? 'Peer',
            color: msg.color ?? '#888',
            from: msg.from ?? 0,
            to: msg.to ?? 0,
          });
          setPeers((prev) => {
            if (prev.some((p) => p.clientId === msg.clientId)) {
              return prev.map((p) =>
                p.clientId === msg.clientId
                  ? {
                      ...p,
                      userName: msg.userName ?? p.userName,
                      color: msg.color ?? p.color,
                    }
                  : p,
              );
            }
            return [
              ...prev,
              {
                clientId: msg.clientId!,
                userId: msg.userId ?? msg.clientId!,
                userName: msg.userName ?? 'Peer',
                color: msg.color ?? '#5a9fd4',
              },
            ];
          });
          view.dispatch(view.state.tr);
          return;
        }

        if (msg.type === 'op' && msg.clientId !== clientId && Array.isArray(msg.steps)) {
          if (msg.clientId) {
            setPeers((prev) =>
              prev.some((p) => p.clientId === msg.clientId)
                ? prev
                : [
                    ...prev,
                    {
                      clientId: msg.clientId!,
                      userId: msg.clientId!,
                      userName: `Peer ${msg.clientId!.slice(-4)}`,
                      color: '#5a9fd4',
                    },
                  ],
            );
          }
          applyingRemote.current = true;
          try {
            let tr = view.state.tr;
            for (const stepJson of msg.steps) {
              const step = Step.fromJSON(editorSchema, stepJson);
              const result = tr.maybeStep(step);
              if (result.failed) break;
            }
            if (typeof msg.revision === 'number') {
              revisionRef.current = msg.revision;
            }
            view.dispatch(tr);
          } catch (err) {
            console.warn('Failed to apply remote steps', err);
          } finally {
            applyingRemote.current = false;
          }
        }
      },
    });
    socketRef.current = socket;
    socket.connect();
    setPending(socket.pendingCount);

    return () => {
      socket.disconnect();
      view.destroy();
      viewRef.current = null;
      socketRef.current = null;
    };
  }, [documentId, clientId, displayName]);

  const runCommand = (cmd: (state: EditorState, dispatch?: (tr: Transaction) => void) => boolean) => {
    const view = viewRef.current;
    if (!view) return;
    cmd(view.state, view.dispatch.bind(view));
    view.focus();
  };

  return (
    <div className="editor-shell">
      <PresenceBar peers={peers} connectionLabel={conn} pendingOps={pending} />
      <div className="toolbar" role="toolbar" aria-label="Formatting">
        <button type="button" onClick={() => runCommand(toggleMark(editorSchema.marks.strong!))}>
          Bold
        </button>
        <button type="button" onClick={() => runCommand(toggleMark(editorSchema.marks.em!))}>
          Italic
        </button>
        <button
          type="button"
          onClick={() => runCommand(setBlockType(editorSchema.nodes.heading!, { level: 1 }))}
        >
          H1
        </button>
        <button
          type="button"
          onClick={() => runCommand(setBlockType(editorSchema.nodes.paragraph!))}
        >
          Paragraph
        </button>
        <button type="button" onClick={() => runCommand(wrapIn(editorSchema.nodes.blockquote!))}>
          Quote
        </button>
        <button type="button" onClick={() => runCommand(undo)}>
          Undo
        </button>
        <button type="button" onClick={() => runCommand(redo)}>
          Redo
        </button>
      </div>
      <div className="editor-surface" ref={hostRef} />
    </div>
  );
}
