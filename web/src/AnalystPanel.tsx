import { useCallback, useEffect, useState } from 'react';
import {
  ApiError,
  approveProposal,
  DETECTION_TYPES,
  detectionLabel,
  getDetection,
  listCases,
  listDetections,
  listProposals,
  rejectProposal,
  type Detection,
  type DetectionDetail,
  type Proposal,
  type Session,
} from './analystApi';

type Tab = 'proposals' | 'detections' | 'cases';

interface Props {
  session: Session;
  /** Increases when the server reports a change. */
  tick: number;
  pending: number;
  onClose: () => void;
  onShow: (d: Detection) => void;
  onSessionExpired: () => void;
}

const when = (iso: string | null) => (iso ? new Date(iso).toLocaleString() : '—');
const json = (v: unknown) => JSON.stringify(v, null, 2);

function useLoad<T>(load: () => Promise<T>, deps: unknown[], onExpired: () => void) {
  const [data, setData] = useState<T | null>(null);
  const [error, setError] = useState<string | null>(null);
  useEffect(() => {
    let cancelled = false;
    load()
      .then((d) => {
        if (!cancelled) {
          setData(d);
          setError(null);
        }
      })
      .catch((e: unknown) => {
        if (cancelled) return;
        if (e instanceof ApiError && e.status === 401) onExpired();
        else setError(e instanceof Error ? e.message : 'Failed to load');
      });
    return () => {
      cancelled = true;
    };
    // eslint-disable-next-line react-hooks/exhaustive-deps
  }, deps);
  return { data, error };
}

export function AnalystPanel({ session, tick, pending, onClose, onShow, onSessionExpired }: Props) {
  const [tab, setTab] = useState<Tab>('proposals');
  return (
    <aside className="analyst" aria-label="Analyst workspace">
      <header>
        <nav>
          <button className={tab === 'proposals' ? 'on' : ''} onClick={() => setTab('proposals')}>
            Proposals{pending > 0 && <span className="badge">{pending}</span>}
          </button>
          <button className={tab === 'detections' ? 'on' : ''} onClick={() => setTab('detections')}>Detections</button>
          <button className={tab === 'cases' ? 'on' : ''} onClick={() => setTab('cases')}>Cases</button>
        </nav>
        <button className="close" onClick={onClose} aria-label="Close">×</button>
      </header>
      <div className="body">
        {tab === 'proposals' && <Proposals session={session} tick={tick} onExpired={onSessionExpired} />}
        {tab === 'detections' && <Detections tick={tick} onShow={onShow} onExpired={onSessionExpired} />}
        {tab === 'cases' && <Cases tick={tick} onExpired={onSessionExpired} />}
      </div>
    </aside>
  );
}

function Proposals({ session, tick, onExpired }: { session: Session; tick: number; onExpired: () => void }) {
  const [status, setStatus] = useState<'PENDING' | 'APPROVED' | 'REJECTED'>('PENDING');
  const [local, setLocal] = useState(0);
  const { data, error } = useLoad(() => listProposals(status), [status, tick, local], onExpired);
  const admin = session.roles.includes('ADMIN');
  return (
    <>
      <div className="filters">
        {(['PENDING', 'APPROVED', 'REJECTED'] as const).map((s) => (
          <button key={s} className={status === s ? 'on' : ''} onClick={() => setStatus(s)}>{s.toLowerCase()}</button>
        ))}
      </div>
      {error && <p className="warn">{error}</p>}
      {data && data.items.length === 0 && <p className="note">No {status.toLowerCase()} proposals.</p>}
      {data?.items.map((p) => (
        <ProposalCard key={p.id} proposal={p} admin={admin} onDecided={() => setLocal((n) => n + 1)} onExpired={onExpired} />
      ))}
    </>
  );
}

function ProposalCard({ proposal: p, admin, onDecided, onExpired }: {
  proposal: Proposal; admin: boolean; onDecided: () => void; onExpired: () => void;
}) {
  const [note, setNote] = useState('');
  const [busy, setBusy] = useState(false);
  const [error, setError] = useState<string | null>(null);
  const [details, setDetails] = useState(false);
  const needsAdmin = p.type === 'RULE_CHANGE' && !admin;

  const decide = useCallback(async (approve: boolean) => {
    setBusy(true);
    setError(null);
    try {
      if (approve) await approveProposal(p.id, note);
      else await rejectProposal(p.id, note);
      onDecided();
    } catch (e) {
      if (e instanceof ApiError && e.status === 401) onExpired();
      else setError(e instanceof Error ? e.message : 'Failed');
    } finally {
      setBusy(false);
    }
  }, [p.id, note, onDecided, onExpired]);

  return (
    <article className="item">
      <h3>
        {p.type.replace('_', ' ').toLowerCase()}
        {p.confidence != null && <span className="chip">confidence {(p.confidence * 100).toFixed(0)}%</span>}
        <span className={`chip st-${p.status.toLowerCase()}`}>{p.status.toLowerCase()}</span>
      </h3>
      <p className="meta">by {p.proposedBy} · {when(p.createdAt)}</p>
      {p.rationale && <p>{p.rationale}</p>}
      <button className="link" onClick={() => setDetails((d) => !d)}>{details ? 'Hide' : 'Show'} proposal and evidence</button>
      {details && <pre>{json({ payload: p.payload, evidence: p.evidence })}</pre>}
      {p.status === 'PENDING' ? (
        <div className="decide">
          <input placeholder="Note (required to reject)" value={note} onChange={(e) => setNote(e.target.value)} />
          <button className="primary" disabled={busy || needsAdmin} title={needsAdmin ? 'Rule changes need an administrator' : ''} onClick={() => void decide(true)}>Approve</button>
          <button disabled={busy || !note.trim()} onClick={() => void decide(false)}>Reject</button>
        </div>
      ) : (
        <p className="meta">{p.status.toLowerCase()} by {p.decidedBy} · {when(p.decidedAt)}{p.decisionNote ? ` — ${p.decisionNote}` : ''}</p>
      )}
      {needsAdmin && p.status === 'PENDING' && <p className="note">Needs an administrator to approve.</p>}
      {error && <p className="warn" role="alert">{error}</p>}
    </article>
  );
}

function Detections({ tick, onShow, onExpired }: { tick: number; onShow: (d: Detection) => void; onExpired: () => void }) {
  const [type, setType] = useState('');
  const [status, setStatus] = useState('');
  const [page, setPage] = useState(0);
  const [openId, setOpenId] = useState<string | null>(null);
  const { data, error } = useLoad(() => listDetections({ type, status, page }), [type, status, page, tick], onExpired);
  return (
    <>
      <div className="filters">
        <select value={type} onChange={(e) => { setType(e.target.value); setPage(0); }} aria-label="Type">
          <option value="">All types</option>
          {DETECTION_TYPES.map(([t, l]) => <option key={t} value={t}>{l}</option>)}
        </select>
        <select value={status} onChange={(e) => { setStatus(e.target.value); setPage(0); }} aria-label="Status">
          <option value="">Open and closed</option>
          <option value="OPEN">Open</option>
          <option value="CLOSED">Closed</option>
        </select>
      </div>
      {error && <p className="warn">{error}</p>}
      {data && data.items.length === 0 && <p className="note">No detections match.</p>}
      {data?.items.map((d) => (
        <article key={d.detectionId} className="item">
          <h3>
            {detectionLabel(d.type)}
            <span className={`chip st-${d.status.toLowerCase()}`}>{d.status.toLowerCase()}</span>
          </h3>
          <p className="meta">
            MMSI {d.mmsi}{d.otherMmsi ? ` & ${d.otherMmsi}` : ''} · {when(d.startedAt)}{d.endedAt ? ` → ${when(d.endedAt)}` : ' → ongoing'}
          </p>
          <div className="row">
            {d.latitude != null && d.longitude != null && <button onClick={() => onShow(d)}>Show on map</button>}
            <button onClick={() => setOpenId(openId === d.detectionId ? null : d.detectionId)}>
              {openId === d.detectionId ? 'Hide evidence' : 'Evidence'}
            </button>
          </div>
          {openId === d.detectionId && <DetectionEvidence id={d.detectionId} onExpired={onExpired} />}
        </article>
      ))}
      {data && (page > 0 || data.hasMore) && (
        <div className="row">
          <button disabled={page === 0} onClick={() => setPage(page - 1)}>Previous</button>
          <button disabled={!data.hasMore} onClick={() => setPage(page + 1)}>Next</button>
        </div>
      )}
      <p className="note">Detections are observations for an analyst to look at, not findings of wrongdoing.</p>
    </>
  );
}

function DetectionEvidence({ id, onExpired }: { id: string; onExpired: () => void }) {
  const { data, error } = useLoad<DetectionDetail>(() => getDetection(id), [id], onExpired);
  if (error) return <p className="warn">{error}</p>;
  if (!data) return <p className="note">Loading…</p>;
  return (
    <>
      <pre>{json(data.detection.evidence)}</pre>
      {data.assessments.map((a) => (
        <p key={a.id} className="meta">{a.category} by {a.assessedBy}: {a.rationale}</p>
      ))}
      {data.caseIds.length > 0 && <p className="meta">In case(s): {data.caseIds.join(', ')}</p>}
    </>
  );
}

function Cases({ tick, onExpired }: { tick: number; onExpired: () => void }) {
  const [status, setStatus] = useState('OPEN');
  const { data, error } = useLoad(() => listCases(status), [status, tick], onExpired);
  return (
    <>
      <div className="filters">
        {['OPEN', 'CLOSED'].map((s) => (
          <button key={s} className={status === s ? 'on' : ''} onClick={() => setStatus(s)}>{s.toLowerCase()}</button>
        ))}
      </div>
      {error && <p className="warn">{error}</p>}
      {data && data.items.length === 0 && <p className="note">No {status.toLowerCase()} cases. Cases open when a proposal is approved.</p>}
      {data?.items.map((c) => (
        <article key={c.id} className="item">
          <h3>{c.title}</h3>
          <p className="meta">MMSI {c.mmsi} · opened {when(c.openedAt)}{c.closedAt ? ` · closed ${when(c.closedAt)}` : ''}</p>
        </article>
      ))}
    </>
  );
}
