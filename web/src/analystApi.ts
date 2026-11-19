// Analyst API (vessel-intelligence, reached through Caddy under /api/v1/...). Needs a login session.

export interface Session {
  authenticated: boolean;
  username: string | null;
  roles: string[];
}

export interface Page<T> {
  items: T[];
  page: number;
  size: number;
  hasMore: boolean;
}

export interface Detection {
  detectionId: string;
  type: string;
  status: string;
  version: number;
  mmsi: string;
  otherMmsi: string | null;
  startedAt: string;
  endedAt: string | null;
  latitude: number | null;
  longitude: number | null;
  h3Cell: string | null;
  rulesVersion: number;
  evidence: unknown;
  updatedAt: string;
}

export interface Assessment {
  id: string;
  detectionId: string;
  category: string;
  rationale: string;
  assessedBy: string;
  createdAt: string;
}

export interface DetectionDetail {
  detection: Detection;
  assessments: Assessment[];
  caseIds: string[];
}

export interface Proposal {
  id: string;
  type: 'CASE' | 'WATCHLIST_CHANGE' | 'RULE_CHANGE';
  status: 'PENDING' | 'APPROVED' | 'REJECTED';
  payload: unknown;
  rationale: string | null;
  evidence: unknown;
  confidence: number | null;
  proposedBy: string;
  createdAt: string;
  decidedBy: string | null;
  decidedAt: string | null;
  decisionNote: string | null;
  resultType: string | null;
  resultId: string | null;
}

export interface CaseSummary {
  id: string;
  title: string;
  mmsi: string;
  status: string;
  openedAt: string;
  closedAt: string | null;
}

export class ApiError extends Error {
  constructor(public status: number, message: string) {
    super(message);
  }
}

function csrfToken(): string | null {
  const match = document.cookie.split('; ').find((c) => c.startsWith('XSRF-TOKEN='));
  return match ? decodeURIComponent(match.slice('XSRF-TOKEN='.length)) : null;
}

async function call<T>(method: string, path: string, body?: unknown): Promise<T> {
  const headers: Record<string, string> = { Accept: 'application/json' };
  if (body !== undefined) headers['Content-Type'] = 'application/json';
  if (method !== 'GET') {
    const token = csrfToken();
    if (token) headers['X-XSRF-TOKEN'] = token;
  }
  const response = await fetch(path, {
    method,
    headers,
    credentials: 'same-origin',
    body: body === undefined ? undefined : JSON.stringify(body),
  });
  if (!response.ok) {
    let detail = response.statusText;
    try {
      detail = (await response.json()).detail ?? detail;
    } catch {
      // no problem document
    }
    throw new ApiError(response.status, detail || `HTTP ${response.status}`);
  }
  if (response.status === 204) return undefined as T;
  return (await response.json()) as T;
}

export const getSession = () => call<Session>('GET', '/api/v1/auth/me');
export const login = (username: string, password: string) =>
  call<Session>('POST', '/api/v1/auth/login', { username, password });
export const logout = () => call<void>('POST', '/api/v1/auth/logout');

export function listDetections(params: { type?: string; status?: string; mmsi?: string; page?: number }) {
  const q = new URLSearchParams({ size: '50' });
  if (params.type) q.set('type', params.type);
  if (params.status) q.set('status', params.status);
  if (params.mmsi) q.set('mmsi', params.mmsi);
  q.set('page', String(params.page ?? 0));
  return call<Page<Detection>>('GET', `/api/v1/detections?${q}`);
}
export const getDetection = (id: string) => call<DetectionDetail>('GET', `/api/v1/detections/${encodeURIComponent(id)}`);
export const listProposals = (status: string) =>
  call<Page<Proposal>>('GET', `/api/v1/proposals?status=${encodeURIComponent(status)}&size=50`);
export const approveProposal = (id: string, note: string) =>
  call<Proposal>('POST', `/api/v1/proposals/${encodeURIComponent(id)}/approve`, { note: note || null });
export const rejectProposal = (id: string, note: string) =>
  call<Proposal>('POST', `/api/v1/proposals/${encodeURIComponent(id)}/reject`, { note });
export const listCases = (status: string) =>
  call<Page<CaseSummary>>('GET', `/api/v1/cases?status=${encodeURIComponent(status)}&size=50`);

export const DETECTION_TYPES = [
  ['UNEXPLAINED_GAP', 'Unexplained AIS gap'],
  ['LOITERING', 'Loitering'],
  ['POTENTIAL_ENCOUNTER', 'Potential encounter'],
  ['IDENTITY_INCONSISTENCY', 'Identity inconsistency'],
] as const;

export const detectionLabel = (type: string) => DETECTION_TYPES.find(([t]) => t === type)?.[1] ?? type;

export const analystEventsUrl = '/api/v1/events';
