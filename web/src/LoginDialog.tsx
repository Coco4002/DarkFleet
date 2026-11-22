import { useState, type FormEvent } from 'react';
import { ApiError, login, type Session } from './analystApi';

export function LoginDialog({ onDone, onCancel }: { onDone: (s: Session) => void; onCancel: () => void }) {
  const [username, setUsername] = useState('');
  const [password, setPassword] = useState('');
  const [error, setError] = useState<string | null>(null);
  const [busy, setBusy] = useState(false);

  async function submit(e: FormEvent) {
    e.preventDefault();
    setBusy(true);
    setError(null);
    try {
      onDone(await login(username, password));
    } catch (err) {
      setError(err instanceof ApiError ? err.message : 'Could not reach the server');
    } finally {
      setBusy(false);
    }
  }

  return (
    <div className="modal" role="dialog" aria-modal="true" aria-label="Analyst login">
      <form className="card" onSubmit={submit}>
        <h2>Analyst login</h2>
        <p className="note">Detections, cases and proposals are visible to signed-in analysts only.</p>
        <label>
          Username
          <input autoFocus autoComplete="username" value={username} onChange={(e) => setUsername(e.target.value)} />
        </label>
        <label>
          Password
          <input type="password" autoComplete="current-password" value={password} onChange={(e) => setPassword(e.target.value)} />
        </label>
        {error && <p className="warn" role="alert">{error}</p>}
        <div className="row">
          <button type="button" onClick={onCancel}>Cancel</button>
          <button type="submit" className="primary" disabled={busy || !username || !password}>Sign in</button>
        </div>
      </form>
    </div>
  );
}
