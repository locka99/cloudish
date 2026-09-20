import { useState, useEffect, FormEvent } from 'react';
import { AuthInfo, Notice, listNotices, addNotice } from '../api';

interface Props {
  auth: AuthInfo;
  onLogout: () => void;
}

export default function NoticeBoard({ auth, onLogout }: Props) {
  const [notices, setNotices] = useState<Notice[]>([]);
  const [message, setMessage] = useState('');
  const [loading, setLoading] = useState(true);
  const [posting, setPosting] = useState(false);
  const [error, setError] = useState('');

  useEffect(() => {
    fetchNotices();
    // eslint-disable-next-line react-hooks/exhaustive-deps
  }, []);

  const fetchNotices = async () => {
    setLoading(true);
    setError('');
    try {
      const data = await listNotices(auth.access_token);
      setNotices(data);
    } catch (err: unknown) {
      setError(err instanceof Error ? err.message : 'Failed to load notices');
    } finally {
      setLoading(false);
    }
  };

  const handlePost = async (e: FormEvent) => {
    e.preventDefault();
    if (!message.trim()) return;
    setPosting(true);
    setError('');
    try {
      const notice = await addNotice(auth.access_token, message.trim());
      setNotices((prev) => [notice, ...prev]);
      setMessage('');
    } catch (err: unknown) {
      setError(err instanceof Error ? err.message : 'Failed to post notice');
    } finally {
      setPosting(false);
    }
  };

  return (
    <div className="app">
      <header className="app-header">
        <div className="header-left">
          <h1>Notice Board</h1>
          <span className="tenant-badge">{auth.tenant_name}</span>
        </div>
        <div className="header-right">
          <span className="user-info">
            Signed in as <strong>{auth.username}</strong>
          </span>
          <button className="btn-logout" onClick={onLogout}>
            Sign Out
          </button>
        </div>
      </header>

      <main className="main-content">
        <section className="post-section">
          <p className="section-title">Post a Notice</p>
          <form onSubmit={handlePost}>
            <textarea
              value={message}
              onChange={(e) => setMessage(e.target.value)}
              placeholder="Share something with your team…"
              rows={3}
              required
            />
            <button
              type="submit"
              className="btn-post"
              disabled={posting || !message.trim()}
            >
              {posting ? 'Posting…' : 'Post Notice'}
            </button>
          </form>
        </section>

        {error && <div className="error-banner">{error}</div>}

        <section className="notices-section">
          <p className="section-title">Recent Notices</p>
          {loading ? (
            <div className="loading-state">Loading notices…</div>
          ) : notices.length === 0 ? (
            <div className="empty-state">
              No notices yet — be the first to post one!
            </div>
          ) : (
            <div className="notices-list">
              {notices.map((notice) => (
                <div key={notice.id} className="notice-card">
                  <p className="notice-message">{notice.message}</p>
                  <div className="notice-meta">
                    <span className="notice-author">{notice.author}</span>
                    <span className="notice-time">
                      {formatDate(notice.created_at)}
                    </span>
                  </div>
                </div>
              ))}
            </div>
          )}
        </section>
      </main>
    </div>
  );
}

function formatDate(iso: string): string {
  try {
    return new Date(iso).toLocaleString(undefined, {
      year: 'numeric',
      month: 'short',
      day: 'numeric',
      hour: '2-digit',
      minute: '2-digit',
    });
  } catch {
    return iso;
  }
}
