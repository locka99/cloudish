export interface AuthInfo {
  access_token: string;
  username: string;
  tenant_name: string;
}

export interface Notice {
  id: string;
  author: string;
  message: string;
  created_at: string;
}

export async function login(username: string, password: string): Promise<AuthInfo> {
  const res = await fetch('/api/auth/login', {
    method: 'POST',
    headers: { 'Content-Type': 'application/json' },
    body: JSON.stringify({ username, password }),
  });
  if (!res.ok) {
    const err = await res.json().catch(() => ({}));
    throw new Error((err as { error?: string }).error ?? 'Login failed');
  }
  return res.json();
}

export async function logout(token: string): Promise<void> {
  await fetch('/api/auth/logout', {
    method: 'POST',
    headers: { Authorization: `Bearer ${token}` },
  });
}

export async function listNotices(token: string): Promise<Notice[]> {
  const res = await fetch('/api/notices', {
    headers: { Authorization: `Bearer ${token}` },
  });
  if (!res.ok) {
    const err = await res.json().catch(() => ({}));
    throw new Error((err as { error?: string }).error ?? 'Failed to fetch notices');
  }
  return res.json();
}

export async function addNotice(token: string, message: string): Promise<Notice> {
  const res = await fetch('/api/notices', {
    method: 'POST',
    headers: {
      'Content-Type': 'application/json',
      Authorization: `Bearer ${token}`,
    },
    body: JSON.stringify({ message }),
  });
  if (!res.ok) {
    const err = await res.json().catch(() => ({}));
    throw new Error((err as { error?: string }).error ?? 'Failed to post notice');
  }
  return res.json();
}
