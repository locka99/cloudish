import { useState, useEffect } from 'react';
import LoginForm from './components/LoginForm';
import NoticeBoard from './components/NoticeBoard';
import { AuthInfo, logout } from './api';

const AUTH_KEY = 'tenant_demo_auth';

export default function App() {
  const [auth, setAuth] = useState<AuthInfo | null>(null);

  useEffect(() => {
    const stored = localStorage.getItem(AUTH_KEY);
    if (stored) {
      try {
        setAuth(JSON.parse(stored) as AuthInfo);
      } catch {
        localStorage.removeItem(AUTH_KEY);
      }
    }
  }, []);

  const handleLogin = (authInfo: AuthInfo) => {
    localStorage.setItem(AUTH_KEY, JSON.stringify(authInfo));
    setAuth(authInfo);
  };

  const handleLogout = async () => {
    if (auth) {
      await logout(auth.access_token).catch(() => {/* ignore */});
    }
    localStorage.removeItem(AUTH_KEY);
    setAuth(null);
  };

  if (!auth) {
    return <LoginForm onLogin={handleLogin} />;
  }

  return <NoticeBoard auth={auth} onLogout={handleLogout} />;
}
