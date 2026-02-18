/**
 * auth.js — LeadSource USA Authentication Guard
 * Add <script src="auth.js"></script> as the FIRST script in any protected page.
 * Redirects to login.html immediately if session is invalid.
 */
(function() {
  async function sha256(message) {
    const msgBuffer = new TextEncoder().encode(message);
    const hashBuffer = await crypto.subtle.digest('SHA-256', msgBuffer);
    const hashArray = Array.from(new Uint8Array(hashBuffer));
    return hashArray.map(b => b.toString(16).padStart(2, '0')).join('');
  }

  async function checkAuth() {
    const storedToken = sessionStorage.getItem('ls_auth');
    const storedHash  = sessionStorage.getItem('ls_hash');
    const storedDate  = sessionStorage.getItem('ls_date');
    const today       = new Date().toISOString().split('T')[0];

    // No session at all
    if (!storedToken || !storedHash || !storedDate) {
      window.location.replace('login.html');
      return;
    }

    // Session expired (new day = must re-login)
    if (storedDate !== today) {
      sessionStorage.clear();
      window.location.replace('login.html');
      return;
    }

    // Verify token integrity
    const expectedToken = await sha256(storedHash + today);
    if (storedToken !== expectedToken) {
      sessionStorage.clear();
      window.location.replace('login.html');
      return;
    }

    // Verify password hash still exists in passwords.json
    // (allows instant revocation by removing hash from file)
    try {
      const res = await fetch('data/passwords.json?_=' + Date.now());
      if (!res.ok) throw new Error();
      const data = await res.json();
      const validHashes = data.passwords.map(p => p.hash);
      if (!validHashes.includes(storedHash)) {
        sessionStorage.clear();
        window.location.replace('login.html');
        return;
      }
    } catch(e) {
      // If passwords.json fails to load, allow access (don't lock out on network error)
      console.warn('auth.js: could not verify passwords.json, allowing cached session');
    }
  }

  // Run auth check immediately
  checkAuth();

  // Expose logout function globally
  window.leadsourceLogout = function() {
    sessionStorage.clear();
    window.location.replace('login.html');
  };
})();
