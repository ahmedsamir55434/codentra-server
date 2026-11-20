(function () {
  const sidebar = document.querySelector('.admin-sidebar');
  const nav = document.querySelector('.admin-sidebar .admin-nav');
  if (!sidebar || !nav) return;

  const syncAdminLinks = () => {
    if (!Array.isArray(window.codentraAdminNav) || !window.codentraAdminNav.length) return;
    const currentPath = window.location.pathname;
    nav.innerHTML = '';
    window.codentraAdminNav.forEach((item) => {
      const link = document.createElement('a');
      link.href = item.href;
      link.textContent = item.label;
      const active = item.href === '/admin'
        ? currentPath === '/admin'
        : currentPath === item.href || currentPath.startsWith(item.href + '/');
      if (active) link.classList.add('active');
      nav.appendChild(link);
    });
  };

  syncAdminLinks();

  const ensureToggleButton = () => {
    let btn = sidebar.querySelector('.admin-nav-toggle');
    if (!btn) {
      // A few older admin pages render the toggle beside the sidebar.
      // Reuse it and move it inside so every page gets one consistent control.
      btn = document.getElementById('adminNavToggle');
      if (btn) sidebar.insertBefore(btn, sidebar.firstChild);
    }
    if (!btn) {
      btn = document.createElement('button');
      btn.type = 'button';
      btn.className = 'admin-nav-toggle';
      btn.setAttribute('aria-expanded', 'false');
      btn.innerHTML = '<span>القائمة</span><span class="icon">☰</span>';
      sidebar.insertBefore(btn, sidebar.firstChild);
    }

    if (btn.dataset.adminNavBound === 'true') return;
    btn.dataset.adminNavBound = 'true';
    btn.addEventListener('click', () => {
      const isOpen = sidebar.classList.toggle('open');
      btn.setAttribute('aria-expanded', isOpen ? 'true' : 'false');
    });

    nav.querySelectorAll('a').forEach((a) => {
      a.addEventListener('click', () => {
        if (window.matchMedia('(max-width: 768px)').matches) {
          sidebar.classList.remove('open');
          btn.setAttribute('aria-expanded', 'false');
        }
      });
    });
  };

  const applyMobileState = () => {
    const isMobile = window.matchMedia('(max-width: 768px)').matches;
    if (isMobile) {
      ensureToggleButton();
    } else {
      sidebar.classList.remove('open');
      const btn = sidebar.querySelector('.admin-nav-toggle');
      if (btn) btn.setAttribute('aria-expanded', 'false');
    }
  };

  window.addEventListener('resize', applyMobileState);
  applyMobileState();
})();
