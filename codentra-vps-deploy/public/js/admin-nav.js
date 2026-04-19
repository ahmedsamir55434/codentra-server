(function () {
  const sidebar = document.querySelector('.admin-sidebar');
  const nav = document.querySelector('.admin-sidebar .admin-nav');
  if (!sidebar || !nav) return;

  const ensureToggleButton = () => {
    if (sidebar.querySelector('.admin-nav-toggle')) return;

    const btn = document.createElement('button');
    btn.type = 'button';
    btn.className = 'admin-nav-toggle';
    btn.setAttribute('aria-expanded', 'false');
    btn.innerHTML = '<span>القائمة</span><span class="icon">☰</span>';

    btn.addEventListener('click', () => {
      const isOpen = sidebar.classList.toggle('open');
      btn.setAttribute('aria-expanded', isOpen ? 'true' : 'false');
    });

    sidebar.insertBefore(btn, sidebar.firstChild);

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
