(function () {
  var STORAGE_KEY = 'codentra_theme';
  var THEMES = { dark: 'dark', light: 'light' };

  function getSystemTheme() {
    try {
      if (window.matchMedia && window.matchMedia('(prefers-color-scheme: light)').matches) {
        return THEMES.light;
      }
    } catch (e) {}
    return THEMES.dark;
  }

  function getSavedTheme() {
    try {
      return localStorage.getItem(STORAGE_KEY);
    } catch (e) {
      return null;
    }
  }

  function setSavedTheme(theme) {
    try {
      localStorage.setItem(STORAGE_KEY, theme);
    } catch (e) {}
  }

  function applyTheme(theme) {
    var root = document.documentElement;
    if (theme === THEMES.light) {
      root.setAttribute('data-theme', 'light');
    } else {
      root.removeAttribute('data-theme');
      theme = THEMES.dark;
    }
    updateToggleButton(theme);
  }

  function getActiveTheme() {
    var root = document.documentElement;
    return root.getAttribute('data-theme') === 'light' ? THEMES.light : THEMES.dark;
  }

  function updateToggleButton(theme) {
    var btn = document.getElementById('themeToggle');
    if (!btn) return;

    if (theme === THEMES.light) {
      btn.textContent = 'نهاري';
      btn.setAttribute('aria-pressed', 'true');
    } else {
      btn.textContent = 'ليلي';
      btn.setAttribute('aria-pressed', 'false');
    }
  }

  function init() {
    var initialTheme = getSavedTheme() || getSystemTheme();
    applyTheme(initialTheme);

    var btn = document.getElementById('themeToggle');
    if (!btn) return;

    btn.addEventListener('click', function () {
      var next = getActiveTheme() === THEMES.light ? THEMES.dark : THEMES.light;
      setSavedTheme(next);
      applyTheme(next);
    });
  }

  // Mobile auto-hide header
  function initHeaderAutoHide() {
    var header = document.querySelector('.main-header');
    if (!header) return;

    var lastScrollY = window.pageYOffset;
    var ticking = false;

    function updateHeader() {
      var currentScrollY = window.pageYOffset;
      if (window.matchMedia('(max-width: 768px)').matches) {
        if (currentScrollY > lastScrollY && currentScrollY > 80) {
          header.classList.add('header-hidden');
        } else {
          header.classList.remove('header-hidden');
        }
      } else {
        header.classList.remove('header-hidden');
      }
      lastScrollY = currentScrollY;
      ticking = false;
    }

    function requestTick() {
      if (!ticking) {
        requestAnimationFrame(updateHeader);
        ticking = true;
      }
    }

    window.addEventListener('scroll', requestTick, { passive: true });
    window.addEventListener('resize', requestTick);
  }

  if (document.readyState === 'loading') {
    document.addEventListener('DOMContentLoaded', function () {
      init();
      initHeaderAutoHide();
    });
  } else {
    init();
    initHeaderAutoHide();
  }
})();
