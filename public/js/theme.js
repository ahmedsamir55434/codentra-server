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

  function initNotificationsPopover() {
    var toggle = document.getElementById('notificationToggle');
    var panel = document.getElementById('notificationPanel');
    var badge = document.getElementById('notificationBadge');
    var body = document.getElementById('notificationPanelBody');
    var markAll = document.getElementById('notificationMarkAll');
    var popover = document.getElementById('notificationPopover');

    if (!toggle || !panel || !badge || !body || !markAll || !popover) return;

    var isOpen = false;
    var lastSignature = '';
    var pollTimer = null;

    function formatDate(value) {
      if (!value) return '';
      try {
        return new Date(value).toLocaleString('ar-EG');
      } catch (e) {
        return value;
      }
    }

    function escapeHtml(value) {
      return String(value || '')
        .replace(/&/g, '&amp;')
        .replace(/</g, '&lt;')
        .replace(/>/g, '&gt;')
        .replace(/"/g, '&quot;')
        .replace(/'/g, '&#39;');
    }

    function setBadge(count) {
      var unreadCount = Number(count || 0);
      if (unreadCount > 0) {
        badge.textContent = unreadCount > 99 ? '99+' : String(unreadCount);
        badge.classList.remove('is-hidden');
      } else {
        badge.textContent = '0';
        badge.classList.add('is-hidden');
      }
    }

    function renderNotifications(notifications) {
      if (!Array.isArray(notifications) || notifications.length === 0) {
        body.innerHTML = '<div class="notification-panel-empty">لا توجد إشعارات حاليًا.</div>';
        return;
      }

      body.innerHTML = notifications.map(function (notification) {
        var href = notification.link || '/notifications';
        return [
          '<a class="notification-preview-item ' + (!notification.readAt ? 'is-unread' : '') + '" href="' + escapeHtml(href) + '" data-notification-id="' + escapeHtml(notification.id) + '">',
          '<div class="notification-preview-title">',
          '<strong>' + escapeHtml(notification.title) + '</strong>',
          !notification.readAt ? '<span class="notification-preview-dot"></span>' : '',
          '</div>',
          '<div class="notification-preview-message">' + escapeHtml(notification.message) + '</div>',
          '<div class="notification-preview-time">' + escapeHtml(formatDate(notification.createdAt)) + '</div>',
          '</a>'
        ].join('');
      }).join('');
    }

    function setOpen(nextOpen) {
      isOpen = Boolean(nextOpen);
      popover.open = isOpen;
      toggle.setAttribute('aria-expanded', isOpen ? 'true' : 'false');
      if (isOpen) {
        positionPanel();
      }
    }

    function positionPanel() {
      if (!popover.open) return;

      var rect = toggle.getBoundingClientRect();
      var viewportPadding = window.matchMedia('(max-width: 768px)').matches ? 12 : 16;
      var panelWidth = Math.min(window.innerWidth - (viewportPadding * 2), window.matchMedia('(max-width: 768px)').matches ? 340 : 360);
      var left = rect.right - panelWidth;

      if (left < viewportPadding) {
        left = viewportPadding;
      }
      if (left + panelWidth > window.innerWidth - viewportPadding) {
        left = window.innerWidth - panelWidth - viewportPadding;
      }

      panel.style.width = panelWidth + 'px';
      panel.style.left = left + 'px';
      panel.style.top = (rect.bottom + 12) + 'px';
    }

    function getSignature(payload) {
      try {
        return JSON.stringify(payload);
      } catch (e) {
        return String(Date.now());
      }
    }

    function refreshNotifications(forceRender) {
      return fetch('/api/notifications/summary', {
        credentials: 'same-origin',
        headers: { 'Accept': 'application/json' }
      })
        .then(function (response) {
          if (!response.ok) throw new Error('Failed to load notifications');
          return response.json();
        })
        .then(function (payload) {
          setBadge(payload.unreadCount || 0);
          var signature = getSignature(payload);
          if (forceRender || signature !== lastSignature || isOpen) {
            renderNotifications(payload.notifications || []);
            lastSignature = signature;
          }
        })
        .catch(function () {
          if (!body.children.length) {
            body.innerHTML = '<div class="notification-panel-empty">تعذر تحميل الإشعارات الآن.</div>';
          }
        });
    }

    function markNotificationRead(notificationId) {
      return fetch('/api/notifications/' + encodeURIComponent(notificationId) + '/read', {
        method: 'POST',
        credentials: 'same-origin',
        headers: { 'Accept': 'application/json' }
      }).catch(function () {});
    }

    popover.addEventListener('toggle', function () {
      isOpen = popover.open;
      toggle.setAttribute('aria-expanded', isOpen ? 'true' : 'false');
      if (isOpen) {
        positionPanel();
        refreshNotifications(true);
      }
    });

    markAll.addEventListener('click', function () {
      fetch('/api/notifications/read-all', {
        method: 'POST',
        credentials: 'same-origin',
        headers: { 'Accept': 'application/json' }
      })
        .then(function () { return refreshNotifications(true); })
        .catch(function () {});
    });

    body.addEventListener('click', function (event) {
      var item = event.target.closest('[data-notification-id]');
      if (!item) return;

      var href = item.getAttribute('href');
      var notificationId = item.getAttribute('data-notification-id');
      event.preventDefault();

      markNotificationRead(notificationId).finally(function () {
        if (href) {
          window.location.href = href;
        } else {
          refreshNotifications(true);
        }
      });
    });

    document.addEventListener('click', function (event) {
      if (!isOpen) return;
      if (!popover.contains(event.target)) {
        setOpen(false);
      }
    });

    document.addEventListener('keydown', function (event) {
      if (event.key === 'Escape' && isOpen) {
        setOpen(false);
      }
    });

    document.addEventListener('visibilitychange', function () {
      if (!document.hidden) {
        refreshNotifications(true);
      }
    });

    window.addEventListener('focus', function () {
      refreshNotifications(true);
    });

    window.addEventListener('resize', function () {
      if (isOpen) {
        positionPanel();
      }
    });

    window.addEventListener('scroll', function () {
      if (isOpen) {
        positionPanel();
      }
    }, { passive: true });

    refreshNotifications(true);
    pollTimer = window.setInterval(function () {
      refreshNotifications(false);
    }, 4000);

    window.addEventListener('beforeunload', function () {
      if (pollTimer) window.clearInterval(pollTimer);
    });
  }

  if (document.readyState === 'loading') {
    document.addEventListener('DOMContentLoaded', function () {
      init();
      initHeaderAutoHide();
      initNotificationsPopover();
    });
  } else {
    init();
    initHeaderAutoHide();
    initNotificationsPopover();
  }
})();
