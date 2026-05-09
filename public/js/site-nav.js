(function () {
  var menuToggle = document.getElementById('navMenuToggle');
  var mobilePanel = document.getElementById('mobileNavPanel');
  if (!menuToggle || !mobilePanel) return;

  var closeButtons = mobilePanel.querySelectorAll('[data-close-nav]');
  var mobileThemeToggle = document.getElementById('mobileThemeToggle');
  var desktopThemeToggle = document.getElementById('themeToggle');
  var desktopMore = document.querySelector('.nav-more-menu');

  function setOpen(isOpen) {
    mobilePanel.hidden = !isOpen;
    mobilePanel.classList.toggle('is-open', isOpen);
    menuToggle.classList.toggle('is-open', isOpen);
    menuToggle.setAttribute('aria-expanded', isOpen ? 'true' : 'false');
    document.body.classList.toggle('nav-panel-open', isOpen);
  }

  menuToggle.addEventListener('click', function () {
    setOpen(mobilePanel.hidden);
  });

  closeButtons.forEach(function (button) {
    button.addEventListener('click', function () {
      setOpen(false);
    });
  });

  mobilePanel.querySelectorAll('a').forEach(function (link) {
    link.addEventListener('click', function () {
      setOpen(false);
    });
  });

  document.addEventListener('keydown', function (event) {
    if (event.key === 'Escape') {
      setOpen(false);
      if (desktopMore) {
        desktopMore.removeAttribute('open');
      }
    }
  });

  window.addEventListener('resize', function () {
    if (window.innerWidth > 960) {
      setOpen(false);
    }
  });

  if (desktopMore) {
    document.addEventListener('click', function (event) {
      if (!desktopMore.contains(event.target)) {
        desktopMore.removeAttribute('open');
      }
    });
  }

  if (mobileThemeToggle && desktopThemeToggle) {
    mobileThemeToggle.addEventListener('click', function () {
      desktopThemeToggle.click();
    });
  }

  var desktopNav = document.querySelector('.smart-nav');
  var desktopPrimary = document.querySelector('.nav-primary');
  var desktopActions = document.querySelector('.nav-actions');
  var desktopMoreMenu = document.querySelector('.nav-more-menu');

  function getMorePanel() {
    return desktopMoreMenu ? desktopMoreMenu.querySelector('.nav-more-panel') : null;
  }

  function clearOverflowLinks() {
    if (!desktopPrimary || !desktopMoreMenu) return;
    var panel = getMorePanel();
    if (!panel) return;

    panel.querySelectorAll('a[data-overflow-link="1"]').forEach(function (a) {
      a.remove();
    });

    desktopPrimary.querySelectorAll('a.nav-pill[data-overflow-hidden="1"]').forEach(function (a) {
      a.style.display = '';
      a.removeAttribute('data-overflow-hidden');
    });
  }

  function measureAvailableWidth() {
    if (!desktopNav || !desktopActions) return 0;
    var navWidth = desktopNav.getBoundingClientRect().width;
    var actionsWidth = desktopActions.getBoundingClientRect().width;
    var reservedGap = 18;
    return Math.max(0, navWidth - actionsWidth - reservedGap);
  }

  function ensureDesktopOverflow() {
    if (!desktopPrimary || !desktopMoreMenu) return;
    if (window.innerWidth <= 960) {
      desktopMoreMenu.style.display = 'none';
      clearOverflowLinks();
      return;
    }

    clearOverflowLinks();

    var panel = getMorePanel();
    if (!panel) return;

    var available = measureAvailableWidth();
    if (!available) {
      desktopMoreMenu.style.display = 'none';
      return;
    }

    var pills = Array.prototype.slice.call(desktopPrimary.querySelectorAll('a.nav-pill'));
    var total = 0;
    var overflow = [];

    pills.forEach(function (pill) {
      pill.style.display = '';
    });

    pills.forEach(function (pill) {
      var pillWidth = pill.getBoundingClientRect().width;
      total += pillWidth;
      if (total > available) {
        overflow.push(pill);
      }
    });

    if (overflow.length === 0) {
      if (panel.querySelectorAll('a.nav-more-link').length > 0) {
        desktopMoreMenu.style.display = '';
      } else {
        desktopMoreMenu.style.display = 'none';
      }
      return;
    }

    desktopMoreMenu.style.display = '';
    overflow.forEach(function (pill) {
      var clone = pill.cloneNode(true);
      clone.classList.remove('nav-pill');
      clone.classList.add('nav-more-link');
      clone.removeAttribute('role');
      clone.setAttribute('data-overflow-link', '1');
      panel.insertBefore(clone, panel.firstChild);

      pill.style.display = 'none';
      pill.setAttribute('data-overflow-hidden', '1');
    });
  }

  window.addEventListener('resize', function () {
    ensureDesktopOverflow();
  });

  setTimeout(function () {
    ensureDesktopOverflow();
  }, 0);
})();
