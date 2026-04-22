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
})();
