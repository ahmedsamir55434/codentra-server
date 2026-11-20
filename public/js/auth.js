(function () {
  if (window.__codentraAuthBound) return;
  window.__codentraAuthBound = true;

  function showError(form, message) {
    var box = form.querySelector('.auth-live-error');
    if (!box) {
      box = document.createElement('div');
      box.className = 'error-message auth-live-error';
      form.parentNode.insertBefore(box, form);
    }
    box.textContent = message || 'تعذر إكمال العملية الآن. حاول مرة أخرى.';
    box.hidden = false;
  }

  document.querySelectorAll('form[action="/login"], form[action="/register"]').forEach(function (form) {
    form.addEventListener('submit', async function (event) {
      event.preventDefault();
      var button = form.querySelector('button[type="submit"]');
      var originalLabel = button ? button.textContent : '';
      if (button) {
        button.disabled = true;
        button.textContent = form.action.endsWith('/register') ? 'جاري إنشاء الحساب...' : 'جاري تسجيل الدخول...';
      }

      try {
        var response = await fetch(form.action, {
          method: 'POST',
          headers: {
            Accept: 'application/json',
            'Content-Type': 'application/x-www-form-urlencoded;charset=UTF-8'
          },
          body: new URLSearchParams(new FormData(form)).toString(),
          credentials: 'same-origin',
          cache: 'no-store'
        });
        var payload = await response.json();
        if (!response.ok || !payload.ok) throw new Error(payload.error || 'تعذر إكمال العملية الآن');
        if (button) button.textContent = 'تم بنجاح';
        // The server saves the session before answering; a normal navigation
        // now renders the authenticated header immediately without a manual refresh.
        window.location.replace(payload.redirect || '/');
      } catch (error) {
        showError(form, error.message);
        if (button) {
          button.disabled = false;
          button.textContent = originalLabel;
        }
      }
    });
  });
})();
