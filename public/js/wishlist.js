(function () {
  if (window.__codentraWishlistBound) return;
  window.__codentraWishlistBound = true;

  function setWishlistState(button, isWishlisted) {
    button.classList.toggle('is-active', isWishlisted);
    button.textContent = isWishlisted ? '♥' : '♡';
    button.setAttribute('aria-pressed', isWishlisted ? 'true' : 'false');
    button.setAttribute('aria-label', isWishlisted ? 'إزالة من المفضلة' : 'إضافة للمفضلة');
  }

  document.addEventListener('submit', function (event) {
    var form = event.target.closest('.wishlist-form');
    if (!form || form.dataset.pending === 'true') return;

    var button = form.querySelector('.wishlist-btn');
    if (!button) return;

    event.preventDefault();
    form.dataset.pending = 'true';
    var previousState = button.classList.contains('is-active');
    var nextState = !previousState;
    setWishlistState(button, nextState);
    button.classList.add('is-pending');
    button.disabled = true;

    fetch(form.action, {
      method: 'POST',
      headers: {
        Accept: 'application/json',
        'X-Requested-With': 'XMLHttpRequest',
        'Content-Type': 'application/x-www-form-urlencoded;charset=UTF-8'
      },
      body: new URLSearchParams(new FormData(form)).toString(),
      credentials: 'same-origin'
    })
      .then(function (response) {
        var contentType = response.headers.get('content-type') || '';
        if (!response.ok || !contentType.includes('application/json')) {
          throw new Error('wishlist-update-failed');
        }
        return response.json();
      })
      .then(function (data) {
        setWishlistState(button, Boolean(data.isWishlisted));
      })
      .catch(function () {
        setWishlistState(button, previousState);
      })
      .finally(function () {
        form.dataset.pending = 'false';
        button.classList.remove('is-pending');
        button.disabled = false;
      });
  });
})();
