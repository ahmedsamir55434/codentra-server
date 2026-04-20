(function () {
  const shareForms = document.querySelectorAll('[data-share-form]');
  if (!shareForms.length) return;

  shareForms.forEach((form) => {
    form.addEventListener('submit', async (event) => {
      const rawUrl = form.getAttribute('data-share-url');
      if (!rawUrl) return;

      const absoluteUrl = new URL(rawUrl, window.location.origin).toString();
      let alreadySubmitted = false;

      const submitForm = () => {
        if (alreadySubmitted) return;
        alreadySubmitted = true;
        form.submit();
      };

      if (navigator.share) {
        event.preventDefault();
        try {
          await navigator.share({ url: absoluteUrl });
        } catch (error) {
          // Fall back to clipboard or a normal submit if the share sheet is cancelled.
        }
        submitForm();
        return;
      }

      if (navigator.clipboard && navigator.clipboard.writeText) {
        event.preventDefault();
        try {
          await navigator.clipboard.writeText(absoluteUrl);
        } catch (error) {
          // Ignore clipboard failures and still record the share.
        }
        submitForm();
      }
    });
  });
})();
