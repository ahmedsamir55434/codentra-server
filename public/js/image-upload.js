(function () {
  if (window.__codentraImageUploadBound) return;
  window.__codentraImageUploadBound = true;

  var MAX_DIMENSION = 1400;
  var MAX_BYTES = 850 * 1024;
  var MIN_QUALITY = 0.42;

  function getStatus(form) {
    var status = form.querySelector('.image-upload-status');
    if (!status) {
      status = document.createElement('small');
      status.className = 'image-upload-status';
      status.hidden = true;
      form.appendChild(status);
    }
    return status;
  }

  function canvasBlob(canvas, quality) {
    return new Promise(function (resolve, reject) {
      canvas.toBlob(function (blob) {
        if (blob) resolve(blob);
        else reject(new Error('image-compression-failed'));
      }, 'image/webp', quality);
    });
  }

  function loadImage(file) {
    if (window.createImageBitmap) {
      return window.createImageBitmap(file);
    }
    return new Promise(function (resolve, reject) {
      var image = new Image();
      var url = URL.createObjectURL(file);
      image.onload = function () {
        URL.revokeObjectURL(url);
        resolve(image);
      };
      image.onerror = function () {
        URL.revokeObjectURL(url);
        reject(new Error('image-read-failed'));
      };
      image.src = url;
    });
  }

  async function compressImage(file) {
    if (!file || !String(file.type || '').startsWith('image/') || file.size <= MAX_BYTES) return file;
    var image = await loadImage(file);
    var sourceWidth = image.width || image.naturalWidth;
    var sourceHeight = image.height || image.naturalHeight;
    if (!sourceWidth || !sourceHeight) return file;

    var ratio = Math.min(1, MAX_DIMENSION / Math.max(sourceWidth, sourceHeight));
    var canvas = document.createElement('canvas');
    canvas.width = Math.max(1, Math.round(sourceWidth * ratio));
    canvas.height = Math.max(1, Math.round(sourceHeight * ratio));
    var context = canvas.getContext('2d');
    context.drawImage(image, 0, 0, canvas.width, canvas.height);

    var quality = 0.72;
    var blob = await canvasBlob(canvas, quality);
    while (blob.size > MAX_BYTES && quality > MIN_QUALITY) {
      quality -= 0.08;
      blob = await canvasBlob(canvas, quality);
    }

    if (window.ImageBitmap && image instanceof window.ImageBitmap) image.close();
    if (blob.size >= file.size && file.type === 'image/webp') return file;
    var name = String(file.name || 'image').replace(/\.[^.]+$/, '') + '.webp';
    return new File([blob], name, { type: 'image/webp', lastModified: Date.now() });
  }

  function replaceInputFiles(input, files) {
    if (typeof DataTransfer === 'undefined') return false;
    var transfer = new DataTransfer();
    files.forEach(function (file) { transfer.items.add(file); });
    input.files = transfer.files;
    return true;
  }

  async function optimizeForm(form) {
    var inputs = Array.from(form.querySelectorAll('input[type="file"]')).filter(function (input) {
      return Array.from(input.files || []).some(function (file) { return String(file.type || '').startsWith('image/'); });
    });
    if (!inputs.length) return false;

    var changed = false;
    for (var input of inputs) {
      var optimizedFiles = [];
      for (var file of Array.from(input.files || [])) {
        var optimized = await compressImage(file);
        optimizedFiles.push(optimized);
        changed = changed || optimized !== file;
      }
      if (changed) replaceInputFiles(input, optimizedFiles);
    }
    return changed;
  }

  document.addEventListener('change', function (event) {
    var input = event.target.closest('input[type="file"]');
    if (input && input.form) input.form.dataset.imagesOptimized = 'false';
  }, true);

  document.addEventListener('submit', async function (event) {
    var form = event.target.closest('form');
    if (!form || form.dataset.imagesOptimized === 'true' || form.dataset.imagesOptimized === 'processing') return;
    var hasImages = Array.from(form.querySelectorAll('input[type="file"]')).some(function (input) {
      return Array.from(input.files || []).some(function (file) { return String(file.type || '').startsWith('image/'); });
    });
    if (!hasImages) return;

    event.preventDefault();
    form.dataset.imagesOptimized = 'processing';
    var status = getStatus(form);
    status.textContent = 'جاري تجهيز الصور بحجم خفيف للرفع...';
    status.hidden = false;
    try {
      var changed = await optimizeForm(form);
      status.textContent = changed ? 'تم ضغط الصور تلقائيًا لتسريع الرفع.' : 'الصور جاهزة للرفع.';
      form.dataset.imagesOptimized = 'true';
    } catch (error) {
      status.textContent = 'تعذر ضغط الصور. جرّب JPG أو PNG بحجم أصغر.';
      form.dataset.imagesOptimized = 'false';
      return;
    }

    if (typeof form.requestSubmit === 'function') {
      form.requestSubmit(event.submitter || form.querySelector('button[type="submit"]'));
    } else {
      form.submit();
    }
  }, true);
})();
