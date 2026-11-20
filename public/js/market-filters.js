(function () {
  if (window.__codentraMarketFiltersBound) return;
  window.__codentraMarketFiltersBound = true;

  var form = document.getElementById('marketFilterForm');
  var grid = document.getElementById('marketGrid');
  if (!form || !grid) return;

  var search = document.getElementById('marketSearch');
  var category = document.getElementById('marketCategory');
  var technology = document.getElementById('marketTechnology');
  var sort = document.getElementById('marketSort');
  var count = document.getElementById('marketResultsCount');
  var empty = document.getElementById('marketFilterEmpty');
  var emptyReset = document.getElementById('marketEmptyReset');
  var cards = Array.from(grid.querySelectorAll('.market-project-card'));
  var dropdowns = Array.from(form.querySelectorAll('[data-market-dropdown]'));

  function normalize(value) {
    return String(value || '').trim().toLocaleLowerCase('ar');
  }

  function sortCards(items) {
    var mode = sort ? sort.value : 'featured';
    return items.sort(function (a, b) {
      if (mode === 'price-low') return Number(a.dataset.price || 0) - Number(b.dataset.price || 0);
      if (mode === 'price-high') return Number(b.dataset.price || 0) - Number(a.dataset.price || 0);
      if (mode === 'best-selling') return Number(b.dataset.sales || 0) - Number(a.dataset.sales || 0);
      if (mode === 'newest') return String(b.dataset.createdAt || '').localeCompare(String(a.dataset.createdAt || ''));
      return Number(b.dataset.featured || 0) - Number(a.dataset.featured || 0);
    });
  }

  function applyFilters() {
    var query = normalize(search && search.value);
    var selectedCategory = normalize(category && category.value);
    var selectedTechnology = normalize(technology && technology.value);
    var visible = cards.filter(function (card) {
      var haystack = normalize([card.dataset.title, card.dataset.description, card.dataset.category, card.dataset.technologies].join(' '));
      var matchesQuery = !query || haystack.includes(query);
      var matchesCategory = !selectedCategory || normalize(card.dataset.category) === selectedCategory;
      var matchesTechnology = !selectedTechnology || normalize(card.dataset.technologies).includes(selectedTechnology);
      return matchesQuery && matchesCategory && matchesTechnology;
    });

    sortCards(visible).forEach(function (card) {
      card.hidden = false;
      grid.appendChild(card);
    });
    cards.forEach(function (card) {
      if (!visible.includes(card)) card.hidden = true;
    });

    if (count) count.textContent = visible.length + ' مشروع';
    if (empty) empty.hidden = visible.length !== 0;
  }

  function closeDropdown(dropdown) {
    var trigger = dropdown.querySelector('.market-dropdown-trigger');
    var menu = dropdown.querySelector('.market-dropdown-menu');
    dropdown.classList.remove('is-open');
    if (trigger) trigger.setAttribute('aria-expanded', 'false');
    if (menu) menu.hidden = true;
  }

  function syncDropdown(dropdown) {
    var select = document.getElementById(dropdown.dataset.marketDropdown);
    var value = select ? select.value : '';
    var selected = Array.from(dropdown.querySelectorAll('[data-value]')).find(function (option) {
      return String(option.dataset.value || '') === String(value || '');
    });
    var valueLabel = dropdown.querySelector('.market-dropdown-value');
    if (valueLabel && selected) valueLabel.textContent = selected.textContent;
    dropdown.querySelectorAll('[role="option"]').forEach(function (option) {
      option.classList.toggle('is-selected', option === selected);
      option.setAttribute('aria-selected', option === selected ? 'true' : 'false');
    });
  }

  dropdowns.forEach(function (dropdown) {
    var trigger = dropdown.querySelector('.market-dropdown-trigger');
    var menu = dropdown.querySelector('.market-dropdown-menu');
    var select = document.getElementById(dropdown.dataset.marketDropdown);
    if (!trigger || !menu || !select) return;

    syncDropdown(dropdown);
    trigger.addEventListener('click', function () {
      var willOpen = !dropdown.classList.contains('is-open');
      dropdowns.forEach(closeDropdown);
      if (!willOpen) return;
      dropdown.classList.add('is-open');
      trigger.setAttribute('aria-expanded', 'true');
      menu.hidden = false;
    });
    menu.querySelectorAll('[role="option"]').forEach(function (option) {
      option.addEventListener('click', function () {
        select.value = option.dataset.value || '';
        syncDropdown(dropdown);
        closeDropdown(dropdown);
        select.dispatchEvent(new Event('change', { bubbles: true }));
      });
    });
  });

  document.addEventListener('click', function (event) {
    if (!event.target.closest('.market-dropdown')) dropdowns.forEach(closeDropdown);
  });

  [search, category, technology, sort].forEach(function (control) {
    if (!control) return;
    control.addEventListener('input', applyFilters);
    control.addEventListener('change', applyFilters);
  });

  form.addEventListener('reset', function () {
    window.requestAnimationFrame(function () {
      dropdowns.forEach(syncDropdown);
      applyFilters();
    });
  });
  if (emptyReset) emptyReset.addEventListener('click', function () {
    form.reset();
    applyFilters();
  });
  applyFilters();
})();
