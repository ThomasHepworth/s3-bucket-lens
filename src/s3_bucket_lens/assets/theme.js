(function () {
  "use strict";
  const storageKey = "s3-bucket-lens-theme";
  let preference = "system";
  try { preference = localStorage.getItem(storageKey) || "system"; } catch {}
  if (!["system", "light", "dark"].includes(preference)) preference = "system";

  function applyTheme(value) {
    preference = value;
    document.documentElement.dataset.theme = value;
    document.querySelectorAll("[data-theme-option]").forEach((button) => {
      button.setAttribute("aria-pressed", String(button.dataset.themeOption === value));
    });
  }

  applyTheme(preference);
  document.addEventListener("DOMContentLoaded", () => applyTheme(preference));
  document.addEventListener("click", (event) => {
    const button = event.target.closest("[data-theme-option]");
    if (!button) return;
    applyTheme(button.dataset.themeOption);
    try { localStorage.setItem(storageKey, preference); } catch {}
  });
}());