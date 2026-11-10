(() => {
  'use strict';
  const ua = navigator.userAgent;
  const ios = /iPhone|iPad|iPod/i.test(ua) || (navigator.platform === 'MacIntel' && navigator.maxTouchPoints > 1);
  const android = /Android/i.test(ua);
  const installed = window.matchMedia('(display-mode: standalone)').matches || navigator.standalone === true;
  const url = new URL('/instalar', window.location.origin).href;
  const status = document.querySelector('#link-status');
  let step = 0;
  function setStep(value, focus) {
    step = Math.max(0, Math.min(2, value));
    document.querySelectorAll('[data-panel]').forEach(panel => { panel.hidden = Number(panel.dataset.panel) !== step; });
    document.querySelectorAll('[data-step]').forEach(button => {
      if (Number(button.dataset.step) === step) button.setAttribute('aria-current', 'step');
      else button.removeAttribute('aria-current');
    });
    document.querySelector('#previous').hidden = step === 0;
    document.querySelector('#next').textContent = step === 2 ? '¿Dónde encuentro mi icono?' : 'Siguiente paso →';
    document.querySelector('#guide-status').textContent = '';
    if (focus) {
      const heading = document.querySelector(`[data-panel="${step}"] h3`);
      heading.tabIndex = -1; heading.focus({preventScroll: true});
      heading.scrollIntoView({block: 'start', behavior: window.matchMedia('(prefers-reduced-motion: reduce)').matches ? 'auto' : 'smooth'});
    }
  }
  function setDevice(device) {
    document.querySelector('#iphone-guide').hidden = device !== 'iphone';
    document.querySelector('#android-guide').hidden = device !== 'android';
    document.querySelectorAll('[data-device]').forEach(button => button.setAttribute('aria-pressed', String(button.dataset.device === device)));
  }
  document.querySelectorAll('[data-device]').forEach(button => button.addEventListener('click', () => setDevice(button.dataset.device)));
  document.querySelectorAll('[data-step]').forEach(button => button.addEventListener('click', () => setStep(Number(button.dataset.step), true)));
  document.querySelector('#previous').addEventListener('click', () => setStep(step - 1, true));
  document.querySelector('#next').addEventListener('click', () => {
    if (step < 2) setStep(step + 1, true);
    else document.querySelector('#guide-status').textContent = 'Después de confirmar en Safari, vuelve a la pantalla de inicio y busca Hollywood App. Si aún no aparece, revisa el paso 2. Esta guía no instala el icono por sí sola.';
  });
  async function copyLink() {
    try {
      if (!navigator.clipboard?.writeText) throw new Error('Clipboard unavailable');
      await navigator.clipboard.writeText(url);
      status.textContent = 'Enlace copiado. Puedes pegarlo en Safari o enviarlo por WhatsApp.';
    } catch (_) {
      const field = document.querySelector('#copy-fallback');
      field.hidden = false; field.value = url; field.focus(); field.select();
      status.textContent = 'Mantén pulsado el enlace y elige Copiar.';
    }
  }
  document.querySelectorAll('[data-copy]').forEach(button => button.addEventListener('click', copyLink));
  document.querySelector('#share-guide').addEventListener('click', async () => {
    if (!navigator.share) { await copyLink(); return; }
    try { await navigator.share({title:'Hollywood App',text:'Agrega Hollywood App al inicio de tu teléfono. Sigue esta guía:',url}); }
    catch (error) { if (error.name !== 'AbortError') await copyLink(); }
  });
  document.querySelector('#browser-help').hidden = !(ios && (!/Safari/i.test(ua) || /CriOS|FxiOS|EdgiOS|GSA|FBAN|FBAV|Instagram|WhatsApp/i.test(ua)));
  document.querySelector('#standalone-panel').hidden = !installed;
  document.querySelector('#setup-panel').hidden = installed;
  setDevice(android ? 'android' : 'iphone');
  setStep(0, false);
})();
