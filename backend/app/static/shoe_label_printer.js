/* Client-side printer bridge: destinations belong to the browser's computer. */
window.ShoeLabelPrinter = (() => {
  async function connect() {
    if (!window.qz) throw new Error('No se pudo cargar el conector de impresión. Recargue la página.');
    if (!qz.websocket.isActive()) {
      try { await qz.websocket.connect(); }
      catch (_) { throw new Error('Abra QZ Tray en esta PC y permita la conexión. Si no está instalado, use el enlace de instalación.'); }
    }
  }
  async function list() {
    await connect();
    const names = await qz.printers.find();
    return names.map(name => ({id: name, name, enabled: true}));
  }
  async function print(blob, printer, widthMm=50, heightMm=20) {
    await connect();
    const names = await qz.printers.find();
    if (!names.includes(printer)) throw new Error('La impresora seleccionada ya no está disponible en esta PC. Actualice la lista.');
    const data = await new Promise((resolve, reject) => {
      const reader = new FileReader();
      reader.onerror = () => reject(new Error('No se pudo leer el PDF de etiquetas.'));
      reader.onload = () => resolve(reader.result.split(',')[1]);
      reader.readAsDataURL(blob);
    });
    const config = qz.configs.create(printer, {
      units: 'in', size: {width: widthMm / 25.4, height: heightMm / 25.4}, margins: 0,
      copies: 1, scaleContent: false, rasterize: true,
      orientation: 'portrait', jobName: 'Miss Zapatos - Etiquetas'
    });
    await qz.print(config, [{type: 'pixel', format: 'pdf', flavor: 'base64', data}]);
  }
  return {list, print};
})();
