# Guía pública de instalación de Hollywood App

Enlace para enviar al usuario: `https://www.hollywoodpacas.ovh/instalar`.

La pantalla no requiere iniciar sesión y solo se sirve en la instancia Pacas Holl. Es independiente del menú y no cambia permisos, usuarios ni datos del sistema.

Incluye tres pasos navegables para iPhone/iPad, ilustraciones identificadas como ejemplos y ayuda para encontrar Agregar a Inicio. Detecta Android para presentar el APK existente y el modo standalone para ofrecer entrada directa. En navegadores internos reconocibles de iPhone muestra ayuda para copiar el enlace y abrirlo en Safari. La detección por agente de usuario es orientativa; ambos sistemas pueden seleccionarse manualmente.

Compartir utiliza Web Share cuando está disponible. Copiar utiliza el portapapeles en HTTPS; si no está disponible, muestra un campo seleccionable. La guía no muestra un falso estado de instalación: el usuario confirma la adición en los menús de Safari. El nombre, icono y página de inicio son los del manifiesto actual; no se instala una cuenta ni se distribuyen credenciales.

Verificación: `cd backend && .venv/bin/python -m tests.browser_hollywood_install`. Usa un servidor local aislado y WebKit con vista de iPhone; cubre 320–430 px, pasos, ayuda, compartir/copiar, fallback, navegador interno, Android y modo standalone. No sustituye la comprobación manual del menú real de un iPhone.
