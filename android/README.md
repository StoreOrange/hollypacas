# Hollywood App para Android

Contenedor Trusted Web Activity generado con Bubblewrap 1.27.0 para la web de Pacas Holl. No copia datos de negocio ni contiene credenciales. Abre `https://www.hollywoodpacas.ovh/`, con el inicio de sesión y permisos del servidor. Requiere conexión y un navegador compatible; si la verificación del dominio falla, el navegador muestra su barra.

- Nombre: Hollywood App.
- Paquete: `ni.hollywoodpacas.app`.
- Versión inicial: 1.0.0, código 1.
- Android mínimo: 7 (API 24); compilación/objetivo: API 36.
- Logo: el mismo de Pacas Holl, con lienzo cuadrado blanco para conservar su proporción.
- Asociación pública: `https://www.hollywoodpacas.ovh/.well-known/assetlinks.json`.
- Certificado público: `backend/app/static/hollywood-app/assetlinks.json`.

## Compilar y firmar

Se necesitan JDK 21, Android SDK 36 y Build Tools 36.0.0. Configurar `android/local.properties` con `sdk.dir=/ruta/al/sdk`. Luego, desde esta carpeta:

```sh
./gradlew assembleRelease
```

La compilación genera un APK sin firmar en `app/build/outputs/apk/release/`. Alinear con `zipalign` y firmar con `apksigner` usando la clave de distribución. Verificar con `apksigner verify --verbose --print-certs` y comprobar que SHA-256 coincida con `assetlinks.json`.

La clave y su contraseña se conservan exclusivamente en `~/.codex/private/hollywood-app/`, con permisos restringidos. No se incluyen en Git, APK ni servidor web. Conservar una copia privada segura: las futuras actualizaciones del mismo paquete deben usar esa clave. No reemplazar el certificado público sin coordinar la firma del APK.

Las bibliotecas generadas por Bubblewrap conservan sus encabezados Apache 2.0. El proyecto usa Google Maven y Maven Central. Se corrigió la representación de `assetStatements` del generador para que sea una única lista JSON válida; al regenerar, conservar esa corrección.

## Comprobaciones

- Pruebas de manifest y aislamiento entre empresas: `backend/tests/test_hollywood_app.py`.
- Validar firma y metadatos del APK compilado.
- Comprobar el manifiesto público, iconos y asociación HTTPS en la VPS.
- La validación automática no sustituye la prueba de instalación, impresión y notificaciones en un Android físico.

## Instalar

Descargar el APK en Android y permitir la instalación desde el navegador si Android lo solicita. También se puede instalar directamente la web desde el menú de Chrome. Si el acceso anterior sigue llamándose Preventas, quitarlo y volver a instalar desde la web actualizada. No se publica automáticamente en Google Play.
