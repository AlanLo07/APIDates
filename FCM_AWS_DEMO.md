# Demo de notificaciones FCM + AWS

Esta demo conecta Firebase Cloud Messaging con el backend actual: API Gateway HTTP, Cognito, Lambda y DynamoDB. Solo se agregaron artefactos locales; no se desplegaron recursos ni se crearon credenciales.

## Alcance

- `POST /notifications/devices`: registra el Firebase Installation ID (FID) del dispositivo autenticado.
- `DELETE /notifications/devices`: elimina el FID del usuario autenticado.
- `POST /notifications/test`: manda un push de prueba a los dispositivos registrados de ese usuario.
- El `sub` siempre se toma del JWT de Cognito; el backend ignora cualquier `userId` enviado por el cliente.
- La demo no agenda recordatorios de citas. Primero se debe definir una fecha-hora con zona horaria; el modelo actual de citas conserva `date` como `dd-mm-yyyy`.

## Configuración requerida

1. En Firebase Console, habilita Firebase Cloud Messaging para el proyecto y configura las aplicaciones Android, iOS y Web. Para iOS, configura también APNs; para web, configura la clave VAPID y el service worker de Firebase.
2. Crea una cuenta de servicio dedicada con permisos mínimos para enviar mensajes FCM. No uses una clave de propietario del proyecto.
3. Guarda el JSON de esa cuenta de servicio como el valor de un secreto en AWS Secrets Manager, en la misma región del stack. No agregues el JSON al repositorio, a variables del cliente ni a logs.
4. Despliega la plantilla SAM de forma deliberada con `DeployFcmDemo=true` y `FcmCredentialsSecretArn=<ARN del secreto>`. Con el valor predeterminado `false`, la función y la tabla nuevas no se crean.
5. El cliente debe llamar a las rutas con `Authorization: Bearer <access token de Cognito>`.

El rol de Lambda obtiene permiso de lectura únicamente sobre el ARN configurado en `FcmCredentialsSecretArn`. Los FID se guardan en `NotificationDevices`; su clave de partición es un SHA-256 del FID, y el índice `userId-index` permite enviar solo a dispositivos del usuario autenticado. La demo usa FID porque Firebase recomienda este identificador; la propiedad `token` del Admin SDK está deprecada.

## Liberación paso a paso

### 1. Preparar Firebase

1. Seleccionar o crear el proyecto Firebase que corresponde a APIDates.
2. En **Settings > General**, registrar las aplicaciones Android, iOS y Web que realmente se publicarán. Los identificadores deben coincidir con `applicationId` de Android, Bundle ID de iOS y dominio/origen de Web.
3. En **Settings > Cloud Messaging**, habilitar FCM HTTP v1.
4. Crear una cuenta de servicio dedicada para el backend. Asignarle el rol **Firebase Cloud Messaging API Admin** en el proyecto objetivo. No usar una cuenta personal ni una clave de propietario.
5. Descargar el JSON de esa cuenta de servicio y guardarlo como un secreto JSON en AWS Secrets Manager, región `us-east-2`. No guardar ese JSON en Flutter, en este repositorio ni en logs.

### 2. Configurar FCM por plataforma

| Plataforma | Configuración necesaria |
| --- | --- |
| Android | Registrar el package/application ID correcto en Firebase; `flutterfire configure` genera la configuración Flutter. Probar en Android 7+ con Google Play Services. Android 13+ requiere permiso de notificaciones. |
| iOS | Registrar Bundle ID; subir a Firebase la clave APNs `.p8`, Key ID y Team ID; habilitar **Push Notifications** y **Background Modes > Remote notifications** en el target de Xcode. Probar en un dispositivo físico. |
| Web | Registrar la app web y el origen; generar una clave pública VAPID en Cloud Messaging; servir por HTTPS; configurar y desplegar `firebase-messaging-sw.js`. El navegador pide permiso al usuario. |

Para desarrollo y producción, verificar los entitlements, perfiles de firma y credenciales APNs del entorno correspondiente. No publicar la clave privada APNs ni la cuenta de servicio.

### 3. Revisar y desplegar AWS

El script actual `lambdas/deploy.ps1` usa `--no-confirm-changeset` y no pasa los parámetros de FCM. **No lo uses para esta primera liberación**, porque omite la revisión interactiva. El despliegue modifica el stack completo de SAM; antes hay que confirmar cuenta, stack y ambiente. La plantilla usa nombres físicos fijos para varios recursos, así que no crear un segundo stack en la misma cuenta sin parametrizar esos nombres.

Desde la carpeta `lambdas`, ejecuta localmente:

```powershell
sam validate --lint
sam build
sam deploy `
  --stack-name <STACK_EXISTENTE_NO_PRODUCCION> `
  --region us-east-2 `
  --capabilities CAPABILITY_IAM `
  --resolve-s3 `
  --parameter-overrides `
    DeployFcmDemo=true `
    FcmCredentialsSecretArn=<ARN_DEL_SECRET_EN_US_EAST_2>
```

Usa una cuenta/stack de pruebas antes de producción. No agregues `--no-confirm-changeset`: inspecciona el change set y confirma que no proponga reemplazos o eliminaciones ajenos a FCM. Si no existe un stack de desarrollo aislado, detente y prepara primero un entorno de staging o parametriza los nombres fijos. Con `DeployFcmDemo=false` (valor predeterminado) los recursos nuevos no se crean.

Al terminar, anota el output `FcmDemoEndpoint` del stack y comprueba que la función tenga permiso para leer solo el secreto indicado. Si el secreto usa una KMS key administrada por el cliente, el rol también necesitará `kms:Decrypt` limitado a esa clave.

### 4. Integrar Flutter

1. Desde el proyecto Flutter, agregar `firebase_core` y `firebase_messaging`, y ejecutar `flutterfire configure` seleccionando solo las plataformas que soporta el producto. El archivo generado `firebase_options.dart` contiene identificadores de configuración, no la clave privada del service account.
2. Inicializar Firebase antes de usar Messaging.
3. Pedir permiso en un momento contextual de la experiencia (no necesariamente en el primer arranque). No intentar registrar el dispositivo en AWS hasta que haya una sesión Cognito.
4. Escuchar `FirebaseMessaging.instance.onRegistered`; ese evento entrega el FID recomendado. Después de autenticar al usuario, enviar `{ "fid": ..., "platform": ... }` a `POST /notifications/devices` con `Authorization: Bearer <access-token de Cognito>`.
5. Al iniciar sesión, volver a sincronizar el FID actual con el backend. Al cerrar sesión, llamar `DELETE /notifications/devices` antes de descartar la sesión y luego limpiar cualquier estado local asociado.
6. Implementar recepción en foreground (`FirebaseMessaging.onMessage`), el handler top-level de background y la navegación al tocar una notificación (`getInitialMessage` y `onMessageOpenedApp`).

Ejemplo mínimo del registro FID en Flutter:

```dart
final messaging = FirebaseMessaging.instance;
String? latestFid;

Future<void> syncFidIfAuthenticated() async {
  final fid = latestFid;
  if (fid == null) return;
  final accessToken = await authSession.getAccessToken();
  if (accessToken == null) return;
  await notificationsApi.registerDevice(
    accessToken: accessToken,
    fid: fid,
    platform: kIsWeb ? 'web' : (defaultTargetPlatform == TargetPlatform.iOS ? 'ios' : 'android'),
  );
}

final registrationSubscription = messaging.onRegistered.listen((fid) {
  latestFid = fid;
  syncFidIfAuthenticated();
});

await messaging.requestPermission();
await messaging.register(
  vapidKey: kIsWeb ? '<CLAVE_PUBLICA_VAPID>' : null,
);
// Llamar también a syncFidIfAuthenticated() al completar el login Cognito.
```

`authSession` y `notificationsApi` representan los servicios propios de la app; no son APIs de Firebase. Mantener la suscripción mientras viva el servicio de notificaciones y cancelarla al desecharlo. Para Web, configura también el service worker conforme a la guía de FlutterFire.

### 5. Probar en staging

1. Instalar builds de desarrollo en un dispositivo Android y uno iOS físico; probar Web en un navegador compatible y sobre HTTPS.
2. Iniciar sesión con Cognito y aceptar permisos.
3. Confirmar que el cliente llama `POST /notifications/devices` y recibe `201`.
4. Llamar `POST /notifications/test` y comprobar recepción con app en background y foreground.
5. Verificar el comportamiento al tocar el push, denegar permiso, cerrar sesión, reinstalar y rotar el FID.
6. Confirmar que el endpoint de prueba solo envía al usuario dueño del JWT, y que errores/logs no contienen FID, título/cuerpo sensible ni JSON de credenciales.
7. Probar `DELETE /notifications/devices` y confirmar que elimina solo el registro vinculado al usuario autenticado.

No habilitar recordatorios automáticos en esta etapa: primero hace falta definir fecha-hora/zona horaria en el modelo de citas y añadir el scheduler.

## Probar la API

Registrar un dispositivo:

```http
POST /notifications/devices
Authorization: Bearer <cognito-access-token>
Content-Type: application/json

{
  "fid": "<firebase-installation-id>",
  "platform": "android"
}
```

`platform` acepta `android`, `ios` o `web`. El servidor no devuelve ni registra el token en logs.

Enviar un push de prueba al usuario actual:

```http
POST /notifications/test
Authorization: Bearer <cognito-access-token>
Content-Type: application/json

{
  "title": "APIDates",
  "body": "Notificaciones conectadas"
}
```

El body puede omitirse para usar un mensaje de prueba predeterminado. La respuesta incluye únicamente los conteos de envíos exitosos y fallidos.

Eliminar el registro al cerrar sesión o retirar permisos:

```http
DELETE /notifications/devices
Authorization: Bearer <cognito-access-token>
Content-Type: application/json

{
  "fid": "<firebase-installation-id>"
}
```

## Integración Flutter

Agregar y configurar `firebase_core` y `firebase_messaging` según la guía oficial de FlutterFire. Inicializar Firebase con las opciones generadas para cada plataforma, pedir permiso y registrar el FID con `onRegistered`:

```dart
await Firebase.initializeApp(
  options: DefaultFirebaseOptions.currentPlatform,
);

final messaging = FirebaseMessaging.instance;
await messaging.requestPermission();
messaging.onRegistered.listen((fid) {
  // Enviar el FID a POST /notifications/devices después del login Cognito.
});
await messaging.register(
  vapidKey: kIsWeb ? '<web-vapid-public-key>' : null,
);
```

Enviar ese FID a `POST /notifications/devices` con el access token de Cognito. Determinar `platform` como `web`, `ios` o `android`. Volver a sincronizar al iniciar sesión y cada vez que `onRegistered` emita un FID; enviar el FID a la ruta DELETE al cerrar sesión. Registrar además un handler de mensajes en foreground; el push no reemplaza el estado de la aplicación ni una bandeja persistente.

Para web, servir la app por HTTPS y configurar el service worker y VAPID. En iOS, revisar permisos y credenciales APNs tanto para desarrollo como para producción.

## Siguientes pasos antes de producción

- Validar notificaciones en foreground/background y la apertura de deep links para cada plataforma.
- Definir preferencias por categoría, consentimiento, quiet hours y contenido que no debe aparecer en pantalla bloqueada.
- Añadir limitación de frecuencia al endpoint de prueba.
- Para recordatorios, almacenar un instante UTC, una zona IANA y la política del recordatorio; después elegir EventBridge Scheduler y cancelar/reprogramar al editar la cita.
- Si se requiere historial dentro de la app, persistir notificaciones aparte; FCM por sí solo no proporciona una bandeja de entrega durable.

## Referencias oficiales

- [Firebase Cloud Messaging](https://firebase.google.com/docs/cloud-messaging)
- [Configurar FCM en Flutter](https://firebase.google.com/docs/cloud-messaging/flutter/client)
- [Recibir mensajes y manejar interacciones](https://firebase.google.com/docs/cloud-messaging/flutter/receive)
- [Enviar con Firebase Admin SDK y dirigir a FID](https://firebase.google.com/docs/cloud-messaging/send/admin-sdk)
- [Ciclo de vida y actualización de FID](https://firebase.google.com/docs/cloud-messaging/manage-tokens)
- [AWS Secrets Manager](https://docs.aws.amazon.com/secretsmanager/latest/userguide/intro.html)