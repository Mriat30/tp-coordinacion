# Trabajo Práctico - Coordinación

En este trabajo se busca familiarizar a los estudiantes con los desafíos de la coordinación del trabajo y el control de la complejidad en sistemas distribuidos. Para tal fin se provee un esqueleto de un sistema de control de stock de una verdulería y un conjunto de escenarios de creciente grado de complejidad y distribución que demandarán mayor sofisticación en la comunicación de las partes involucradas.

## Ejecución

Los comandos siguientes se ejecutan desde el directorio del lenguaje elegido (por ejemplo `python/` o `golang/`), donde vive el `Makefile` y el `docker-compose` del escenario activo.

`make up` : Inicia los contenedores del sistema y comienza a seguir los logs de todos ellos en un solo flujo de salida.

`make down`:   Detiene los contenedores y libera los recursos asociados.

`make logs`: Sigue los logs de todos los contenedores en un solo flujo de salida.

`make test`: Inicia los contenedores del sistema, espera a que los clientes finalicen, compara los resultados con una ejecución serial y detiene los contenederes.

`make switch`: Permite alternar rápidamente entre los archivos de docker compose de los distintos escenarios provistos.

## Elementos del sistema objetivo

![ ](./imgs/diagrama_de_robustez.jpg  "Diagrama de Robustez")
*Fig. 1: Diagrama de Robustez*

### Client

Lee un archivo de entrada y envía por TCP/IP pares (fruta, cantidad) al sistema.
Cuando finaliza el envío de datos, aguarda un top de pares (fruta, cantidad) y vuelca el resultado en un archivo de salida csv.
El criterio y tamaño del top dependen de la configuración del sistema. Por defecto se trata de un top 3 de frutas de acuerdo a la cantidad total almacenada.

### Gateway

Es el punto de entrada y salida del sistema. Intercambia mensajes con los clientes y las colas internas utilizando distintos protocolos.

### Sum
 
Recibe pares  (fruta, cantidad) y aplica la función Suma de la clase `FruitItem`. Por defecto esa suma es la canónica para los números enteros, ej:

`("manzana", 5) + ("manzana", 8) = ("manzana", 13)`

Pero su implementación podría modificarse.
Cuando se detecta el final de la ingesta de datos envía los pares (fruta, cantidad) totales a los Aggregators.

### Aggregator

Consolida los datos de las distintas instancias de Sum.
Cuando se detecta el final de la ingesta, se calcula un top parcial y se envía esa información al Joiner.

### Joiner

Recibe tops parciales de las instancias del Aggregator.
Cuando se detecta el final de la ingesta, se envía el top final hacia el gateway para ser entregado al cliente.

## Implementación de referencia (Python): flujo, controladores y coordinación

Lo anterior describe el sistema *objetivo* a nivel de responsabilidades. En la carpeta `python/` hay una implementación concreta que usa **TCP** hacia el Gateway y **RabbitMQ** (colas y exchanges directos) entre etapas. El camino típico de un mensaje es: cliente → Gateway → cola hacia Sum → exchanges hacia Aggregation → cola hacia Join → cola hacia Gateway → cliente. Cada etapa puede replicarse según el `docker-compose` del escenario; la coordinación entre réplicas se resuelve con **identificador de cliente** (`client_id` en mensajes internos), **EOF** explícitos y **conteos de EOF** donde hace falta saber que “todas las partes” terminaron para un cliente.

### Client (casos y coordinación)

Además de lo ya indicado (CSV de entrada, top en salida), el cliente negocia un **ACK** por cada registro y por el fin de envío (`END_OF_RECODS`), y confirma la recepción del top. Eso acopla el flujo con el Gateway: el servidor no avanza de fase si el cliente no confirmó. Ante **SIGTERM** se marca el cierre, se hace shutdown del socket y se evita seguir escribiendo sobre un descriptor inválido.

### Gateway (casos y coordinación)

Cada conexión TCP recibe un `MessageHandler` con un **`client_id` único (UUID)** que viaja en todos los mensajes internos hacia Sum; así el resto del pipeline puede segregar estado por cliente sin mezclar sesiones. El proceso que atiende TCP serializa registros y EOF hacia la **cola de salida** hacia Sum. En paralelo, otro proceso consume la **cola de entrada** desde Join: por cada resultado recorre la lista compartida de sesiones (`multiprocessing.Manager`) hasta encontrar el `client_id` que coincide y entonces envía `FRUIT_TOP` por el socket correcto, espera ACK del cliente y **retira** esa sesión de la lista. La concurrencia entre clientes se aísla con un **pool de procesos** (no hilos): un cliente bloqueado en red no frena el accept ni el consumo de resultados de los demás.

### Sum (casos, hilos, locks y límites)

Cada réplica de Sum consume la **misma cola de datos** que alimenta el Gateway (competencia entre consumidores de RabbitMQ): cualquier instancia puede recibir registros de cualquier cliente. El estado local `data_per_client` acumula por `(client_id, fruta)` usando la suma de `FruitItem`. Al llegar un **EOF de datos** para un cliente, esa réplica vacía *su* copia del acumulado, envía los totales por fruta a los Aggregation mediante **hash de la fruta** (`zlib.crc32` módulo cantidad de aggregators, en lugar de `hash()` de Python, para ser determinista entre procesos) y envía **un EOF a cada instancia de Aggregation**, haya o no filas para ese cliente en esa réplica (los EOF alinean el conteo aguas abajo).

**Separación en dos etapas dentro del proceso: datos vs control.** Un **hilo principal** ejecuta `_run_data_consumer`: consume la cola del Gateway, actualiza el diccionario y, en EOF, hace flush hacia Aggregation y además **publica** el mismo EOF en el exchange de control `SUM_CONTROL_EXCHANGE` hacia las **otras** réplicas de Sum (todas las routing keys menos la propia). Un **hilo en segundo plano** (`_run_control_consumer`) consume **solo** la routing key de control de esta réplica: cuando otra Sum fue la que vio el EOF en la cola compartida, esta réplica igual debe vaciar su estado parcial para ese cliente y reenviar EOF a los Aggregation, aunque nunca hubiera visto el EOF por la cola de datos.

**Uso de `threading.Lock`.** Ambos hilos pueden tocar `data_per_client` (registros tardíos por la cola de datos vs flush ordenado por control). El lock envuelve la deserialización, la actualización o el `_process_eof` que borra la entrada del cliente; así no se intercalan un `_process_data` y un `_process_eof` del mismo cliente de forma inconsistente **dentro de esta réplica**. El envío a RabbitMQ se hace **fuera** del lock cuando ya se copió la lista a enviar, para no bloquear al otro hilo durante I/O.

**Qué no resuelven esos hilos ni el lock.** No existe barrera que espere a que **no queden mensajes en vuelo** para un cliente en la cola compartida antes de borrar estado: si una réplica recibe el EOF por control y hace flush, y **después** le llegan por la cola de datos registros viejos de ese mismo cliente, el estado ya fue eliminado y esos registros quedan fuera del agregado. El diseño prioriza **no bloquear** el consumo de la cola de datos con un único consumidor que también atienda control (de ahí el segundo hilo y el exchange aparte). Esa ventana de inconsistencia estricta frente a reordenamiento/cola es el trade-off explícito frente a consistencia “fuerte” con drenaje total.

**Alternativa en la rama `solucion_alternativa`.** Allí, si hay más de un Sum, la réplica con `ID == 0` levanta un **hilo enrutador** que lee la cola única del Gateway y **reenvía cada mensaje a una cola dedicada** por réplica Sum, eligiendo la réplica con `crc32(client_id) % SUM_AMOUNT`. Cada Sum solo consume su cola; un cliente queda **afinado** a una réplica. Con ello desaparece el estado parcial compartido entre Sum para un mismo cliente y el riesgo de “flush con mensajes tardíos” entre réplicas, a cambio de posible **desbalance de carga** (pocas sesiones fuertes vs muchas réplicas ociosas) y de la complejidad operativa del enrutador. En `master` se mantuvo el modelo de cola compartida más control por exchange.

### Aggregation (casos y coordinación)

Cada instancia escucha solo su binding del exchange de entrada (partición por fruta vía hash en Sum). Cuenta EOF **por cliente**: necesita **tantos EOF como réplicas de Sum** (`SUM_AMOUNT`) antes de considerar cerrado el bloque para ese cliente en *esta* instancia. Si tenía datos, calcula un **top parcial** (orden global de `FruitItem` y se toman las últimas `TOP_SIZE` invertidas) y lo envía a Join; **siempre** envía además un EOF a Join para que el Joiner pueda contar cuántos aggregators reportaron fin para ese cliente, incluso si este aggregator no aportó filas al top parcial.

### Joiner (casos y coordinación)

Acumula tops parciales (listas de `(fruta, cantidad)`) por `client_id`. Espera **tantos EOF como aggregators** (`AGGREGATION_AMOUNT`); al completarse, fusiona todos los `FruitItem` del cliente, recalcula el top global con el mismo criterio de tamaño y lo envía al Gateway en la cola de resultados. La coordinación entre aggregators es puramente por **conteo de EOF**, no por un orden global de mensajes entre sí.

### Middleware y errores

Los consumidores reciben callbacks con `ack` / `nack`: éxito confirma el mensaje; error registra y puede reencolar o detener el consumidor según el caso. Sum, Aggregation y Join registran **SIGTERM** dentro de un context manager, restauran el manejador anterior y cierran colas y exchanges de forma ordenada. El Gateway ante SIGTERM apaga el socket servidor y los sockets de clientes abiertos.

### Escalado y hashing

El reparto de frutas entre aggregators es determinista y barato (CRC32). El Gateway escala con clientes vía multiprocessing; Sum escala en **throughput de consumo** de la cola compartida, no en partición por cliente salvo que se adopte la variante con enrutador en otra rama. El Joiner y el Gateway de resultados son puntos centralizados por diseño del enunciado (un top final por cliente devuelto por la misma sesión TCP). En Sum, los dos hilos comparten el GIL de Python, pero el trabajo dominante son bloqueos de red sobre RabbitMQ; en la práctica el cuello de botella no suele ser el paralelismo de CPU sino la I/O con el broker.

## Limitaciones del esqueleto provisto

La implementación base respeta la división de responsabilidades de los distintos controles y hace uso de la clase `FruitItem` como un elemento opaco, sin asumir la implementación de las funciones de Suma y Comparación.

No obstante, esta implementación no cubre los objetivos buscados tal y como es presentada. Entre sus falencias puede destactarse que:

 - No se implementa la interfaz del middleware. 
 - No se dividen los flujos de datos de los clientes más allá del Gateway, por lo que no se es capaz de resolver múltiples consultas concurrentemente.
 - No se implementan mecanismos de sincronización que permitan escalar los controles Sum y Aggregator. En particular:
   - Las instancias de Sum se dividen el trabajo, pero solo una de ellas recibe la notificación de finalización en la ingesta de datos.
   - Las instancias de Sum realizan _broadcast_ a todas las instancias de Aggregator, en lugar de agrupar los datos por algún criterio y evitar procesamiento redundante.
  - No se maneja la señal SIGTERM, con la salvedad de los clientes y el Gateway.

La implementación de referencia en `python/` (documentada en la sección anterior) incorpora middleware RabbitMQ, flujos separados por cliente después del Gateway, sincronización entre réplicas de Sum y de Aggregation mediante EOF y canales de control, y manejo de SIGTERM en Sum, Aggregation y Join; el texto de esta lista sigue siendo útil como contraste con el esqueleto mínimo original.

## Condiciones de Entrega

El código de este repositorio se agrupa en dos carpetas, una para Python y otra para Golang. Los estudiantes deberán elegir **sólo uno** de estos lenguajes y realizar una implementación que funcione correctamente ante cambios en la multiplicidad de los controles (archivo de docker compose), los archivos de entrada y las implementaciones de las funciones de Suma y Comparación del `FruitItem`.

![ ](./imgs/mutabilidad.jpg  "Mutabilidad de Elementos")
*Fig. 2: Elementos mutables e inmutables*

A modo de referencia, en la *Figura 2* se marcan en tonos oscuros los elementos que los estudiantes no deben alterar y en tonos claros aquellos sobre los que tienen libertad de decisión.
Al momento de la evaluación y ejecución de las pruebas se **descartarán** o **reemplazarán** :

- Los archivos de entrada de la carpeta `datasets`.
- El archivo docker compose principal y los de la carpeta `scenarios`.
- Todos los archivos Dockerfile.
- Todo el código del cliente.
- Todo el código del gateway, salvo `message_handler`.
- La implementación del protocolo de comunicación externo y `FruitItem`.

Redactar un breve informe explicando el modo en que se coordinan las instancias de Sum y Aggregation, así como el modo en el que el sistema escala respecto a los clientes y a la cantidad de controles.
