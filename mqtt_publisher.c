/*
 * Court 02 - Remote Node
 * Based on the professor's mqtt_publisher.c example.
 *
 * Project modifications:
 *   1. booking_status variable
 *   2. handle_booking_command() function
 *   3. Booking management in pub_handler()
 *   4. Added "booking" and "ts" fields in JSON payload
 */

#define DEBUG 1 //DEBUG_NONE 0

#include "contiki-conf.h"
#include "rpl/rpl-private.h"
#include "mqtt.h"
#include "net/rpl/rpl.h"
#include "net/ip/uip.h"
#include "net/ipv6/uip-icmp6.h"
#include "net/ipv6/sicslowpan.h"
#include "sys/etimer.h"
#include "sys/ctimer.h"
#include "lib/sensors.h"
#include "dev/leds.h"
#include "dev/button-sensor.h"

#if CONTIKI_TARGET_ZOUL
#include "dev/adc-zoul.h"
#include "dev/zoul-sensors.h"
#include "dev/dht22.h"
#include "dev/tsl256x.h"
#define PIR_ADC_CHANNEL   ZOUL_SENSORS_ADC1
#else
#include "dev/adxl345.h"
#include "dev/i2cmaster.h"
#include "dev/tmp102.h"
#endif

#include <string.h>

/*---------------------------------------------------------------------------*/
static const char *broker_ip = MQTT_DEMO_BROKER_IP_ADDR;
/*---------------------------------------------------------------------------*/
#define STATE_MACHINE_PERIODIC     (CLOCK_SECOND >> 1)
#define CONNECTING_LED_DURATION    (CLOCK_SECOND >> 2)
#define PUBLISH_LED_ON_DURATION    (CLOCK_SECOND)
#define RETRY_FOREVER              0xFF
#define RECONNECT_INTERVAL         (CLOCK_SECOND * 2)
#define RECONNECT_ATTEMPTS         RETRY_FOREVER
#define CONNECTION_STABLE_TIME     (CLOCK_SECOND * 5)
#define NET_CONNECT_PERIODIC       (CLOCK_SECOND >> 2)
#define NO_NET_LED_DURATION        (NET_CONNECT_PERIODIC >> 1)
/*---------------------------------------------------------------------------*/
static struct timer connection_life;
static uint8_t connect_attempt;
static uint8_t state;

#define STATE_INIT                    0
#define STATE_REGISTERED              1
#define STATE_CONNECTING              2
#define STATE_CONNECTED               3
#define STATE_PUBLISHING              4
#define STATE_DISCONNECTED            5
#define STATE_NEWCONFIG               6
#define STATE_CONFIG_ERROR         0xFE
#define STATE_ERROR                0xFF
/*---------------------------------------------------------------------------*/
#define CONFIG_EVENT_TYPE_ID_LEN     24
#define CONFIG_CMD_TYPE_LEN          24
#define CONFIG_IP_ADDR_STR_LEN       32
/*---------------------------------------------------------------------------*/
PROCESS_NAME(mqtt_publisher_process);
AUTOSTART_PROCESSES(&mqtt_publisher_process);
/*---------------------------------------------------------------------------*/
typedef struct mqtt_client_config {
  char event_type_id[CONFIG_EVENT_TYPE_ID_LEN];
  char broker_ip[CONFIG_IP_ADDR_STR_LEN];
  char cmd_type[CONFIG_CMD_TYPE_LEN];
  clock_time_t pub_interval;
  uint16_t broker_port;
} mqtt_client_config_t;
/*---------------------------------------------------------------------------*/
static char client_id[BUFFER_SIZE];
static char pub_topic[BUFFER_SIZE];
static char sub_topic[BUFFER_SIZE];
static struct mqtt_connection conn;
static char app_buffer[APP_BUFFER_SIZE];
static struct mqtt_message *msg_ptr = 0;
static struct etimer publish_periodic_timer;
static struct ctimer ct;
static char *buf_ptr;
static uint16_t seq_nr_value = 0;
static mqtt_client_config_t conf;

/* Booking status variable
 * 0 = available
 * 1 = booked
 */
static uint8_t booking_status = 0;
/* ---------------------------------------------------- */

/*---------------------------------------------------------------------------*/
PROCESS(mqtt_publisher_process, "DEEC MQTT Publisher Demo");
/*---------------------------------------------------------------------------*/
static void
publish_led_off(void *d)
{
  leds_off(LEDS_GREEN);
}
/*---------------------------------------------------------------------------*/

/* Handles incoming booking commands from MQTT downlink.
 * Updates LED state and booking status if the message
 * is intended for this specific court.
 */
static void
handle_booking_command(const uint8_t *chunk, uint16_t chunk_len)
{
  char buf[APP_BUFFER_SIZE];

  uint16_t len = (chunk_len < APP_BUFFER_SIZE - 1) ?
                 chunk_len : APP_BUFFER_SIZE - 1;

  memcpy(buf, chunk, len);
  buf[len] = '\0';

  printf("Court Node - Downlink received: %s\n", buf);

  /* Ignore messages for other courts */
  if(strstr(buf, "Court_02") == NULL) {
    printf("Court Node - Command not for this node. Ignored.\n");
    return;
  }

  if(strstr(buf, "\"status\":\"ON\"") != NULL) {

    booking_status = 1;
    leds_off(LEDS_GREEN);
    leds_on(LEDS_RED);

    printf("Court Node - Booking ON: RED LED activated.\n");

  } else if(strstr(buf, "\"status\":\"OFF\"") != NULL) {

    booking_status = 0;
    leds_off(LEDS_RED);
    leds_on(LEDS_GREEN);

    printf("Court Node - Booking OFF: GREEN LED activated.\n");
  }
}
/* ----------------------------------------------------------- */

/*---------------------------------------------------------------------------*/
/* Handles incoming MQTT messages.
 * Routes booking commands and LED control messages.
 */
static void
pub_handler(const char *topic, uint16_t topic_len,
            const uint8_t *chunk, uint16_t chunk_len)
{
  printf("Pub Handler: topic='%s' (len=%u), payload=%u\n",
         topic, topic_len, chunk_len);

  /* Booking command handling */
  if(strncmp(topic, DEFAULT_LEDS_CMD_TOPIC, topic_len) == 0) {
    handle_booking_command(chunk, chunk_len);
    return;
  }

  if(topic_len != 13 || chunk_len != 1) {
    printf("Invalid message format. Ignored.\n");
    return;
  }

  if(strncmp(&topic[9], "leds", 4) == 0) {

    if(chunk[0] == '1') {
      leds_on(LEDS_RED);
      printf("RED LED ON\n");
    }
    else if(chunk[0] == '0') {
      leds_off(LEDS_RED);
      printf("RED LED OFF\n");
    }

    return;
  }
}
/*---------------------------------------------------------------------------*/
/* MQTT event handler */
static void
mqtt_event(struct mqtt_connection *m, mqtt_event_t event, void *data)
{
  switch(event) {

  case MQTT_EVENT_CONNECTED:
    printf("APP - MQTT connection established\n");
    timer_set(&connection_life, CONNECTION_STABLE_TIME);
    state = STATE_CONNECTED;
    break;

  case MQTT_EVENT_DISCONNECTED:
    printf("APP - MQTT disconnected. Reason %u\n",
           *((mqtt_event_t *)data));
    state = STATE_DISCONNECTED;
    process_poll(&mqtt_publisher_process);
    break;

  case MQTT_EVENT_PUBLISH:
    msg_ptr = data;

    if(msg_ptr->first_chunk) {
      msg_ptr->first_chunk = 0;

      printf("APP - Message received on topic '%s' (size %i bytes)\n",
             msg_ptr->topic,
             msg_ptr->payload_length);
    }

    pub_handler(msg_ptr->topic,
                strlen(msg_ptr->topic),
                msg_ptr->payload_chunk,
                msg_ptr->payload_length);
    break;

  case MQTT_EVENT_SUBACK:
    printf("APP - Successfully subscribed\n");
    break;

  case MQTT_EVENT_UNSUBACK:
    printf("APP - Successfully unsubscribed\n");
    break;

  case MQTT_EVENT_PUBACK:
    printf("APP - Publish completed\n");
    break;

  default:
    printf("APP - Unhandled MQTT event: %i\n", event);
    break;
  }
}
/*---------------------------------------------------------------------------*/
static int
construct_pub_topic(void)
{
  int len = snprintf(pub_topic, BUFFER_SIZE, "%s", conf.event_type_id);
  if(len < 0 || len >= BUFFER_SIZE) {
    printf("Pub Topic too large: %d, Buffer %d\n", len, BUFFER_SIZE);
    return 0;
  }
  return 1;
}
/*---------------------------------------------------------------------------*/
static int
construct_sub_topic(void)
{
  int len = snprintf(sub_topic, BUFFER_SIZE, "%s", conf.cmd_type);
  if(len < 0 || len >= BUFFER_SIZE) {
    printf("Sub Topic too large: %d, Buffer %d\n", len, BUFFER_SIZE);
    return 0;
  }
  printf("Subscription topic %s\n", sub_topic);
  return 1;
}
/*---------------------------------------------------------------------------*/
static int
construct_client_id(void)
{
  int len = snprintf(client_id, BUFFER_SIZE, "d:%02x%02x%02x%02x%02x%02x",
                     linkaddr_node_addr.u8[0], linkaddr_node_addr.u8[1],
                     linkaddr_node_addr.u8[2], linkaddr_node_addr.u8[5],
                     linkaddr_node_addr.u8[6], linkaddr_node_addr.u8[7]);
  if(len < 0 || len >= BUFFER_SIZE) {
    printf("Client ID: %d, Buffer %d\n", len, BUFFER_SIZE);
    return 0;
  }
  return 1;
}
/*---------------------------------------------------------------------------*/
static void
update_config(void)
{
  if(construct_client_id() == 0) { state = STATE_CONFIG_ERROR; return; }
  if(construct_sub_topic() == 0) { state = STATE_CONFIG_ERROR; return; }
  if(construct_pub_topic() == 0) { state = STATE_CONFIG_ERROR; return; }
  seq_nr_value = 0;
  state = STATE_INIT;
  etimer_set(&publish_periodic_timer, 0);
  return;
}
/*---------------------------------------------------------------------------*/
static int
init_config()
{
  memset(&conf, 0, sizeof(mqtt_client_config_t));
  memcpy(conf.event_type_id, DEFAULT_STATUS_EVENT_TOPIC, strlen(DEFAULT_STATUS_EVENT_TOPIC));
  memcpy(conf.broker_ip, broker_ip, strlen(broker_ip));
  memcpy(conf.cmd_type, DEFAULT_LEDS_CMD_TOPIC, strlen(DEFAULT_LEDS_CMD_TOPIC));
  conf.broker_port = DEFAULT_BROKER_PORT;
  conf.pub_interval = DEFAULT_PUBLISH_INTERVAL;
  return 1;
}
/*---------------------------------------------------------------------------*/
static void
subscribe(void)
{
  mqtt_status_t status;
  status = mqtt_subscribe(&conn, NULL, sub_topic, MQTT_QOS_LEVEL_0);
  printf("APP - Subscribing to %s\n", sub_topic);
  if(status == MQTT_STATUS_OUT_QUEUE_FULL) {
    printf("APP - Tried to subscribe but command queue was full!\n");
  }
}
/*---------------------------------------------------------------------------*/
static void
publish(void)
{
  static int len;
  int remaining = APP_BUFFER_SIZE;

  seq_nr_value++;
  buf_ptr = app_buffer;

  static uint16_t temperature;
  static uint16_t humidity;
  static uint16_t lux;
  static uint8_t occupancy;

  temperature = 0;
  humidity = 0;
  lux = 0;
  occupancy = 0;

  // 1. Lettura DHT22
  if(dht22_read_all(&temperature, &humidity) == DHT22_ERROR) {
    printf("Padel Node - Errore di lettura dal sensore DHT22\n");
  }

  // 2. Lettura TSL2561
  lux = tsl256x.value(TSL256X_VAL_READ);

  // 3. Lettura PIR
  if(adc_zoul.value(PIR_ADC_CHANNEL) > 2000) {
    occupancy = 1;
  } else {
    occupancy = 0;
  }

  // 4. Costruzione payload JSON
  // ---- MODIFICA 4: aggiunto "booking" e "ts" al payload ----
  len = snprintf(buf_ptr, remaining,
                 "{"
                 "\"node_id\":\"Court_02\","
                 "\"ts\":\"%lu\","
                 "\"temp\":%d.%d,"
                 "\"hum\":%d.%d,"
                 "\"lux\":%d,"
                 "\"occupancy\":%d,"
                 "\"booking\":%d"
                 "}",
                 clock_seconds(),
                 temperature / 10, temperature % 10,
                 humidity / 10, humidity % 10,
                 lux, occupancy, booking_status);
  /* -------------------------------------------------------- */

  if(len < 0 || len >= remaining) {
    printf("Buffer too short. Have %d, need %d + \\0\n", remaining, len);
    return;
  }

  mqtt_publish(&conn, NULL, pub_topic, (uint8_t *)app_buffer,
               strlen(app_buffer), MQTT_QOS_LEVEL_0, MQTT_RETAIN_OFF);

  printf("Padel Node - Messaggio inviato: %s\n", app_buffer);
  printf("APP - Publish to %s: %s\n", pub_topic, app_buffer);
}
/*---------------------------------------------------------------------------*/
static void
connect_to_broker(void)
{
  mqtt_connect(&conn, conf.broker_ip, conf.broker_port, conf.pub_interval * 3);
  state = STATE_CONNECTING;
}
/*---------------------------------------------------------------------------*/
static void
state_machine(void)
{
  switch(state) {
  case STATE_INIT:
    mqtt_register(&conn, &mqtt_publisher_process, client_id, mqtt_event,
                  MAX_TCP_SEGMENT_SIZE);
    conn.auto_reconnect = 0;
    connect_attempt = 1;
    state = STATE_REGISTERED;
    printf("Init\n");
  case STATE_REGISTERED:
    if(uip_ds6_get_global(ADDR_PREFERRED) != NULL) {
      printf("Registered. Connect attempt %u\n", connect_attempt);
      connect_to_broker();
    } else {
      leds_on(LEDS_GREEN);
      ctimer_set(&ct, NO_NET_LED_DURATION, publish_led_off, NULL);
    }
    etimer_set(&publish_periodic_timer, NET_CONNECT_PERIODIC);
    return;
    break;
  case STATE_CONNECTING:
    leds_on(LEDS_GREEN);
    ctimer_set(&ct, CONNECTING_LED_DURATION, publish_led_off, NULL);
    printf("Connecting (%u)\n", connect_attempt);
    break;
  case STATE_CONNECTED:
  case STATE_PUBLISHING:
    if(timer_expired(&connection_life)) {
      connect_attempt = 0;
    }
    if(mqtt_ready(&conn) && conn.out_buffer_sent) {
      if(state == STATE_CONNECTED) {
        subscribe();
        state = STATE_PUBLISHING;
      } else {
        leds_on(LEDS_GREEN);
        printf("Publishing\n");
        ctimer_set(&ct, PUBLISH_LED_ON_DURATION, publish_led_off, NULL);
        publish();
      }
      etimer_set(&publish_periodic_timer, conf.pub_interval);
      return;
    } else {
      printf("Publishing... (MQTT state=%d, q=%u)\n", conn.state,
          conn.out_queue_full);
    }
    break;
  case STATE_DISCONNECTED:
    printf("Disconnected\n");
    if(connect_attempt < RECONNECT_ATTEMPTS ||
       RECONNECT_ATTEMPTS == RETRY_FOREVER) {
      clock_time_t interval;
      mqtt_disconnect(&conn);
      connect_attempt++;
      interval = connect_attempt < 3 ? RECONNECT_INTERVAL << connect_attempt :
        RECONNECT_INTERVAL << 3;
      printf("Disconnected. Attempt %u in %lu ticks\n", connect_attempt, interval);
      etimer_set(&publish_periodic_timer, interval);
      state = STATE_REGISTERED;
      return;
    } else {
      state = STATE_ERROR;
      printf("Aborting connection after %u attempts\n", connect_attempt - 1);
    }
    break;
  case STATE_CONFIG_ERROR:
    printf("Bad configuration.\n");
    return;
  case STATE_ERROR:
  default:
    leds_on(LEDS_GREEN);
    printf("Default case: State=0x%02x\n", state);
    return;
  }
  etimer_set(&publish_periodic_timer, STATE_MACHINE_PERIODIC);
}
/*---------------------------------------------------------------------------*/
PROCESS_THREAD(mqtt_publisher_process, ev, data)
{
  PROCESS_BEGIN();

  printf("DEEC MQTT Publisher Demo Process\n");

  if(init_config() != 1) {
    PROCESS_EXIT();
  }

#if CONTIKI_TARGET_ZOUL
  adc_zoul.configure(SENSORS_HW_INIT, ZOUL_SENSORS_ADC_ALL);
  SENSORS_ACTIVATE(dht22);
  SENSORS_ACTIVATE(tsl256x);
#else
  SENSORS_ACTIVATE(adxl345);
  SENSORS_ACTIVATE(tmp102);
#endif

  update_config();

  while(1) {
    PROCESS_YIELD();

    if((ev == PROCESS_EVENT_TIMER && data == &publish_periodic_timer) ||
       ev == PROCESS_EVENT_POLL) {
      state_machine();
    }
  }

  PROCESS_END();
}
/*---------------------------------------------------------------------------*/