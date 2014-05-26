/*
Copyright (c) 2014 Roger Light <roger@atchoo.org>
All rights reserved.

Redistribution and use in source and binary forms, with or without
modification, are permitted provided that the following conditions are met:

1. Redistributions of source code must retain the above copyright notice,
   this list of conditions and the following disclaimer.
2. Redistributions in binary form must reproduce the above copyright
   notice, this list of conditions and the following disclaimer in the
   documentation and/or other materials provided with the distribution.
3. Neither the name of mosquitto nor the names of its
   contributors may be used to endorse or promote products derived from
   this software without specific prior written permission.

THIS SOFTWARE IS PROVIDED BY THE COPYRIGHT HOLDERS AND CONTRIBUTORS "AS IS"
AND ANY EXPRESS OR IMPLIED WARRANTIES, INCLUDING, BUT NOT LIMITED TO, THE
IMPLIED WARRANTIES OF MERCHANTABILITY AND FITNESS FOR A PARTICULAR PURPOSE
ARE DISCLAIMED. IN NO EVENT SHALL THE COPYRIGHT OWNER OR CONTRIBUTORS BE
LIABLE FOR ANY DIRECT, INDIRECT, INCIDENTAL, SPECIAL, EXEMPLARY, OR
CONSEQUENTIAL DAMAGES (INCLUDING, BUT NOT LIMITED TO, PROCUREMENT OF
SUBSTITUTE GOODS OR SERVICES; LOSS OF USE, DATA, OR PROFITS; OR BUSINESS
INTERRUPTION) HOWEVER CAUSED AND ON ANY THEORY OF LIABILITY, WHETHER IN
CONTRACT, STRICT LIABILITY, OR TORT (INCLUDING NEGLIGENCE OR OTHERWISE)
ARISING IN ANY WAY OUT OF THE USE OF THIS SOFTWARE, EVEN IF ADVISED OF THE
POSSIBILITY OF SUCH DAMAGE.
*/

#ifdef WITH_WEBSOCKETS

#include <libwebsockets.h>
#include "mosquitto_internal.h"
#include "mosquitto_broker.h"
#include "mqtt3_protocol.h"
#include "memory_mosq.h"

#ifdef WITH_SYS_TREE
extern uint64_t g_bytes_received;
extern uint64_t g_bytes_sent;
extern unsigned long g_msgs_received;
extern unsigned long g_msgs_sent;
extern unsigned long g_pub_msgs_received;
extern unsigned long g_pub_msgs_sent;
#endif
extern struct mosquitto_db int_db;

static int callback_mqtt(struct libwebsocket_context *context,
		struct libwebsocket *wsi,
		enum libwebsocket_callback_reasons reason,
		void *user,
		void *in,
		size_t len);
static int callback_http(struct libwebsocket_context *context,
	struct libwebsocket *wsi,
	enum libwebsocket_callback_reasons reason,
	void *user,
	void *in,
	size_t len);

enum mosq_ws_protocols {
	PROTOCOL_HTTP = 0,
	PROTOCOL_MQTT,
	DEMO_PROTOCOL_COUNT
};

struct libws_mqtt_data {
	struct mosquitto *mosq;
};

struct libws_http_data {
	char blank;
};

static struct libwebsocket_protocols protocols[] = {
	/* first protocol must always be HTTP handler */
	{
		"http-only",
		callback_http,
		sizeof (struct libws_http_data),
		0,
	},
	{
		"mqttv3.1",
		callback_mqtt,
		sizeof(struct libws_mqtt_data),
		0,
		1,
	},
	{ NULL, NULL, 0, 0 }
};


static int callback_mqtt(struct libwebsocket_context *context,
		struct libwebsocket *wsi,
		enum libwebsocket_callback_reasons reason,
		void *user,
		void *in,
		size_t len)
{
	struct mosquitto_db *db;
	struct mosquitto *mosq = NULL;
	struct _mosquitto_packet *packet;
	int count;
	struct libws_mqtt_data *u = (struct libws_mqtt_data *)user;
	size_t pos;
	uint8_t *buf;
	int rc;
	uint8_t byte;

	db = &int_db;

	switch (reason) {
		case LWS_CALLBACK_ESTABLISHED:
			mosq = mqtt3_context_init(-1);
			if(mosq){
				mosq->ws_context = context;
				mosq->wsi = wsi;
				u->mosq = mosq;
			}
			break;

		case LWS_CALLBACK_CLOSED:
			mosq = u->mosq;
			mqtt3_context_cleanup(db, mosq, true);
			break;

		case LWS_CALLBACK_SERVER_WRITEABLE:
			mosq = u->mosq;
			if(mosq->out_packet && !mosq->current_out_packet){
				mosq->current_out_packet = mosq->out_packet;
				mosq->out_packet = mosq->out_packet->next;
				if(!mosq->out_packet){
					mosq->out_packet_last = NULL;
				}
			}

			while(mosq->current_out_packet){
				packet = mosq->current_out_packet;

				if(packet->pos == 0 && packet->to_process == packet->packet_length){
					/* First time this packet has been dealt with.
					 * libwebsockets requires that the payload has
					 * LWS_SEND_BUFFER_PRE_PADDING space available before the
					 * actual data and LWS_SEND_BUFFER_POST_PADDING afterwards.
					 * We've already made the payload big enough to allow this,
					 * but need to move it into position here. */
					memmove(&packet->payload[LWS_SEND_BUFFER_PRE_PADDING], packet->payload, packet->packet_length);
					packet->pos += LWS_SEND_BUFFER_PRE_PADDING;
				}
				count = libwebsocket_write(wsi, &packet->payload[packet->pos], packet->to_process, LWS_WRITE_BINARY);
				if(count < 0){
					return 0;
				}
				packet->to_process -= count;
				packet->pos += count;
				if(packet->to_process > 0){
					break;
				}

				/* Free data and reset values */
				mosq->current_out_packet = mosq->out_packet;
				if(mosq->out_packet){
					mosq->out_packet = mosq->out_packet->next;
					if(!mosq->out_packet){
						mosq->out_packet_last = NULL;
					}
				}

				_mosquitto_packet_cleanup(packet);
				_mosquitto_free(packet);

				mosq->last_msg_out = mosquitto_time();
			}
			break;

		case LWS_CALLBACK_RECEIVE:
			mosq = u->mosq;
			pos = 0;
			buf = (uint8_t *)in;
#ifdef WITH_SYS_TREE
			g_bytes_received += len;
#endif
			while(pos < len){
				if(!mosq->in_packet.command){
					mosq->in_packet.command = buf[pos];
					pos++;
					/* Clients must send CONNECT as their first command. */
					if(mosq->state == mosq_cs_new && (mosq->in_packet.command&0xF0) != CONNECT){
						return -1;
					}
				}
				if(!mosq->in_packet.have_remaining){
					do{
						if(pos == len){
							return 0;
						}
						byte = buf[pos];
						pos++;

						mosq->in_packet.remaining_count++;
						/* Max 4 bytes length for remaining length as defined by protocol.
						* Anything more likely means a broken/malicious client.
						*/
						if(mosq->in_packet.remaining_count > 4){
							return -1;
						}

						mosq->in_packet.remaining_length += (byte & 127) * mosq->in_packet.remaining_mult;
						mosq->in_packet.remaining_mult *= 128;
					}while((byte & 128) != 0);

					if(mosq->in_packet.remaining_length > 0){
						mosq->in_packet.payload = _mosquitto_malloc(mosq->in_packet.remaining_length*sizeof(uint8_t));
						if(!mosq->in_packet.payload) return -1;
						mosq->in_packet.to_process = mosq->in_packet.remaining_length;
					}
					mosq->in_packet.have_remaining = 1;
				}
				while(mosq->in_packet.to_process>0){
					if(len - pos >= mosq->in_packet.to_process){
						memcpy(&mosq->in_packet.payload[mosq->in_packet.pos], &buf[pos], mosq->in_packet.to_process);
						mosq->in_packet.pos += mosq->in_packet.to_process;
						pos += mosq->in_packet.to_process;
						mosq->in_packet.to_process = 0;
					}else{
						memcpy(&mosq->in_packet.payload[mosq->in_packet.pos], &buf[pos], len-pos);
						mosq->in_packet.pos += len-pos;
						mosq->in_packet.to_process -= len-pos;
						break;
					}
				}

				/* All data for this packet is read. */
				mosq->in_packet.pos = 0;
#ifdef WITH_SYS_TREE
				g_msgs_received++;
				if(((mosq->in_packet.command)&0xF5) == PUBLISH){
					g_pub_msgs_received++;
				}
#endif
				rc = mqtt3_packet_handle(db, mosq);

				/* Free data and reset values */
				_mosquitto_packet_cleanup(&mosq->in_packet);

				mosq->last_msg_in = mosquitto_time();

				if(rc){
					if(db->config->connection_messages == true){
						if(mosq->state != mosq_cs_disconnecting){
							_mosquitto_log_printf(NULL, MOSQ_LOG_NOTICE, "Socket error on client %s, disconnecting.", mosq->id);
						}else{
							_mosquitto_log_printf(NULL, MOSQ_LOG_NOTICE, "Client %s disconnected.", mosq->id);
						}
					}
					mqtt3_context_disconnect(db, mosq);
					return -1;
				}
			}
			break;

		default:
			break;
	}

	return 0;
}


static int callback_http(struct libwebsocket_context *context,
		struct libwebsocket *wsi,
		enum libwebsocket_callback_reasons reason,
		void *user,
		void *in,
		size_t len)
{
	return 0;
}

struct libwebsocket_context *mosq_websockets_init(int port)
{
	struct lws_context_creation_info info;

	memset(&info, 0, sizeof(info));
	info.port = port;
	info.protocols = protocols;
	info.gid = -1;
	info.uid = -1;

	lws_set_log_level(0, NULL);

	return libwebsocket_create_context(&info);
}


#endif
