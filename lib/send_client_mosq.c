/*
Copyright (c) 2009-2014 Roger Light <roger@atchoo.org>

All rights reserved. This program and the accompanying materials
are made available under the terms of the Eclipse Public License v1.0
and Eclipse Distribution License v1.0 which accompany this distribution.
 
The Eclipse Public License is available at
   http://www.eclipse.org/legal/epl-v10.html
and the Eclipse Distribution License is available at
  http://www.eclipse.org/org/documents/edl-v10.php.
 
Contributors:
   Roger Light - initial implementation and documentation.
*/

#include <assert.h>
#include <string.h>

#include <mosquitto.h>
#include <logging_mosq.h>
#include <memory_mosq.h>
#include <mqtt3_protocol.h>
#include <net_mosq.h>
#include <send_mosq.h>
#include <util_mosq.h>

#ifdef WITH_BROKER
#include <mosquitto_broker.h>
#endif

int _mosquitto_send_connect(struct mosquitto *mosq, uint16_t keepalive, bool clean_session)
{
	struct _mosquitto_packet *packet = NULL;
	int payloadlen;
	uint8_t will = 0;
	uint8_t byte;
	int rc;
	uint8_t version = PROTOCOL_VERSION_v31;
	char *clientid;

	assert(mosq);
	assert(mosq->id);

#if defined(WITH_BROKER) && defined(WITH_BRIDGE)
	if(mosq->bridge){
		clientid = mosq->bridge->clientid;
	}else{
		clientid = mosq->id;
	}
#else
	clientid = mosq->id;
#endif

	packet = _mosquitto_calloc(1, sizeof(struct _mosquitto_packet));
	if(!packet) return MOSQ_ERR_NOMEM;

	payloadlen = 2+strlen(clientid);
	if(mosq->will){
		will = 1;
		assert(mosq->will->topic);

		payloadlen += 2+strlen(mosq->will->topic) + 2+mosq->will->payloadlen;
	}
	if(mosq->username){
		payloadlen += 2+strlen(mosq->username);
		if(mosq->password){
			payloadlen += 2+strlen(mosq->password);
		}
	}

	packet->command = CONNECT;
	packet->remaining_length = 12+payloadlen;
	rc = _mosquitto_packet_alloc(packet);
	if(rc){
		_mosquitto_free(packet);
		return rc;
	}

	/* Variable header */
	_mosquitto_write_string(packet, PROTOCOL_NAME_v31, strlen(PROTOCOL_NAME_v31));
#if defined(WITH_BROKER) && defined(WITH_BRIDGE)
	if(mosq->bridge && mosq->bridge->try_private && mosq->bridge->try_private_accepted){
		version |= 0x80;
	}else{
	}
#endif
	_mosquitto_write_byte(packet, version);
	byte = (clean_session&0x1)<<1;
	if(will){
		byte = byte | ((mosq->will->retain&0x1)<<5) | ((mosq->will->qos&0x3)<<3) | ((will&0x1)<<2);
	}
	if(mosq->username){
		byte = byte | 0x1<<7;
		if(mosq->password){
			byte = byte | 0x1<<6;
		}
	}
	_mosquitto_write_byte(packet, byte);
	_mosquitto_write_uint16(packet, keepalive);

	/* Payload */
	_mosquitto_write_string(packet, clientid, strlen(clientid));
	if(will){
		_mosquitto_write_string(packet, mosq->will->topic, strlen(mosq->will->topic));
		_mosquitto_write_string(packet, (const char *)mosq->will->payload, mosq->will->payloadlen);
	}
	if(mosq->username){
		_mosquitto_write_string(packet, mosq->username, strlen(mosq->username));
		if(mosq->password){
			_mosquitto_write_string(packet, mosq->password, strlen(mosq->password));
		}
	}

	mosq->keepalive = keepalive;
#ifdef WITH_BROKER
# ifdef WITH_BRIDGE
	_mosquitto_log_printf(mosq, MOSQ_LOG_DEBUG, "Bridge %s sending CONNECT", clientid);
# endif
#else
	_mosquitto_log_printf(mosq, MOSQ_LOG_DEBUG, "Client %s sending CONNECT", clientid);
#endif
	return _mosquitto_packet_queue(mosq, packet);
}

int _mosquitto_send_disconnect(struct mosquitto *mosq)
{
	assert(mosq);
#ifdef WITH_BROKER
# ifdef WITH_BRIDGE
	_mosquitto_log_printf(mosq, MOSQ_LOG_DEBUG, "Bridge %s sending DISCONNECT", mosq->id);
# endif
#else
	_mosquitto_log_printf(mosq, MOSQ_LOG_DEBUG, "Client %s sending DISCONNECT", mosq->id);
#endif
	return _mosquitto_send_simple_command(mosq, DISCONNECT);
}

int _mosquitto_send_subscribe(struct mosquitto *mosq, int *mid, bool dup, const char *topic, uint8_t topic_qos)
{
	/* FIXME - only deals with a single topic */
	struct _mosquitto_packet *packet = NULL;
	uint32_t packetlen;
	uint16_t local_mid;
	int rc;

	assert(mosq);
	assert(topic);

	packet = _mosquitto_calloc(1, sizeof(struct _mosquitto_packet));
	if(!packet) return MOSQ_ERR_NOMEM;

	packetlen = 2 + 2+strlen(topic) + 1;

	packet->command = SUBSCRIBE | (dup<<3) | (1<<1);
	packet->remaining_length = packetlen;
	rc = _mosquitto_packet_alloc(packet);
	if(rc){
		_mosquitto_free(packet);
		return rc;
	}

	/* Variable header */
	local_mid = _mosquitto_mid_generate(mosq);
	if(mid) *mid = (int)local_mid;
	_mosquitto_write_uint16(packet, local_mid);

	/* Payload */
	_mosquitto_write_string(packet, topic, strlen(topic));
	_mosquitto_write_byte(packet, topic_qos);

#ifdef WITH_BROKER
# ifdef WITH_BRIDGE
	_mosquitto_log_printf(mosq, MOSQ_LOG_DEBUG, "Bridge %s sending SUBSCRIBE (Mid: %d, Topic: %s, QoS: %d)", mosq->id, local_mid, topic, topic_qos);
# endif
#else
	_mosquitto_log_printf(mosq, MOSQ_LOG_DEBUG, "Client %s sending SUBSCRIBE (Mid: %d, Topic: %s, QoS: %d)", mosq->id, local_mid, topic, topic_qos);
#endif

	return _mosquitto_packet_queue(mosq, packet);
}


int _mosquitto_send_unsubscribe(struct mosquitto *mosq, int *mid, bool dup, const char *topic)
{
	/* FIXME - only deals with a single topic */
	struct _mosquitto_packet *packet = NULL;
	uint32_t packetlen;
	uint16_t local_mid;
	int rc;

	assert(mosq);
	assert(topic);

	packet = _mosquitto_calloc(1, sizeof(struct _mosquitto_packet));
	if(!packet) return MOSQ_ERR_NOMEM;

	packetlen = 2 + 2+strlen(topic);

	packet->command = UNSUBSCRIBE | (dup<<3) | (1<<1);
	packet->remaining_length = packetlen;
	rc = _mosquitto_packet_alloc(packet);
	if(rc){
		_mosquitto_free(packet);
		return rc;
	}

	/* Variable header */
	local_mid = _mosquitto_mid_generate(mosq);
	if(mid) *mid = (int)local_mid;
	_mosquitto_write_uint16(packet, local_mid);

	/* Payload */
	_mosquitto_write_string(packet, topic, strlen(topic));

#ifdef WITH_BROKER
# ifdef WITH_BRIDGE
	_mosquitto_log_printf(mosq, MOSQ_LOG_DEBUG, "Bridge %s sending UNSUBSCRIBE (Mid: %d, Topic: %s)", mosq->id, local_mid, topic);
# endif
#else
	_mosquitto_log_printf(mosq, MOSQ_LOG_DEBUG, "Client %s sending UNSUBSCRIBE (Mid: %d, Topic: %s)", mosq->id, local_mid, topic);
#endif
	return _mosquitto_packet_queue(mosq, packet);
}

