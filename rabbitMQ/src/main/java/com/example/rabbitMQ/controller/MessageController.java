package com.example.rabbitMQ.controller;

import com.example.rabbitMQ.config.RabbitMQConfig;
import com.example.rabbitMQ.model.MessageObject;
import org.springframework.amqp.core.MessageBuilder;
import org.springframework.amqp.rabbit.core.RabbitTemplate;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.beans.factory.annotation.Value;
import org.springframework.web.bind.annotation.PostMapping;
import org.springframework.web.bind.annotation.RequestBody;
import org.springframework.web.bind.annotation.RequestMapping;
import org.springframework.web.bind.annotation.RestController;

/*
 *   Date : 12/28/2020
 *   Created by : viet.nguyen
 *
 */
@RestController
@RequestMapping("/api/v1")
public class MessageController {

	@Autowired
	private RabbitTemplate rabbitTemplate;

	@Value("${message.create.queue}")
	private String messageQueue;

	@PostMapping("/message")
	public void sendMessage(@RequestBody final MessageObject message) {
		org.springframework.amqp.core.Message headerMessage = MessageBuilder.withBody(
				("header error: " + message.getMessage()).getBytes())
				.setHeader("level", "error")
				.build();
		rabbitTemplate.convertAndSend(RabbitMQConfig.FANOUT_EXCHANGE_NAME, "", "fanout: " + message.getMessage());

		rabbitTemplate.convertAndSend(RabbitMQConfig.TOPIC_EXCHANGE_NAME, "msg.important.warn",
				"topic important warn: " + message.getMessage());

		rabbitTemplate.convertAndSend(RabbitMQConfig.TOPIC_EXCHANGE_NAME, "msg.error",
				"topic important error: " + message.getMessage());

		rabbitTemplate.convertAndSend(RabbitMQConfig.HEADERS_EXCHANGE_NAME, "", headerMessage);

		rabbitTemplate.convertAndSend(RabbitMQConfig.DEFAULT_EXCHANGE_NAME, RabbitMQConfig.HEADERS_QUEUE_NAME,
				("default: " + message.getMessage()).getBytes());

		rabbitTemplate.convertAndSend(RabbitMQConfig.DIRECT_EXCHANGE_NAME, "direct.exchange-1",
				"direct-1: " + message.getMessage());

		rabbitTemplate.convertAndSend(RabbitMQConfig.DIRECT_EXCHANGE_NAME, "direct.exchange-2",
				"direct-2: " + message.getMessage());
	}

	@PostMapping("/message/v2")
	public void sendMessageV2(@RequestBody final MessageObject message) {
		rabbitTemplate.convertAndSend(messageQueue, message);
	}
}