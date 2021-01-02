package com.example.rabbitMQ.config;

import com.example.rabbitMQ.model.MessageObject;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.amqp.core.AcknowledgeMode;
import org.springframework.amqp.core.BindingBuilder;
import org.springframework.amqp.core.Queue;
import org.springframework.amqp.core.TopicExchange;
import org.springframework.amqp.rabbit.annotation.EnableRabbit;
import org.springframework.amqp.rabbit.annotation.RabbitHandler;
import org.springframework.amqp.rabbit.annotation.RabbitListener;
import org.springframework.amqp.rabbit.config.SimpleRabbitListenerContainerFactory;
import org.springframework.amqp.rabbit.connection.ConnectionFactory;
import org.springframework.amqp.rabbit.core.RabbitAdmin;
import org.springframework.amqp.support.converter.Jackson2JsonMessageConverter;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.beans.factory.annotation.Value;
import org.springframework.context.annotation.Bean;
import org.springframework.context.annotation.Configuration;
import org.springframework.stereotype.Component;

import javax.annotation.PostConstruct;

/*
 *   Date : 12/28/2020
 *   Created by : viet.nguyen
 *
 */
@Component
@RabbitListener(

		queues = {

				"${message.create.queue}",

				"${message.update.queue}",

		},

		containerFactory = "messageRabbitFactory"

)
@Configuration
@EnableRabbit
public class RabbitMQConfigV2 {

	private static final Logger logger = LoggerFactory.getLogger(RabbitMQConfigV2.class);

	private static final int MAX_CONCURRENT_CONSUMERS = 20;

	@Autowired
	private ConnectionFactory connection;

	@Autowired
	private RabbitAdmin rabbitAdmin;

	@Value("${message.create.queue}")
	private String messageCreateQueueName;

	@Value("${message.update.queue}")
	private String messageUpdateQueueName;

	private void declareAndBinding(String name) {

		Queue queue = new Queue(name, true);
		rabbitAdmin.declareQueue(queue);

		TopicExchange exchange = new TopicExchange(name);
		rabbitAdmin.declareExchange(exchange);

		rabbitAdmin.declareBinding(BindingBuilder.bind(queue).to(exchange).with(name));
	}

	@PostConstruct
	public void init() {
		try {

			declareAndBinding(messageCreateQueueName);

			declareAndBinding(messageUpdateQueueName);

		} catch (Exception e) {
			logger.error("Unable to init queue. Caused by {}", e.getMessage());
		}
	}

	@Bean(name = "messageRabbitFactory")
	public SimpleRabbitListenerContainerFactory rabbitListenerContainerFactory() {

		SimpleRabbitListenerContainerFactory container = new SimpleRabbitListenerContainerFactory();
		container.setConnectionFactory(connection);

		container.setMessageConverter(new Jackson2JsonMessageConverter());
		container.setAcknowledgeMode(AcknowledgeMode.AUTO);
		container.setMaxConcurrentConsumers(MAX_CONCURRENT_CONSUMERS);
		container.setConcurrentConsumers(MAX_CONCURRENT_CONSUMERS / 2);

		return container;
	}

	@RabbitHandler
	public void handleCompletedTask(MessageObject obj) {

		logger.info("Handle MessageObject. Message : {}", obj.getMessage());

//		try {
//			taskConverter.transform(taskNotif);
//		} catch (Exception ex) {
//			ex.printStackTrace();
//			logger.error("handleCompletedTask", ex);
//		}
	}
}
