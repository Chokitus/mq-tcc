package br.edu.ufabc.chokitus.impl

import br.edu.ufabc.chokitus.benchmark.impl.configuration.ReceiverConfiguration
import br.edu.ufabc.chokitus.mq.client.AbstractProducer
import br.edu.ufabc.chokitus.mq.client.AbstractReceiver
import br.edu.ufabc.chokitus.mq.factory.AbstractClientFactory
import br.edu.ufabc.chokitus.mq.message.AbstractMessage
import br.edu.ufabc.chokitus.mq.properties.ClientProperties
import com.rabbitmq.client.Channel
import com.rabbitmq.client.Connection
import com.rabbitmq.client.ConnectionFactory
import com.rabbitmq.client.GetResponse
import java.util.concurrent.ConcurrentHashMap
import java.util.concurrent.ConcurrentLinkedDeque

object RabbitMQ {

	data class RabbitMQProperties(
		val username: String,
		val password: String,
		val virtualHost: String,
		val host: String,
		val port: Int,
		val autoAck: Boolean?,
		val batchReceive: Boolean
	) : ClientProperties()

	class RabbitMQMessage(
		private val getResponse: GetResponse,
		private val ack: (GetResponse) -> Unit = {},
		private val nack: (GetResponse) -> Unit = {},
	) : AbstractMessage() {

		override fun body(): ByteArray = getResponse.body

		override fun ack() = ack(getResponse)
		override fun nack() = nack(getResponse)
	}

	class RabbitMQReceiver(
		properties: RabbitMQProperties,
		connection: Connection
	) : AbstractReceiver<Channel, RabbitMQMessage, RabbitMQProperties>(properties) {

		private val receiver = connection.createChannel()

		private val queueMap = ConcurrentHashMap<String, ConcurrentLinkedDeque<RabbitMQMessage>>()

		private val ackFunction: (GetResponse) -> Unit =
			{ receiver.basicAck(it.envelope.deliveryTag, false) }

		private val nackFunction: (GetResponse) -> Unit =
			{ receiver.basicNack(it.envelope.deliveryTag, false, true) }

		override fun close() {
			receiver.close()
		}

		override fun receiveBatch(
			destination: String,
			properties: ReceiverConfiguration
		): List<RabbitMQMessage> {

			return listOf()
		}

		override fun getReceiver(destination: String, properties: RabbitMQProperties?): Channel =
			receiver

		override fun ackAll(messages: List<RabbitMQMessage>) {
			TODO("Not yet implemented")
		}

		override fun receive(destination: String, properties: ReceiverConfiguration): RabbitMQMessage? =
			receiver
				.basicGet(destination, this.properties.autoAck ?: false) // Nullable
				?.let { RabbitMQMessage(it, ackFunction, nackFunction) }
	}

	class RabbitMQProducer(
		properties: RabbitMQProperties,
		private val connection: Connection
	) : AbstractProducer<Channel, RabbitMQMessage, RabbitMQProperties>(properties) {

		private val producer = connection.createChannel()

		override fun produce(destination: String, body: ByteArray, properties: RabbitMQProperties?) {
			producer.basicPublish("", destination, null, body)
		}

		override fun getProducer(destination: String, properties: RabbitMQProperties?): Channel =
			producer

		override fun close() {
			producer.close()
		}
	}

	class RabbitMQClientFactory(
		properties: RabbitMQProperties
	) : AbstractClientFactory<RabbitMQReceiver, RabbitMQProducer, RabbitMQProperties>(properties) {

		private val connectionFactory =
			ConnectionFactory().apply {
				username = properties.username
				password = properties.password
				virtualHost = properties.virtualHost
				host = properties.host
				port = properties.port
			}

		override fun createReceiverImpl(): RabbitMQReceiver =
			RabbitMQReceiver(properties, connectionFactory.newConnection())

		override fun createProducerImpl(): RabbitMQProducer =
			RabbitMQProducer(properties, connectionFactory.newConnection())

	}
}
