package br.edu.ufabc.chokitus.impl

import br.edu.ufabc.chokitus.benchmark.ClientFactory
import br.edu.ufabc.chokitus.benchmark.impl.configuration.DestinationConfiguration
import br.edu.ufabc.chokitus.benchmark.impl.configuration.ProducerConfiguration
import br.edu.ufabc.chokitus.benchmark.impl.configuration.ReceiverConfiguration
import br.edu.ufabc.chokitus.mq.BenchmarkDefiner
import br.edu.ufabc.chokitus.mq.client.AbstractProducer
import br.edu.ufabc.chokitus.mq.client.AbstractReceiver
import br.edu.ufabc.chokitus.mq.factory.AbstractClientFactory
import br.edu.ufabc.chokitus.mq.message.AbstractMessage
import br.edu.ufabc.chokitus.mq.message.MessageBatch
import br.edu.ufabc.chokitus.mq.properties.ClientProperties
import br.edu.ufabc.chokitus.util.ArgumentParser
import br.edu.ufabc.chokitus.util.Extensions.runDelayError
import com.rabbitmq.client.AMQP
import com.rabbitmq.client.BuiltinExchangeType
import com.rabbitmq.client.Channel
import com.rabbitmq.client.Connection
import com.rabbitmq.client.ConnectionFactory
import com.rabbitmq.client.DefaultConsumer
import com.rabbitmq.client.Envelope
import com.rabbitmq.client.MessageProperties
import java.util.concurrent.ConcurrentLinkedDeque
import kotlin.reflect.KClass

object RabbitMQ : BenchmarkDefiner {

	data class RabbitMQProperties(
		val username: String,
		val password: String,
		val virtualHost: String,
		val host: String,
		val port: Int,
		val autoAck: Boolean?,
	) : ClientProperties()

	class RabbitMQMessage(
		private val messageBody: ByteArray,
		val envelope: Envelope,
		private val ack: (Envelope) -> Unit = {},
		private val nack: (Envelope) -> Unit = {},
	) : AbstractMessage() {

		override fun body(): ByteArray = messageBody

		override fun ack() = ack(envelope)
		override fun nack() = nack(envelope)
	}

	class RabbitMQReceiver(
		properties: RabbitMQProperties,
		private val arguments: ArgumentParser.ParseResult,
		private val connection: Connection
	) : AbstractReceiver<Channel, RabbitMQMessage, RabbitMQProperties>(properties) {

		private val internalQueue = ConcurrentLinkedDeque<RabbitMQMessage>()

		var initialized: Boolean = false

		private val receiver = connection.createChannel().apply {
			basicQos(arguments.batchSize, false)
		}

		private val ackFunction: (Envelope) -> Unit =
			{ receiver.basicAck(it.deliveryTag, false) }

		private val multiAck: (Envelope) -> Unit =
			{ receiver.basicAck(it.deliveryTag, true) }

		private val nackFunction: (Envelope) -> Unit =
			{ receiver.basicNack(it.deliveryTag, false, true) }

		override fun close() {
			receiver.close()
			connection.close()
		}

		override fun receiveBatch(
			destination: String,
			properties: ReceiverConfiguration
		): MessageBatch<RabbitMQMessage> {
			if (!initialized) {
				initialize(destination)
			}

			val messages = mutableListOf<RabbitMQMessage>()

			while (messages.size < arguments.batchSize) {
				val msg = internalQueue.poll() ?: break
				messages.add(msg)
			}

			return MessageBatch(messages) { msgs ->
				multiAck(msgs.maxByOrNull { it.envelope.deliveryTag }!!.envelope)
			}
		}

		private fun initialize(destination: String) {
			receiver.basicConsume(
				destination,
				object : DefaultConsumer(receiver) {
					override fun handleDelivery(
						consumerTag: String,
						envelope: Envelope,
						properties: AMQP.BasicProperties,
						body: ByteArray
					) {
						internalQueue.add(
							RabbitMQMessage(
								messageBody = body,
								envelope = envelope,
								ack = multiAck,
								nack = nackFunction
							)
						)
					}
				}
			)
			initialized = true
		}

		override fun getReceiver(destination: String, properties: RabbitMQProperties?): Channel =
			receiver

		override fun receive(destination: String, properties: ReceiverConfiguration): RabbitMQMessage? =
			receiver
				.basicGet(destination, this.properties.autoAck ?: false) // Nullable
				?.let { RabbitMQMessage(it.body, it.envelope, ackFunction, nackFunction) }
	}

	class RabbitMQProducer(
		properties: RabbitMQProperties,
		private val connection: Connection
	) : AbstractProducer<Channel, RabbitMQMessage, RabbitMQProperties>(properties) {

		private val producer =
			connection.createChannel()
				// We must enable confirms as to not "cheat" on producer speed, otherwise all publishes
				// happen asynchronously
				.also { it.confirmSelect() }

		override fun produce(destination: String, body: ByteArray, properties: RabbitMQProperties?) {
			producer.basicPublish("", destination, MessageProperties.PERSISTENT_TEXT_PLAIN, body)
			producer.waitForConfirms()
		}

		override fun getProducer(destination: String, properties: RabbitMQProperties?): Channel =
			producer

		override fun close() {
			producer.close()
			connection.close()
		}

		override fun produceBatch(
			destination: String,
			bodies: Iterable<ByteArray>,
			properties: RabbitMQProperties?
		) {
			bodies.forEach {
				producer.basicPublish("", destination, MessageProperties.PERSISTENT_TEXT_PLAIN, it)
			}
			producer.waitForConfirms()
		}
	}

	class RabbitMQClientFactory(
		properties: RabbitMQProperties,
		arguments: ArgumentParser.ParseResult
	) : AbstractClientFactory<RabbitMQReceiver, RabbitMQProducer, RabbitMQProperties>(
		properties,
		arguments
	) {

		private val declaredExchanges: MutableSet<String> = mutableSetOf()
		private val declaredQueues: MutableSet<String> = mutableSetOf()

		private val connectionFactory =
			ConnectionFactory().apply {
				username = properties.username
				password = properties.password
				virtualHost = properties.virtualHost
				host = properties.host
				port = properties.port
			}

		private val adminConnection = connectionFactory.newConnection("admin")
		private val adminChannel = adminConnection.createChannel()

		override fun closeImpl() {
			adminChannel.close()
			adminConnection.close()
		}

		override fun createDestination(config: DestinationConfiguration) {
			val exchangeName = config.topicName
			val queueName = config.queueName

			if (!declaredQueues.contains(queueName)) {
				adminChannel.queueDeclare(
					queueName,
					true,
					false,
					false,
					mapOf()
				)
				declaredQueues.add(queueName)
			}

			if (exchangeName != null) {
				adminChannel.exchangeDeclare(
					exchangeName,
					BuiltinExchangeType.DIRECT,
					true
				)
				declaredExchanges.add(exchangeName)

				adminChannel.queueBind(queueName, exchangeName, queueName)
			}

		}

		override fun cleanUpDestinations() {
			val deleteQueues: List<() -> Unit> = declaredQueues
				.map { { adminChannel.queueDelete(it) } }

			val deleteExchanges: List<() -> Unit> = declaredExchanges
				.map { { adminChannel.exchangeDelete(it) } }

			runDelayError(deleteQueues + deleteExchanges)
		}

		override fun createReceiverImpl(receiverConfiguration: ReceiverConfiguration?): RabbitMQReceiver =
			RabbitMQReceiver(properties, arguments, connectionFactory.newConnection())

		override fun createProducerImpl(producerConfiguration: ProducerConfiguration?): RabbitMQProducer =
			RabbitMQProducer(properties, connectionFactory.newConnection())

	}

	override fun clientFactory(): (ClientProperties, ArgumentParser.ParseResult) -> ClientFactory =
		{ props, args -> RabbitMQClientFactory(props as RabbitMQProperties, args) }

	override fun clientProperties(): KClass<out ClientProperties> = RabbitMQProperties::class
}
