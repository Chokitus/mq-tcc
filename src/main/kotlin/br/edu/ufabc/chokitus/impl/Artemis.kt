package br.edu.ufabc.chokitus.impl

import br.edu.ufabc.chokitus.benchmark.ClientFactory
import br.edu.ufabc.chokitus.benchmark.impl.configuration.DestinationConfiguration
import br.edu.ufabc.chokitus.benchmark.impl.configuration.ReceiverConfiguration
import br.edu.ufabc.chokitus.mq.BenchmarkDefiner
import br.edu.ufabc.chokitus.mq.client.AbstractProducer
import br.edu.ufabc.chokitus.mq.client.AbstractReceiver
import br.edu.ufabc.chokitus.mq.factory.AbstractClientFactory
import br.edu.ufabc.chokitus.mq.message.AbstractMessage
import br.edu.ufabc.chokitus.mq.properties.ClientProperties
import br.edu.ufabc.chokitus.util.Extensions.closeAll
import br.edu.ufabc.chokitus.util.Extensions.runDelayError
import kotlin.reflect.KClass
import org.apache.activemq.artemis.api.core.ActiveMQQueueExistsException
import org.apache.activemq.artemis.api.core.QueueConfiguration
import org.apache.activemq.artemis.api.core.RoutingType
import org.apache.activemq.artemis.api.core.SimpleString
import org.apache.activemq.artemis.api.core.client.ActiveMQClient
import org.apache.activemq.artemis.api.core.client.ClientConsumer
import org.apache.activemq.artemis.api.core.client.ClientMessage
import org.apache.activemq.artemis.api.core.client.ClientProducer
import org.apache.activemq.artemis.api.core.client.ClientSession
import org.apache.activemq.artemis.api.core.client.ClientSessionFactory
import org.apache.activemq.artemis.api.core.client.ServerLocator

object Artemis : BenchmarkDefiner {

	data class ArtemisProperties(
		internal val serverLocatorURL: String,
		internal val username: String,
		internal val password: String,
		internal val ackBatchSize: Int
	) : ClientProperties()

	class ArtemisMessage(
		private val message: ClientMessage
	) : AbstractMessage() {

		override fun body(): ByteArray =
			ByteArray(message.bodySize).also(message.readOnlyBodyBuffer::readBytes)

		override fun ack() {
			message.acknowledge()
		}

		override fun nack() {
			/**
			 * TODO
			 */
		}
	}

	class ArtemisReceiver(
		properties: ArtemisProperties,
		private val clientSession: ClientSession,
	) : AbstractReceiver<ClientConsumer, ArtemisMessage, ArtemisProperties>(
		properties
	) {

		private val receiverByQueue: MutableMap<String, ClientConsumer> = mutableMapOf()

		override fun start() {
			clientSession.start()
		}

		override fun receiveBatch(
			destination: String,
			properties: ReceiverConfiguration
		): List<ArtemisMessage> {
			return listOf()
		}

		override fun receive(destination: String, properties: ReceiverConfiguration): ArtemisMessage? =
			getReceiver(destination)
				.receive(500)
				?.let(::ArtemisMessage)

		override fun ackAll(messages: List<ArtemisMessage>): Unit =
			messages.forEach { it.ack() }

		override fun getReceiver(destination: String, properties: ArtemisProperties?): ClientConsumer =
			receiverByQueue.getOrPut(destination) {
				clientSession.createConsumer(destination)
			}

		override fun close() {
			receiverByQueue.values.closeAll()
		}

	}

	class ArtemisProducer(
		properties: ArtemisProperties,
		private val clientSession: ClientSession,
	) : AbstractProducer<ClientProducer, ArtemisMessage, ArtemisProperties>(
		properties
	) {

		override fun start() {
			clientSession.start()
		}

		private val clientProducer = clientSession.createProducer()

		override fun produce(destination: String, body: ByteArray, properties: ArtemisProperties?) {
			val message = clientSession.createMessage(true).apply {
				bodyBuffer.writeBytes(body)
			}
			clientProducer.send(destination, message)
			clientSession.commit()
		}

		override fun getProducer(destination: String, properties: ArtemisProperties?): ClientProducer =
			clientProducer

		override fun close() {
			clientProducer.close()
		}

	}

	class ArtemisClientFactory(
		properties: ArtemisProperties
	) : AbstractClientFactory<ArtemisReceiver, ArtemisProducer, ArtemisProperties>(
		properties
	) {

		private lateinit var clientFactory: ClientSessionFactory
		private lateinit var serverLocator: ServerLocator
		private lateinit var adminSession: ClientSession

		val createdAddresses: MutableSet<String> = mutableSetOf()
		val createdQueues: MutableSet<String> = mutableSetOf()

		override fun start() {
			serverLocator = ActiveMQClient.createServerLocator(properties.serverLocatorURL).apply {
				isBlockOnDurableSend = false
				isBlockOnAcknowledge = false
			}
			clientFactory = serverLocator.createSessionFactory()
			adminSession = createSession()
		}

		override fun createReceiverImpl(): ArtemisReceiver =
			ArtemisReceiver(
				properties = properties,
				clientSession = createSession(),
			)

		override fun createProducerImpl(): ArtemisProducer =
			ArtemisProducer(
				properties = properties,
				clientSession = createSession()
			)

		override fun createDestination(config: DestinationConfiguration) {
			val value = config.additionalInfo()["routingType"] ?: "ANYCAST"
			val parsedRoutingType = RoutingType.valueOf(value)
			val addressName = config.topicOrQueue()
			val queueName = config.queueName

			if (!createdAddresses.contains(addressName)) {
				adminSession.createAddress(
					addressName.toSimpleString(),
					parsedRoutingType,
					false
				)
				createdAddresses.add(addressName)
			}

			runCatching {
				adminSession.createQueue(
					QueueConfiguration(queueName).apply {
						address = addressName.toSimpleString()
						isDurable = true
						isExclusive = false
						isEnabled = true
						isAutoCreateAddress = false
						routingType = parsedRoutingType
					}
				)
			}
				.onFailure {
					if (it !is ActiveMQQueueExistsException) {
						throw it
					}
				}

			createdQueues.add(queueName)
		}

		override fun cleanUpDestinations() {
			createdQueues
				.map { { adminSession.deleteQueue(it) } }
				.let(::runDelayError)
		}

		private fun createSession() = clientFactory.createSession(
			properties.username,
			properties.password,
			false,
			false,
			true,
			false,
			properties.ackBatchSize
		)

	}

	override fun clientFactory(): (ClientProperties) -> ClientFactory =
		{ ArtemisClientFactory(it as ArtemisProperties) }

	override fun clientProperties(): KClass<out ClientProperties> = ArtemisProperties::class

}

private fun String.toSimpleString() = SimpleString.toSimpleString(this)
