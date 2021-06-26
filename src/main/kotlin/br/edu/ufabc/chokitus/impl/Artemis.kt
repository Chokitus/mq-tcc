package br.edu.ufabc.chokitus.impl

import br.edu.ufabc.chokitus.mq.client.AbstractProducer
import br.edu.ufabc.chokitus.mq.client.AbstractReceiver
import br.edu.ufabc.chokitus.mq.factory.AbstractClientFactory
import br.edu.ufabc.chokitus.mq.message.AbstractMessage
import br.edu.ufabc.chokitus.mq.properties.ClientProperties
import br.edu.ufabc.chokitus.util.Extensions.closeAll
import org.apache.activemq.artemis.api.core.client.ActiveMQClient
import org.apache.activemq.artemis.api.core.client.ClientConsumer
import org.apache.activemq.artemis.api.core.client.ClientMessage
import org.apache.activemq.artemis.api.core.client.ClientProducer
import org.apache.activemq.artemis.api.core.client.ClientSession
import org.apache.activemq.artemis.api.core.client.ClientSessionFactory
import org.apache.activemq.artemis.api.core.client.ServerLocator

object Artemis {

	class ArtemisProperties(
		internal val serverLocatorURL: String
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

		override fun receiveBatch(
			destination: String,
			properties: ArtemisProperties?
		): List<ArtemisMessage> {
			return listOf()
		}

		override fun receive(destination: String, properties: ArtemisProperties?): ArtemisMessage? =
			getReceiver(destination)
				.receive(500)
				?.let(::ArtemisMessage)

		override fun ackAll(messages: List<ArtemisMessage>) = messages.forEach { it.ack() }

		override fun getReceiver(destination: String, properties: ArtemisProperties?): ClientConsumer =
			receiverByQueue.getOrPut(destination) { clientSession.createConsumer(destination) }

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

		private val clientProducer = clientSession.createProducer()

		override fun produce(destination: String, body: ByteArray, properties: ArtemisProperties?) {
			val message = clientSession.createMessage(true).apply {
				bodyBuffer.writeBytes(body)
			}
			clientProducer.send(destination, message)
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

		override fun start() {
			serverLocator = ActiveMQClient.createServerLocator(properties.serverLocatorURL)
			clientFactory = serverLocator.createSessionFactory()
		}

		override fun createReceiverImpl(): ArtemisReceiver =
			ArtemisReceiver(
				properties = properties,
				clientSession = clientFactory.createSession(),
			)

		override fun createProducerImpl(): ArtemisProducer =
			ArtemisProducer(
				properties = properties,
				clientSession = clientFactory.createSession()
			)

	}

}
