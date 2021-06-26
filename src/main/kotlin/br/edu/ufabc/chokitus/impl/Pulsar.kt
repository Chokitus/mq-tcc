package br.edu.ufabc.chokitus.impl

import br.edu.ufabc.chokitus.mq.client.AbstractProducer
import br.edu.ufabc.chokitus.mq.client.AbstractReceiver
import br.edu.ufabc.chokitus.mq.factory.AbstractClientFactory
import br.edu.ufabc.chokitus.mq.message.AbstractMessage
import br.edu.ufabc.chokitus.mq.properties.ClientProperties
import br.edu.ufabc.chokitus.util.Extensions.closeAll
import java.util.UUID
import java.util.concurrent.TimeUnit.MILLISECONDS
import org.apache.pulsar.client.api.Consumer
import org.apache.pulsar.client.api.Message
import org.apache.pulsar.client.api.MessageId
import org.apache.pulsar.client.api.Producer
import org.apache.pulsar.client.api.PulsarClient
import org.apache.pulsar.client.api.SubscriptionInitialPosition
import org.apache.pulsar.client.api.SubscriptionType

object Pulsar {

	data class PulsarProperties(
		val serviceURL: String,
		val receiveTimeoutMs: Long
	) : ClientProperties()

	class PulsarMessage(
		private val message: Message<ByteArray>,
		internal val consumer: Consumer<ByteArray>,
	) : AbstractMessage() {

		override fun body(): ByteArray = message.data

		override fun ack() = consumer.acknowledge(message.messageId)
		override fun nack() = consumer.negativeAcknowledge(message.messageId)

		fun messageId(): MessageId = message.messageId
	}

	/**
	 * This client represents a consumer to a single subscription under any topic.
	 *
	 * @property pulsarClient PulsarClient
	 * @property consumerByTopic MutableMap<String, Consumer<ByteArray>>
	 * @constructor
	 */
	class PulsarReceiver(
		properties: PulsarProperties,
		private val pulsarClient: PulsarClient,
	) : AbstractReceiver<Consumer<ByteArray>, PulsarMessage, PulsarProperties>(properties) {

		private val consumerByTopic: MutableMap<String, Consumer<ByteArray>> = mutableMapOf()
		private val randomSubscriptionName = UUID.randomUUID().toString()

		override fun close() {
			consumerByTopic.values.closeAll()
		}

		override fun receiveBatch(
			destination: String,
			properties: PulsarProperties?
		): List<PulsarMessage> =
			getReceiver(destination).let { receiver ->
				receiver
					.batchReceive()
					.map { PulsarMessage(it, receiver) }
			}

		override fun receive(destination: String, properties: PulsarProperties?): PulsarMessage? =
			getReceiver(destination)
				.let { receiver ->
					receiver
						.receive(this.properties.receiveTimeoutMs.toInt(), MILLISECONDS)
						?.let { message -> PulsarMessage(message, receiver) }
				}

		override fun ackAll(messages: List<PulsarMessage>) {
			messages
				.groupBy { it.consumer }
				.forEach { (consumer, messages) ->
					consumer.acknowledge(messages.map { it.messageId() })
				}
		}

		override fun getReceiver(topic: String, properties: PulsarProperties?): Consumer<ByteArray> =
			consumerByTopic.getOrPut(topic) {
				pulsarClient.newConsumer().apply {
					topic(topic)
					subscriptionName(topic)
					// Consume from the first non-consumed message
					subscriptionInitialPosition(SubscriptionInitialPosition.Earliest)

					// Round-robin distribution within this Subscription
					subscriptionType(SubscriptionType.Shared)
				}
					.subscribe()
			}

		private fun ackFun(receiver: Consumer<ByteArray>): (Message<*>) -> Unit =
			{ receiver.acknowledge(it) }

		private fun nackFun(receiver: Consumer<ByteArray>): (Message<*>) -> Unit =
			{ receiver.negativeAcknowledge(it) }
	}

	class PulsarProducer(
		properties: PulsarProperties,
		private val pulsarClient: PulsarClient,
	) : AbstractProducer<Producer<ByteArray>, PulsarMessage, PulsarProperties>(properties) {

		private val producerByTopic: MutableMap<String, Producer<ByteArray>> = mutableMapOf()

		override fun close() {
			producerByTopic.values.closeAll()
		}

		override fun produce(destination: String, body: ByteArray, properties: PulsarProperties?) {
			getProducer(destination, properties).send(body)
		}

		override fun getProducer(
			destination: String,
			properties: PulsarProperties?
		): Producer<ByteArray> =
			producerByTopic.getOrPut(destination) {
				pulsarClient.newProducer().apply {
					topic(destination)
				}
					.create()
			}

	}

	class PulsarClientFactory(
		properties: PulsarProperties
	) : AbstractClientFactory<PulsarReceiver, PulsarProducer, PulsarProperties>(properties) {

		private lateinit var client: PulsarClient

		override fun start() {
			client = PulsarClient.builder().serviceUrl(properties.serviceURL).build()
		}

		override fun createReceiverImpl(): PulsarReceiver =
			PulsarReceiver(
				properties,
				client
			)

		override fun createProducerImpl(): PulsarProducer =
			PulsarProducer(
				properties,
				client
			)

	}

}