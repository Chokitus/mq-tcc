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
import java.time.Duration
import java.util.Optional
import java.util.Properties
import java.util.concurrent.ExecutionException
import kotlin.math.max
import kotlin.reflect.KClass
import org.apache.kafka.clients.admin.AdminClient
import org.apache.kafka.clients.admin.NewTopic
import org.apache.kafka.clients.consumer.ConsumerConfig
import org.apache.kafka.clients.consumer.ConsumerRecord
import org.apache.kafka.clients.consumer.KafkaConsumer
import org.apache.kafka.clients.consumer.OffsetAndMetadata
import org.apache.kafka.clients.producer.ProducerConfig
import org.apache.kafka.clients.producer.ProducerRecord
import org.apache.kafka.common.TopicPartition
import org.apache.kafka.common.errors.TopicExistsException
import org.slf4j.LoggerFactory
import org.apache.kafka.clients.producer.KafkaProducer as TrueKafkaProducer

object Kafka : BenchmarkDefiner {

	data class KafkaProperties(
		val hosts: String,
		val commitIntervalMs: Int
	) : ClientProperties()

	class KafkaMessage(
		private val consumerRecord: ConsumerRecord<String, ByteArray>,
		private val ackFn: () -> Unit
	) : AbstractMessage() {
		override fun body(): ByteArray {
			return consumerRecord.value()
		}

		override fun ack() = ackFn()

		override fun nack() {
			TODO("Not yet implemented")
		}

	}

	class KafkaReceiver(
		private val kafkaConsumer: KafkaConsumer<String, ByteArray>,
		properties: KafkaProperties,
	) : AbstractReceiver<KafkaConsumer<String, ByteArray>, KafkaMessage, KafkaProperties>(
		properties
	) {

		private val log = LoggerFactory.getLogger(javaClass)

		private val idsToCommit: MutableMap<TopicPartition, Long> = mutableMapOf()
		private val commitEach = Duration.ofMillis(properties.commitIntervalMs.toLong())
		private var lastCommit = System.nanoTime()

		override fun receiveBatch(
			destination: String,
			properties: ReceiverConfiguration
		): MessageBatch<KafkaMessage> =
			getReceiver(destination, this.properties)
				.poll(Duration.ofMillis(1000))
				.map { msg ->
					KafkaMessage(msg) {
						idsToCommit.putOrRecompute(
							key = TopicPartition(msg.topic(), msg.partition()),
							defaultValue = { msg.offset() + 1 },
							remapValue = { max(it, msg.offset() + 1) }
						)
					}
				}
				.let(::MessageBatch)

		override fun receive(destination: String, properties: ReceiverConfiguration): KafkaMessage? =
			getReceiver(destination, this.properties)
				.let { receiver ->
					receiver.poll(Duration.ofMillis(1000))
						.firstOrNull()
						?.let { msg ->
							KafkaMessage(msg) {
								idsToCommit.putOrRecompute(
									key = TopicPartition(msg.topic(), msg.partition()),
									defaultValue = { msg.offset() + 1 },
									remapValue = { max(it, msg.offset() + 1) }
								)
							}
						}
				}

		override fun close() {
			kafkaConsumer.close()
		}

		override fun getReceiver(
			destination: String,
			properties: KafkaProperties?
		): KafkaConsumer<String, ByteArray> =
			kafkaConsumer.also {
				if (System.nanoTime() - lastCommit > commitEach.toNanos()) {
					commit(it)
				}
			}

		private fun commit(kafkaConsumer: KafkaConsumer<String, ByteArray>) {
			lastCommit = System.nanoTime()
			if (idsToCommit.isNotEmpty()) {
				log.info("Acking $idsToCommit messages...")
				kafkaConsumer.commitSync(idsToCommit.mapValues { OffsetAndMetadata(it.value) })
				idsToCommit.clear()
			}
		}

	}

	class KafkaProducer(
		private val kafkaProducer: TrueKafkaProducer<String, ByteArray>,
		properties: KafkaProperties
	) : AbstractProducer<TrueKafkaProducer<String, ByteArray>, KafkaMessage, KafkaProperties>(
		properties
	) {
		override fun produce(destination: String, body: ByteArray, properties: KafkaProperties?) {
			getProducer(destination, this.properties)
				.send(ProducerRecord(destination, null, body))
				.get()
		}

		override fun getProducer(
			destination: String,
			properties: KafkaProperties?
		): TrueKafkaProducer<String, ByteArray> =
			kafkaProducer

		override fun close() {
			kafkaProducer.close()
		}

		override fun produceBatch(
			destination: String,
			bodies: Iterable<ByteArray>,
			properties: KafkaProperties?
		) {
			val producer = getProducer(destination, this.properties)
			bodies.map {
				producer.send(ProducerRecord(destination, null, it))
			}
				.forEach { it.get() }
		}

	}

	class KafkaClientFactory(
		properties: KafkaProperties,
		arguments: ArgumentParser.ParseResult
	) : AbstractClientFactory<KafkaReceiver, KafkaProducer, KafkaProperties>(
		properties,
		arguments
	) {

		private val byteArrayDes = "org.apache.kafka.common.serialization.ByteArrayDeserializer"
		private val stringDes = "org.apache.kafka.common.serialization.StringDeserializer"

		private val byteArraySer = "org.apache.kafka.common.serialization.ByteArraySerializer"
		private val stringSer = "org.apache.kafka.common.serialization.StringSerializer"

		private val adminClient = AdminClient.create(
			Properties().apply {
				put(ConsumerConfig.BOOTSTRAP_SERVERS_CONFIG, properties.hosts)
			}
		)

		private val createdTopics: MutableList<String> = mutableListOf()

		override fun createReceiverImpl(receiverConfiguration: ReceiverConfiguration?): KafkaReceiver =
			receiverConfiguration!!.let {
				Properties().apply {
					put(ConsumerConfig.BOOTSTRAP_SERVERS_CONFIG, properties.hosts)
					put(ConsumerConfig.GROUP_ID_CONFIG, "simple-group")
					put(ConsumerConfig.KEY_DESERIALIZER_CLASS_CONFIG, stringDes)
					put(ConsumerConfig.VALUE_DESERIALIZER_CLASS_CONFIG, byteArrayDes)
					put(ConsumerConfig.ENABLE_AUTO_COMMIT_CONFIG, "false")
					put(ConsumerConfig.AUTO_OFFSET_RESET_CONFIG, "earliest")
					put(ConsumerConfig.MAX_POLL_RECORDS_CONFIG, 50)
				}
			}
				.let { KafkaConsumer<String, ByteArray>(it) }
				.also {
					it.subscribe(
						listOf(
							receiverConfiguration.topicName ?: receiverConfiguration.queueName
						)
					)
				}
				.let { KafkaReceiver(it, properties) }

		override fun createProducerImpl(producerConfiguration: ProducerConfiguration?): KafkaProducer =
			producerConfiguration!!.let {
				Properties().apply {
					put(ProducerConfig.BOOTSTRAP_SERVERS_CONFIG, properties.hosts)
					put(ProducerConfig.KEY_SERIALIZER_CLASS_CONFIG, stringSer)
					put(ProducerConfig.VALUE_SERIALIZER_CLASS_CONFIG, byteArraySer)
					put(ProducerConfig.ACKS_CONFIG, "-1")
				}
			}
				.let { TrueKafkaProducer<String, ByteArray>(it) }
				.let { KafkaProducer(it, properties) }

		override fun createDestination(config: DestinationConfiguration) {
			NewTopic(
				config.topicOrQueue(),
				Optional.of(config.additionalInfo().getNotNull("subscriberCount")),
				Optional.empty()
			)
				.also { createdTopics.add(it.name()) }
				.let {
					runCatching {
						adminClient.createTopics(listOf(it))
							.all().get()
					}
						.onFailure {
							if (it is ExecutionException && it.cause !is TopicExistsException) {
								throw it
							}
						}

				}
		}

		override fun cleanUpDestinations() {
			adminClient.deleteTopics(createdTopics).all().get()
		}

		override fun closeImpl() {
			adminClient.close()
		}

	}

	private fun <T : Any, V : Any> MutableMap<T, V>.putOrRecompute(
		key: T,
		defaultValue: () -> V,
		remapValue: (V) -> V
	) =
		compute(key) { _, value ->
			value?.let(remapValue) ?: defaultValue()
		}

	override fun clientFactory(): (ClientProperties, ArgumentParser.ParseResult) -> ClientFactory =
		{ props, args -> KafkaClientFactory(props as KafkaProperties, args) }

	override fun clientProperties(): KClass<out ClientProperties> =
		KafkaProperties::class

}
