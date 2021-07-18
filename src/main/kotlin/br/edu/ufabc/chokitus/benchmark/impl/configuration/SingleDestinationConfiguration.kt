package br.edu.ufabc.chokitus.benchmark.impl.configuration

import br.edu.ufabc.chokitus.util.SmartCastingMap

data class SingleDestinationConfiguration(
	val receiverConfigurations: List<ReceiverConfiguration>,
	val producerConfigurations: List<ProducerConfiguration>,
	val destinationConfigurations: List<DestinationConfiguration>,
	val messageCount: Int,
	val messageSize: Int,
) {
	val receiverCount: Int
		get() = receiverConfigurations.size

	val producerCount: Int
		get() = producerConfigurations.size
}

data class ReceiverConfiguration(
	val queueName: String,
	val topicName: String?,
	val batchSize: Int = 1
)

data class ProducerConfiguration(
	val destinationName: String,
	val batchSize: Int = 1
)

data class DestinationConfiguration(
	val queueName: String,
	val topicName: String?,
	private val additionalInfo: Map<String, Any>?
) {
	fun additionalInfo(): SmartCastingMap = SmartCastingMap(additionalInfo ?: mapOf())
}
