package br.edu.ufabc.chokitus.benchmark.impl.configuration

import br.edu.ufabc.chokitus.util.SmartCastingMap

data class TestConfiguration(
	val receiverConfigurations: List<ReceiverConfiguration>,
	val producerConfigurations: List<ProducerConfiguration>,
	val destinationConfigurations: List<DestinationConfiguration>,
) {
	val receiverCount: Int
		get() = receiverConfigurations.size

	val producerCount: Int
		get() = producerConfigurations.size
}

data class ReceiverConfiguration(
	val queueName: String,
	val topicName: String?,
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

	private val smartCastingInfo =
		lazy { SmartCastingMap(additionalInfo ?: mapOf()) }

	fun additionalInfo(): SmartCastingMap = smartCastingInfo.value
	fun topicOrQueue(): String = topicName ?: queueName
}
