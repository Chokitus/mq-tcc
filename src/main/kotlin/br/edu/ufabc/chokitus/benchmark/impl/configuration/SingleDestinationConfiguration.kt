package br.edu.ufabc.chokitus.benchmark.impl.configuration

data class SingleDestinationConfiguration(
	val receiverCount: Int,
	val producerCount: Int,
	val messageCount: Int,
	val messageSize: Int
)
