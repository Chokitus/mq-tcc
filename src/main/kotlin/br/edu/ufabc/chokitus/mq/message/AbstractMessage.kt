package br.edu.ufabc.chokitus.mq.message

abstract class AbstractMessage {
	protected lateinit var body: ByteArray

	abstract fun body(): ByteArray
	abstract fun ack(): Unit
	abstract fun nack(): Unit

	fun bodyAsString(): String = body().decodeToString()
}
