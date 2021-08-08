package br.edu.ufabc.chokitus.mq.message

class MessageBatch<M : AbstractMessage>(
	val messages: List<M>,
	private val ackFn: (List<M>) -> Unit = {}
) : Iterable<M> {
	fun ackAll(): Unit = ackFn(messages)
	fun isNotEmpty() = messages.isNotEmpty()

	override fun iterator(): Iterator<M> = messages.iterator()

	companion object {
		fun <M : AbstractMessage> empty() = MessageBatch<M>(listOf()) {}
	}

}
