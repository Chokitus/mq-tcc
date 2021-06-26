package br.edu.ufabc.chokitus.mq.client

import br.edu.ufabc.chokitus.mq.message.AbstractMessage
import br.edu.ufabc.chokitus.mq.properties.ClientProperties

abstract class AbstractProducer<P : Any, M : AbstractMessage, Y : ClientProperties>(
	protected val properties: Y
) : AutoCloseable, Startable {
	abstract fun produce(destination: String, body: ByteArray, properties: Y? = null)
	protected abstract fun getProducer(destination: String, properties: Y?): P
}
