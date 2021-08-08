package br.edu.ufabc.chokitus.mq.client

import br.edu.ufabc.chokitus.benchmark.impl.configuration.ReceiverConfiguration
import br.edu.ufabc.chokitus.mq.message.AbstractMessage
import br.edu.ufabc.chokitus.mq.message.MessageBatch
import br.edu.ufabc.chokitus.mq.properties.ClientProperties

abstract class AbstractReceiver<R : Any, M : AbstractMessage, Y : ClientProperties>(
	protected val properties: Y
) : AutoCloseable, Startable {

	/**
	 * Should receive multiple messages in batch.
	 *
	 * This method should not be multiple [receive] calls, but rather use the MQ's native batch
	 * mechanism.
	 *
	 * @param destination String
	 * @param properties Y?
	 * @return List<M>
	 */
	abstract fun receiveBatch(destination: String, properties: ReceiverConfiguration): MessageBatch<M>

	/**
	 * Should receive a single message, or null if no message was available
	 *
	 * @param destination String
	 * @param properties Y?
	 * @return M?
	 */
	abstract fun receive(destination: String, properties: ReceiverConfiguration): M?

	protected abstract fun getReceiver(destination: String, properties: Y? = null): R
}
