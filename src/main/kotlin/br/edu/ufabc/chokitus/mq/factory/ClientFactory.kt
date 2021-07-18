package br.edu.ufabc.chokitus.mq.factory

import br.edu.ufabc.chokitus.benchmark.impl.configuration.DestinationConfiguration
import br.edu.ufabc.chokitus.mq.client.AbstractProducer
import br.edu.ufabc.chokitus.mq.client.AbstractReceiver
import br.edu.ufabc.chokitus.mq.client.Startable
import br.edu.ufabc.chokitus.mq.message.AbstractMessage
import br.edu.ufabc.chokitus.mq.properties.ClientProperties
import br.edu.ufabc.chokitus.util.Extensions.closeAll
import java.io.Closeable
import java.util.LinkedList

/**
 * This class should take note of every [Closeable] created between the tests made, and close them
 * when the [close] method is called.
 *
 * @param R : AbstractReceiver<*, *, Y>
 * @param P : AbstractProducer<*, *, Y>
 * @param Y : ClientProperties
 * @property properties Y
 * @constructor
 */
abstract class AbstractClientFactory<
		R : AbstractReceiver<*, out AbstractMessage, Y>,
		P : AbstractProducer<*, out AbstractMessage, Y>,
		Y : ClientProperties
		>(
	protected val properties: Y
) : AutoCloseable, Startable {

	private val receivers: MutableList<R> = LinkedList()
	private val producers: MutableList<P> = LinkedList()

	fun createReceiver() =
		createReceiverImpl().also(receivers::add)

	fun createProducer() =
		createProducerImpl().also(producers::add)

	open fun createDestination(config: DestinationConfiguration): Unit = Unit
	open fun cleanUpDestinations(): Unit = Unit

	protected abstract fun createReceiverImpl(): R
	protected abstract fun createProducerImpl(): P

	override fun close() {
		(receivers + producers).closeAll()
		closeImpl()
	}

	open fun closeImpl(): Unit = Unit

}
