package br.edu.ufabc.chokitus.util

object Extensions {

	fun <T : AutoCloseable> Collection<T>.closeAll() =
		runDelayError(map { { it?.close() } })

	fun runDelayError(vararg functions: () -> Unit) =
		runDelayError(functions.toList())

	fun <T> Iterable<T>.forEachRun(consumer: T.() -> Unit) =
		forEach { it.run(consumer) }

	/**
	 * Runs all functions, delaying errors, collecting exceptions to suppressed.
	 *
	 * @param functions
	 */
	fun runDelayError(functions: Collection<() -> Unit>) =
		functions
			.mapNotNull { runCatching(it).exceptionOrNull() }
			.reduceOrNull { acc, e -> acc.apply { addSuppressed(e) } }
			?.let { throw it }

}
