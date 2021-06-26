package br.edu.ufabc.chokitus.util

object Extensions {

	fun <T : AutoCloseable> Collection<T>.closeAll() =
		mapNotNull { runCatching { it.close() }.exceptionOrNull() }
			.reduceOrNull { acc, e -> acc.apply { addSuppressed(e) } }
			?.let { throw it }

}
