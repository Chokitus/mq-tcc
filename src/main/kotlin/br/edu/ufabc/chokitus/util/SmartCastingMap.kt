package br.edu.ufabc.chokitus.util

import br.edu.ufabc.chokitus.mq.properties.ClientProperty
import java.util.Properties

class SmartCastingMap(
	private val backingProperties: Map<String, Any>
) {

	@Suppress("UNCHECKED_CAST")
	operator fun <T : Any> get(key: String): T? = backingProperties[key] as T?

	@Suppress("UNCHECKED_CAST")
	fun <T : Any> getNotNull(key: String): T = backingProperties[key]!! as T

	fun <T : Any> get(property: ClientProperty): T? =
		get(property.property)

	fun <T : Any> getNotNull(property: ClientProperty): T =
		getNotNull(property.property)

	fun asProperties() = Properties().putAll(backingProperties)

	companion object {
		fun empty() = SmartCastingMap(mapOf())
	}

}
