package br.edu.ufabc.chokitus.mq

import br.edu.ufabc.chokitus.benchmark.ClientFactory
import br.edu.ufabc.chokitus.mq.properties.ClientProperties
import kotlin.reflect.KClass

interface BenchmarkDefiner {
	fun clientFactory(): (ClientProperties) -> ClientFactory
	fun clientProperties(): KClass<out ClientProperties>
}
