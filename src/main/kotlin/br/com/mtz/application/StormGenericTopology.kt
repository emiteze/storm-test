package br.com.mtz.application

import br.com.mtz.bolt.SplitSentenceBolt
import br.com.mtz.bolt.WordCountBolt
import br.com.mtz.config.TopologyProperties
import org.apache.storm.LocalCluster
import org.apache.storm.StormSubmitter
import org.apache.storm.generated.StormTopology
import org.apache.storm.kafka.bolt.KafkaBolt
import org.apache.storm.kafka.bolt.mapper.FieldNameBasedTupleToKafkaMapper
import org.apache.storm.kafka.spout.KafkaSpout
import org.apache.storm.kafka.spout.KafkaSpoutConfig
import org.apache.storm.topology.TopologyBuilder
import org.slf4j.LoggerFactory
import java.util.Properties

class StormGenericTopology(private val topologyProperties: TopologyProperties) {

    private val log = LoggerFactory.getLogger(this.javaClass)

    fun runTopology() {

        val stormTopology = buildTopology()
        val stormExecutionMode = topologyProperties.stormExecutionMode
        val conf = topologyProperties.stormConfig

        when (stormExecutionMode) {
            "cluster" -> StormSubmitter.submitTopology(topologyProperties.topologyName, conf, stormTopology)
            else -> {
                val cluster = LocalCluster()
                cluster.submitTopology(topologyProperties.topologyName, conf, stormTopology)
                Thread.sleep(topologyProperties.localTimeExecution.toLong())
                cluster.killTopology(topologyProperties.topologyName)
                cluster.shutdown()
                System.exit(0)
            }
        }

    }

    private fun buildTopology(): StormTopology {

        log.debug("Starting topology builder...")

        val builder = TopologyBuilder()

        val properties: Properties = Properties().apply {
            this.putAll(mapOf(
                "bootstrap.servers" to "3.18.107.217:9092",
                "acks" to "1",
                "key.serializer" to "org.apache.kafka.common.serialization.StringSerializer",
                "value.serializer" to "org.apache.kafka.common.serialization.StringSerializer"
            ))
        }

        val offlineTopicConfig = KafkaSpoutConfig.builder(topologyProperties.kafkaBootstrapServers, "minas.offline.input").build()
        val currentModelTopicConfig = KafkaSpoutConfig.builder(topologyProperties.kafkaBootstrapServers, "minas.current.model").build()
        val onlineTopicConfig = KafkaSpoutConfig.builder(topologyProperties.kafkaBootstrapServers, "minas.online.input").build()

        val kafkaOfflineBolt = KafkaBolt<Any, Any>()
            .withProducerProperties(properties)
            .withTopicSelector("minas.current.model")
            .withTupleToKafkaMapper(FieldNameBasedTupleToKafkaMapper())

        val kafkaOnlineBolt = KafkaBolt<Any, Any>()
            .withProducerProperties(properties)
            .withTopicSelector("minas.current.model")
            .withTupleToKafkaMapper(FieldNameBasedTupleToKafkaMapper())

        builder.setSpout("minas.offline.spout", KafkaSpout(offlineTopicConfig), 1)

        builder.setBolt("minas.offline.process.bolt", SplitSentenceBolt(), 3).shuffleGrouping("minas.offline.spout")

        builder.setBolt("minas.offline.publish.bolt", kafkaOfflineBolt, 3).shuffleGrouping("minas.offline.process.bolt")

        builder.setSpout("minas.model.spout", KafkaSpout(currentModelTopicConfig), 1)

        builder.setSpout("minas.online.spout", KafkaSpout(onlineTopicConfig), 1)

        builder.setBolt("minas.online.process.bolt", WordCountBolt(), 3).shuffleGrouping("minas.online.spout").shuffleGrouping("minas.model.spout")

        builder.setBolt("minas.online.publish.bolt", kafkaOnlineBolt, 3).shuffleGrouping("minas.online.process.bolt")

        return builder.createTopology()

    }

}

fun main(args: Array<String>) {

    System.setProperty("storm.jar", "/Users/mateus/Documents/Pessoal/storm-test/target/storm-test-1.0-SNAPSHOT.jar")
    val topologyProperties = TopologyProperties("application.properties")
    val stormTopology = StormGenericTopology(topologyProperties)
    stormTopology.runTopology()

}