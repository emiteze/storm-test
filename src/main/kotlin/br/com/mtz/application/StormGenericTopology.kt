package br.com.mtz.application

import br.com.mtz.bolt.PrintCountBolt
import br.com.mtz.bolt.WordCountBolt
import br.com.mtz.config.TopologyProperties
import br.com.mtz.spout.RandomWordSpout
import org.apache.storm.LocalCluster
import org.apache.storm.StormSubmitter
import org.apache.storm.generated.StormTopology
import org.apache.storm.topology.TopologyBuilder
import org.apache.storm.tuple.Fields
import org.slf4j.LoggerFactory

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

        builder.setSpout("random.word.spout", RandomWordSpout(), 5)

        builder.setBolt("word.count.bolt", WordCountBolt(), 5).shuffleGrouping("random.word.spout")

        builder.setBolt("print.count.bolt", PrintCountBolt(), 2).fieldsGrouping("word.count.bolt", Fields("word", "count"))

        return builder.createTopology()
    }

}

fun main(args: Array<String>) {

    System.setProperty("storm.jar", "/home/mateus-cruz/Documents/Projects/pessoal/storm-test/target/storm-test-1.0-SNAPSHOT.jar")
    val topologyProperties = TopologyProperties("application.properties")
    val stormTopology = StormGenericTopology(topologyProperties)
    stormTopology.runTopology()

}