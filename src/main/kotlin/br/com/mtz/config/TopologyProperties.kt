package br.com.mtz.config

import org.apache.storm.Config
import org.apache.storm.metric.LoggingMetricsConsumer
import java.io.FileInputStream
import java.util.ArrayList
import java.util.Properties

class TopologyProperties(fileName: String) {

    lateinit var topologyName: String
    lateinit var stormExecutionMode: String
    lateinit var localTimeExecution: String
    lateinit var topologyBatchEmitMillis: String
    lateinit var zookeeperHosts: String
    lateinit var nimbusSeeds: String
    lateinit var nimbusPort: String
    lateinit var debug: String
    lateinit var stormWorkersNumber: String
    lateinit var maxTaskParallism: String
    var stormConfig: Config = Config()

    init {
        setProperties(fileName)
        stormConfig["topologyName"] = topologyName
        stormConfig["localTimeExecution"] = localTimeExecution
        stormConfig["stormExecutionMode"] = stormExecutionMode
        stormConfig["topologyBatchEmitMillis"] = topologyBatchEmitMillis
        stormConfig["zookeeperHosts"] = zookeeperHosts
        stormConfig["nimbusSeeds"] = nimbusSeeds
        stormConfig["nimbusPort"] = nimbusPort
        stormConfig["debug"] = debug
        stormConfig["stormWorkersNumber"] = stormWorkersNumber
        stormConfig["maxTaskParallism"] = maxTaskParallism
        stormConfig[Config.TOPOLOGY_TRIDENT_BATCH_EMIT_INTERVAL_MILLIS] = Integer.parseInt(topologyBatchEmitMillis)
        stormConfig[Config.NIMBUS_SEEDS] = parseHosts(nimbusSeeds)
        stormConfig[Config.NIMBUS_THRIFT_PORT] = Integer.parseInt(nimbusPort)
        stormConfig[Config.STORM_ZOOKEEPER_PORT] = parseZkPort(zookeeperHosts)
        stormConfig[Config.STORM_ZOOKEEPER_SERVERS] = parseHosts(zookeeperHosts)
        stormConfig.setNumWorkers(Integer.parseInt(stormWorkersNumber))
        stormConfig.setMaxTaskParallelism(Integer.parseInt(maxTaskParallism))
        stormConfig.setDebug(debug.toBoolean())
        stormConfig.registerMetricsConsumer(LoggingMetricsConsumer::class.java, 1)
    }

    private fun setProperties(fileName: String) {
        val properties: Properties = readPropertiesFile(fileName)
        topologyName = properties.getProperty("storm.topology.name", "defaultTopologyName")
        localTimeExecution = properties.getProperty("storm.local.execution.time", "20000")
        stormExecutionMode = properties.getProperty("storm.execution.mode", "local")
        topologyBatchEmitMillis = properties.getProperty("storm.topology.batch.interval.miliseconds", "2000")
        zookeeperHosts = properties.getProperty("zookeeper.hosts")
        nimbusSeeds = properties.getProperty("storm.nimbus.seeds", "localhost")
        nimbusPort = properties.getProperty("storm.nimbus.port", "6627")
        debug = properties.getProperty("storm.execution.debug", "true")
        stormWorkersNumber = properties.getProperty("storm.workers.number", "1")
        maxTaskParallism = properties.getProperty("storm.max.task.parallelism", "2")
    }

    private fun readPropertiesFile(fileName: String): Properties {
        val properties: Properties = Properties()
        properties.load(javaClass.classLoader.getResourceAsStream(fileName))
        return properties
    }

    private fun parseZkPort(zkNodes: String): Int {
        val hostsAndPorts = zkNodes.split(",")
        return Integer.parseInt(hostsAndPorts[0].split(":")[1])
    }

    private fun parseHosts(zkNodes: String): List<String> {

        val hostsAndPorts = zkNodes.split(",")
        val hosts = ArrayList<String>(hostsAndPorts.size)

        for (i in hostsAndPorts.indices) {
            hosts.add(i, hostsAndPorts[i].split(":")[0])
        }
        return hosts
    }

}