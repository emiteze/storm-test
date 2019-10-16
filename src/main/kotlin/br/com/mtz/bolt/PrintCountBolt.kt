package br.com.mtz.bolt

import org.apache.storm.topology.BasicOutputCollector
import org.apache.storm.topology.OutputFieldsDeclarer
import org.apache.storm.topology.base.BaseBasicBolt
import org.apache.storm.tuple.Fields
import org.apache.storm.tuple.Tuple
import org.apache.storm.tuple.Values
import org.slf4j.LoggerFactory

class PrintCountBolt : BaseBasicBolt() {

    private val log = LoggerFactory.getLogger(this.javaClass)

    override fun execute(tuple: Tuple, colletor: BasicOutputCollector) {
        log.info("Loggin count of words {}", tuple.getString(0))
        colletor.emit(Values(tuple.getString(0)))
    }

    override fun declareOutputFields(declarer: OutputFieldsDeclarer) {
        declarer.declare(Fields("counts"))
    }

}