package source

import model.SmokeLevel
import org.apache.flink.configuration.Configuration
import org.apache.flink.streaming.api.functions.source.RichParallelSourceFunction
import org.apache.flink.streaming.api.functions.source.SourceFunction.SourceContext
import java.security.SecureRandom
import java.util.*

class SmokeLevelSource
    : RichParallelSourceFunction<SmokeLevel>() {

    private var running: Boolean = true
    private lateinit var random: Random

    override fun open(parameters: Configuration) {
        this.random = SecureRandom()
    }

    override fun run(ctx: SourceContext<SmokeLevel>) {
        while (running) {
            when {
                random.nextGaussian() > 0.8 -> ctx.collect(SmokeLevel.HIGH)
                else -> ctx.collect(SmokeLevel.LOW)
            }

            Thread.sleep(1_000L)
        }
    }

    override fun cancel() {
        this.running = false
    }
}
