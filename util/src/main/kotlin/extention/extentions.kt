package extention

import org.apache.flink.api.common.functions.FlatMapFunction
import org.apache.flink.api.common.typeinfo.TypeInformation
import org.apache.flink.api.java.tuple.Tuple2
import org.apache.flink.api.java.tuple.Tuple3
import org.apache.flink.streaming.api.datastream.DataStream
import org.apache.flink.streaming.api.datastream.SingleOutputStreamOperator
import kotlin.reflect.KClass

infix fun <T, U> T.tuple(u: U) = Tuple2(this, u)

infix fun <T, U, V> Tuple2<T, U>.tuple(v: V) = Tuple3(this.f0, this.f1, v)


fun <InputT, OutputT> DataStream<InputT>.flatMap(
    outputT: TypeInformation<OutputT>,
    flatMapper: FlatMapFunction<InputT, OutputT>,
): SingleOutputStreamOperator<OutputT> = this.flatMap(flatMapper, outputT)

fun <T, Clazz : KClass<T>> Clazz.typeInformation(): TypeInformation<T> = TypeInformation.of(this.java)