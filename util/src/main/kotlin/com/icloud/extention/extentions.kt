package com.icloud.extention

import org.apache.flink.api.common.functions.FlatMapFunction
import org.apache.flink.api.common.functions.ReduceFunction
import org.apache.flink.api.common.state.ValueState
import org.apache.flink.api.common.state.ValueStateDescriptor
import org.apache.flink.api.common.typeinfo.TypeInformation
import org.apache.flink.api.java.tuple.Tuple2
import org.apache.flink.api.java.tuple.Tuple3
import org.apache.flink.api.java.tuple.Tuple4
import org.apache.flink.streaming.api.datastream.DataStream
import org.apache.flink.streaming.api.datastream.SingleOutputStreamOperator
import org.apache.flink.streaming.api.datastream.WindowedStream
import org.apache.flink.streaming.api.functions.windowing.ProcessWindowFunction
import org.apache.flink.streaming.api.windowing.windows.Window
import kotlin.reflect.KClass

infix fun <T, U> T.tuple(u: U) = Tuple2(this, u)

infix fun <T, U, V> Tuple2<T, U>.tuple(v: V) = Tuple3(this.f0, this.f1, v)

infix fun <T, U, V, R> Tuple3<T, U, V>.tuple(r: R) = Tuple4(this.f0, this.f1, this.f2, r)


fun <InputT, OutputT> DataStream<InputT>.flatMap(
    outputT: TypeInformation<OutputT>,
    flatMapper: FlatMapFunction<InputT, OutputT>,
): SingleOutputStreamOperator<OutputT> = this.flatMap(flatMapper, outputT)

fun <T, Clazz : KClass<T>> Clazz.typeInformation(): TypeInformation<T> = TypeInformation.of(this.java)


@Suppress("UNCHECKED_CAST")
fun <T : Number> T.max(target: T): T =
    when (this) {
        is Double -> kotlin.math.max(this as Double, target as Double)
        is Long -> kotlin.math.max(this as Long, target as Long)
        is Int -> kotlin.math.max(this as Int, target as Int)
        is Float -> kotlin.math.max(this as Float, target as Float)
        else -> kotlin.math.max(this.toDouble(), target.toDouble())
    } as T

@Suppress("UNCHECKED_CAST")
fun <T : Number> T.min(target: T): T =
    when (this) {
        is Double -> kotlin.math.min(this as Double, target as Double)
        is Long -> kotlin.math.min(this as Long, target as Long)
        is Int -> kotlin.math.min(this as Int, target as Int)
        is Float -> kotlin.math.min(this as Float, target as Float)
        else -> kotlin.math.min(this.toDouble(), target.toDouble())
    } as T


@Suppress("UNCHECKED_CAST")
fun <T : Number> ValueState<T>.valueNonNull(): T {
    val value: T = this.value() ?: return 0 as T
    return value
}

fun <ElemT, KeyT, WindowT : Window, OutT> WindowedStream<ElemT, KeyT, WindowT>.reduce(
    processWindowFunction: ProcessWindowFunction<ElemT, OutT, KeyT, WindowT>,
    reduceFunction: ReduceFunction<ElemT>,
): SingleOutputStreamOperator<OutT> = this.reduce(reduceFunction, processWindowFunction)

/**
 * sugar code for create ValueStateDescriptor
 */
fun <ValueT : Any> String.valueState(
    type: KClass<ValueT>,
): ValueStateDescriptor<ValueT> = ValueStateDescriptor(this, type.java)
