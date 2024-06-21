package io.github.toyota32k.rock

import java.nio.ByteBuffer

open class RockWriter(private val writer:IRawRockWriter) {
    private fun Long.toByteArray(): ByteArray {
        return ByteBuffer.allocate(8).putLong(this).array()
    }

    private fun Int.toByteArray(): ByteArray {
        return ByteBuffer.allocate(4).putInt(this).array()
    }
    private fun Double.toByteArray(): ByteArray {
        return ByteBuffer.allocate(8).putDouble(this).array()
    }
    private fun Float.toByteArray(): ByteArray {
        return ByteBuffer.allocate(4).putFloat(this).array()
    }
    private fun Boolean.toByteArray(): ByteArray {
        return if (this) byteArrayOf(1) else byteArrayOf(0)
    }

    fun put(key: String, value: String) {
        writer.put(key.toByteArray(), value.toByteArray())
    }

    fun put(key: String, value: Int) {
        writer.put(key.toByteArray(), value.toByteArray())
    }

    fun put(key: String, value: Long) {
        writer.put(key.toByteArray(), value.toByteArray())
    }

    fun put(key: String, value: Double) {
        writer.put(key.toByteArray(), value.toByteArray())
    }

    fun put(key: String, value: Float) {
        writer.put(key.toByteArray(), value.toByteArray())
    }

    fun put(key: String, value: Boolean) {
        writer.put(key.toByteArray(), value.toByteArray())
    }

    fun put(key: String, value: ByteArray) {
        writer.put(key.toByteArray(), value)
    }

    open fun column(cf:String?):RockWriter {
        return if(cf==null) {
            RockWriter(writer)
        } else {
            RockWriter(writer.column(cf))
        }
    }

    open fun columns(includeDefault:Boolean=false):List<RockWriter> {
        return writer.columns(includeDefault).map { it.toWriter() }
    }

    fun delete(key: String) {
        writer.delete(key.toByteArray())
    }
    fun clean() {
        writer.clean()
    }
}

open class RockReaderWriter(private val reader: IRawRockReaderWriter) : RockWriter(reader) {
    private fun ByteArray.toLong(): Long {
        return ByteBuffer.wrap(this).long
    }
    private fun ByteArray.toInt(): Int {
        return ByteBuffer.wrap(this).int
    }
    private fun ByteArray.toDouble(): Double {
        return ByteBuffer.wrap(this).double
    }
    private fun ByteArray.toFloat(): Float {
        return ByteBuffer.wrap(this).float
    }
    private fun ByteArray.toBoolean(): Boolean {
        return this[0] == 1.toByte()
    }

    fun getString(key: String): String? {
        return reader.get(key.toByteArray())?.toString(Charsets.UTF_8)
    }

    fun getInt(key: String): Int? {
        return reader.get(key.toByteArray())?.toInt()
    }
    fun getInt(key: String, def:Int): Int {
        return getInt(key) ?: def
    }

    fun getLong(key: String): Long? {
        return reader.get(key.toByteArray())?.toLong()
    }

    fun getLong(key: String, def: Long): Long {
        return getLong(key) ?: def
    }

    fun getDouble(key: String): Double? {
        return reader.get(key.toByteArray())?.toDouble()
    }

    fun getDouble(key: String, def: Double): Double {
        return getDouble(key) ?: def
    }

    fun getFloat(key: String): Float? {
        return reader.get(key.toByteArray())?.toFloat()
    }

    fun getFloat(key: String, def: Float): Float {
        return getFloat(key) ?: def
    }

    fun getBoolean(key: String): Boolean? {
        return reader.get(key.toByteArray())?.toBoolean()
    }

    fun getBoolean(key: String, def: Boolean): Boolean {
        return getBoolean(key) ?: def
    }
    fun getBlob(key: String): ByteArray? {
        return reader.get(key.toByteArray())
    }

    fun listAsString(): Sequence<Pair<String, String>> {
        return reader.list {k,v-> Pair(k.toString(Charsets.UTF_8), v.toString(Charsets.UTF_8))}
    }
    fun listAsInt(): Sequence<Pair<String, Int>> {
        return reader.list {k,v-> Pair(k.toString(Charsets.UTF_8), v.toInt())}
    }
    fun listAsLong(): Sequence<Pair<String, Long>> {
        return reader.list {k,v-> Pair(k.toString(Charsets.UTF_8), v.toLong())}
    }
    fun listAsDouble(): Sequence<Pair<String, Double>> {
        return reader.list {k,v-> Pair(k.toString(Charsets.UTF_8), v.toDouble())}
    }
    fun listAsFloat(): Sequence<Pair<String, Float>> {
        return reader.list {k,v-> Pair(k.toString(Charsets.UTF_8), v.toFloat())}
    }
    fun listAsBoolean(): Sequence<Pair<String, Boolean>> {
        return reader.list {k,v-> Pair(k.toString(Charsets.UTF_8), v.toBoolean())}
    }
    fun listAsBlob(): Sequence<Pair<String, ByteArray>> {
        return reader.list {k,v-> Pair(k.toString(Charsets.UTF_8), v)}
    }

    fun isExist(key: String): Boolean {
        return reader.isExist(key.toByteArray())
    }

    fun isExist2(key: String): Boolean {
        return reader.isExist2(key.toByteArray())
    }

    override fun column(cf: String?): RockReaderWriter {
        return if(cf==null) {
            RockReaderWriter(reader)
        } else {
            RockReaderWriter(reader.column(cf))
        }
    }

    override fun columns(includeDefault: Boolean): List<RockReaderWriter> {
        return reader.columns(includeDefault).map { it.toReaderWriter() }
    }
}

fun IRawRockWriter.toWriter(): RockWriter {
    return RockWriter(this)
}
inline fun <T> IRawRockWriter.withWriter(block: RockWriter.()->T):T {
    return RockWriter(this).block()
}

fun IRawRockReaderWriter.toReaderWriter(): RockReaderWriter {
    return RockReaderWriter(this)
}
inline fun <T> IRawRockReaderWriter.withReaderWriter(block: RockReaderWriter.()->T):T {
    return RockReaderWriter(this).block()
}