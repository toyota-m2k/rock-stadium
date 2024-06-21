package io.github.toyota32k.rock

interface IRawRockWriter {
    fun put(key:ByteArray, value:ByteArray)
    fun delete(key:ByteArray)
    fun clean()
    fun column(cf:String?):IRawRockWriter
    fun columns(includeDefault: Boolean=false):List<IRawRockWriter>
//    fun merge(key:String, value:ByteArray)    merge method は、java版では、まだちゃんと動きそうにないので割愛
    companion object {
        val empty = object:IRawRockWriter {
            override fun put(key: ByteArray, value: ByteArray) {}
            override fun delete(key: ByteArray) {}
            override fun clean() {}
            override fun column(cf: String?): IRawRockWriter = this
            override fun columns(includeDefault: Boolean): List<IRawRockWriter> = emptyList()
        }
    }
}

interface IRawRockReaderWriter : IRawRockWriter {
    fun get(key:ByteArray):ByteArray?
    fun gets(keys:List<ByteArray>):List<ByteArray>
    fun list():Sequence<Pair<ByteArray, ByteArray>>
    fun <K,V> list(mapper:(ByteArray,ByteArray)->Pair<K,V>):Sequence<Pair<K, V>>
    override fun column(cf:String?):IRawRockReaderWriter
    override fun columns(includeDefault:Boolean): List<IRawRockReaderWriter>
    fun isExist(key: ByteArray): Boolean
    fun isExist2(key: ByteArray): Boolean

    companion object {
        val empty = object:IRawRockReaderWriter {
            override fun get(key: ByteArray): ByteArray? = null
            override fun gets(keys: List<ByteArray>): List<ByteArray> = emptyList()
            override fun list(): Sequence<Pair<ByteArray, ByteArray>> = emptySequence()
            override fun <K, V> list(mapper: (ByteArray, ByteArray) -> Pair<K, V>): Sequence<Pair<K, V>> = emptySequence()
            override fun isExist(key: ByteArray): Boolean = false
            override fun isExist2(key: ByteArray): Boolean = false
            override fun put(key: ByteArray, value: ByteArray) {}
            override fun delete(key: ByteArray) {}
            override fun clean() {}
            override fun column(cf: String?): IRawRockReaderWriter = this
            override fun columns(includeDefault: Boolean): List<IRawRockReaderWriter> = emptyList()
        }
    }
}
