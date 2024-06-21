package io.github.toyota32k.rock

import org.rocksdb.*
import java.util.Properties

@Suppress("unused")
class RockStadium private constructor(
    val db:RocksDB,
    val compactOnClose:Boolean,
    val handleMap:Map<String, ColumnFamilyHandle> = emptyMap()):AutoCloseable {
    enum class TransactionType {
        NONE,           // トランザクションなし
        OPTIMISTIC,     // 低い一貫性レベル(読み込み時の整合性のみ)
        FULL,           // 高い一貫性レベル(読み込み時の整合性と書き込み時の整合性)
    }
//    data class StadiumOptions(val compactOnClose:Boolean)
    class Builder {
        // 一般的な設定
        private var dbPath: String = "rocks.db"
        private var compactOnClose:Boolean = false
        private var destroyBeforehand:Boolean = false

        // ColumnFamilies を使わない場合のColumnFamily設定
        private var options: Options? = null
        private val ensureOptions get() = options ?: Options().apply {
            setCreateIfMissing(true)
            options = this
        }

        // ColumnFamilies を使う場合のDB設定
        private var dbOptions: DBOptions? = null
        private val ensureDbOptions get() = dbOptions ?: DBOptions().apply {
            setCreateIfMissing(true)
            setCreateMissingColumnFamilies(true)
            dbOptions = this
        }
        data class CFInfo(val name:String, val descriptor: ColumnFamilyDescriptor, val autoCompact:Boolean)
        private val columnFamilies = mutableListOf<CFInfo>()

        // Transactionを使うかどうか
        private var transactionType: TransactionType = TransactionType.NONE
        // Transactionを使う場合の設定
        private var transactionOptions: TransactionDBOptions? = null
        private val ensureTransactionOptions get() = transactionOptions ?: TransactionDBOptions().also { transactionOptions = it }

        fun dbOption(options:DBOptions): Builder {
            dbOptions = options
            return this
        }
        fun dbOption(properties:Properties): Builder {
            this.dbOptions = DBOptions.getDBOptionsFromProps(properties)
            return this
        }
        fun dbOption(fn:DBOptions.()->Unit): Builder {
            ensureDbOptions.fn()
            return this
        }

        fun option(options:Options): Builder {
            this.options = options
            return this
        }
        fun option(fn:Options.()->Unit): Builder {
            this.ensureOptions.fn()
            return this
        }

        fun transactionOption(options:TransactionDBOptions): Builder {
            this.transactionOptions = options
            return this
        }
        fun transactionOption(fn:TransactionDBOptions.()->Unit): Builder {
            this.ensureTransactionOptions.fn()
            return this
        }

        fun dbPath(path:String): Builder {
            this.dbPath = path
            return this
        }

        fun transaction(type:TransactionType): Builder {
            this.transactionType = type
            return this
        }
        fun compactOnClose(v:Boolean): Builder {
            this.compactOnClose = v
            return this
        }
        fun destroyBeforehand(v:Boolean): Builder {
            this.destroyBeforehand = v
            return this
        }

        class CFBuilder {
            private lateinit var name:String
            private var options:ColumnFamilyOptions? = null
            private val ensureOptions get() = options ?: ColumnFamilyOptions().also { options = it }
            private var autoCompact:Boolean = false

            fun name(name:String): CFBuilder {
                this.name = name
                return this
            }
            fun options(options:ColumnFamilyOptions): CFBuilder {
                this.options = options
                return this
            }
            fun options(properties:Properties): CFBuilder {
                this.options = ColumnFamilyOptions.getColumnFamilyOptionsFromProps(properties)
                return this
            }
            fun options(fn:ColumnFamilyOptions.()->Unit): CFBuilder {
                ensureOptions.fn()
                return this
            }
            fun autoCompact(v:Boolean): CFBuilder {
                autoCompact = v
                return this
            }
            fun build(): CFInfo {
                return CFInfo(name, ColumnFamilyDescriptor(name.toByteArray(), ensureOptions), autoCompact)
            }
        }

        fun column(cf:CFBuilder.()->Unit) {
            val cfBuilder = CFBuilder()
            cfBuilder.cf()
            columnFamilies.add(cfBuilder.build())
        }

        fun build():RockStadium {
            if(destroyBeforehand) {
                RocksDB.destroyDB(dbPath, Options())
            }
            return if(columnFamilies.isEmpty()) {
                val db = when(transactionType) {
                    TransactionType.NONE -> RocksDB.open(ensureOptions, dbPath)
                    TransactionType.OPTIMISTIC -> OptimisticTransactionDB.open(ensureOptions, dbPath)
                    TransactionType.FULL -> TransactionDB.open(ensureOptions, ensureTransactionOptions, dbPath)
                } ?: throw IllegalStateException("Failed to open RocksDB")
                RockStadium(db, compactOnClose)
            } else {
                if(columnFamilies.firstOrNull { it.name == "default" } == null) {
                    // ColumnFamily を利用する場合は、default カラムを自力で追加しておかないと open で例外が出る。へんな仕様。
                    columnFamilies.add(CFInfo("default", ColumnFamilyDescriptor("default".toByteArray(), ColumnFamilyOptions()), false))
                }
                val descriptors = columnFamilies.map { it.descriptor }
                val handles = mutableListOf<ColumnFamilyHandle>()
                val db = when(transactionType) {
                    TransactionType.NONE -> RocksDB.open(ensureDbOptions, dbPath, descriptors, handles)
                    TransactionType.OPTIMISTIC -> OptimisticTransactionDB.open(ensureDbOptions, dbPath, descriptors, handles)
                    TransactionType.FULL -> TransactionDB.open(ensureDbOptions, ensureTransactionOptions, dbPath, descriptors, handles)
                } ?: throw IllegalStateException("Failed to open RocksDB")
                val handlerMap = handles.fold(mutableMapOf<String, ColumnFamilyHandle>()) { acc, handle->
                    acc.apply { set(handle.name.toString(Charsets.UTF_8),handle) }
                }
                val autoCompacts = columnFamilies.mapNotNull { if(it.autoCompact) handlerMap[it.name] else null }
                if(autoCompacts.isNotEmpty()) {
                    db.enableAutoCompaction(autoCompacts)
                }
                RockStadium(db, compactOnClose, handlerMap)
            }
        }
    }

    companion object {
        init {
            // 最初にこれをやっておかないと、WriteOptions()などが例外をスローする
            RocksDB.loadLibrary()
        }

        val emptyReadOptions by lazy { ReadOptions() }
        val emptyWriteOptions by lazy { WriteOptions() }

        /**
         * スタジアムを建設する
         */
        fun constract(fn:Builder.()->Unit):RockStadium {
            val builder = Builder()
            fn(builder)
            return builder.build()
        }
    }

    fun put(cf:String, key:ByteArray, value:ByteArray, writeOption:WriteOptions) {
       db.put(handleMap[cf], writeOption, key, value)
    }
    fun put(key: ByteArray, value: ByteArray, writeOption:WriteOptions) {
        db.put(writeOption, key, value)
    }
    fun get(cf:String, key:ByteArray, readOptions: ReadOptions):ByteArray? {
        return db.get(handleMap[cf], readOptions, key)
    }
    fun get(key:ByteArray, readOptions: ReadOptions):ByteArray? {
        return db.get(readOptions, key)
    }
    fun gets(cf:String, keys:List<ByteArray>, readOptions: ReadOptions):List<ByteArray> {
        return db.multiGetAsList(readOptions, listOf(handleMap[cf]), keys)
    }
    fun gets(keys:List<ByteArray>, readOptions: ReadOptions):List<ByteArray> {
        return db.multiGetAsList(readOptions, keys)
    }
    fun gets(cfs:List<String>, keys:List<String>, readOptions: ReadOptions):List<ByteArray> {
        return db.multiGetAsList(readOptions, cfs.map { handleMap[it] }, keys.map { it.toByteArray() })
    }
    fun delete(key:ByteArray, writeOption: WriteOptions) {
        db.delete(writeOption, key)
    }
    fun delete(cf:String, key:ByteArray, writeOption:WriteOptions) {
        db.delete(handleMap[cf], writeOption, key)
    }
    fun clean(writeOption: WriteOptions) {
        // db.deleteRange(writeOption, byteArrayOf(), byteArrayOf())
        // Claude は、↑でイケると言ったが、やってみたら何も削除されなかった。
        db.newIterator(emptyReadOptions).use { itr->
            val batch = WriteBatch()
            itr.seekToFirst()
            while (itr.isValid) {
                batch.delete(itr.key())
                itr.next()
            }
            db.write(writeOption, batch)
        }
    }
    fun clean(cf:String, writeOption: WriteOptions) {
        clean(handleMap[cf]!!, writeOption)
    }
    private fun clean(hcf:ColumnFamilyHandle, writeOption: WriteOptions) {
        db.newIterator(hcf, emptyReadOptions).use { itr->
            val batch = WriteBatch()
            itr.seekToFirst()
            while (itr.isValid) {
                batch.delete(hcf, itr.key())
                itr.next()
            }
            db.write(writeOption, batch)
        }
    }
    fun isExist(key:ByteArray):Boolean {
        return db.keyExists(key)
    }
    fun isExist(cf:String, key: ByteArray):Boolean {
        return db.keyExists(handleMap[cf], key)
    }
    fun isExist2(key:ByteArray):Boolean {
        return db.keyMayExist(key, null) || db.keyExists(key)
    }
    fun isExist2(cf:String, key: ByteArray):Boolean {
        return db.keyMayExist(handleMap[cf], key, null) || db.keyExists(handleMap[cf], key)
    }

    private open inner class DefaultBatchWriter(val batch: WriteBatch = WriteBatch()): IRawRockWriter {
        override fun put(key: ByteArray, value: ByteArray) {
            batch.put(key, value)
        }
        override fun delete(key: ByteArray) {
            batch.delete(key)
        }

        override fun clean() {
            db.newIterator(emptyReadOptions).use { itr ->
                itr.seekToFirst()
                while (itr.isValid) {
                    batch.delete(itr.key())
                    itr.next()
                }
            }
        }
        override fun column(cf: String?): IRawRockWriter {
            return if(cf==null) {
                DefaultBatchWriter(batch)
            } else {
                CFBatchWriter(handleMap[cf]!!, batch)
            }
        }

        override fun columns(includeDefault: Boolean): List<IRawRockWriter> {
            return handleMap.map { CFBatchWriter(it.value, batch) }.apply {
                if(includeDefault) {
                    plus(DefaultBatchWriter(batch))
                }
            }
        }
    }

    private inner class CFBatchWriter(val hcf:ColumnFamilyHandle, batch: WriteBatch = WriteBatch()): DefaultBatchWriter(batch) {
        override fun put(key: ByteArray, value: ByteArray) {
            batch.put(hcf, key, value)
        }
        override fun delete(key: ByteArray) {
            batch.delete(hcf, key)
        }
        override fun clean() {
            db.newIterator(hcf, emptyReadOptions).use { itr ->
                itr.seekToFirst()
                while (itr.isValid) {
                    batch.delete(hcf, itr.key())
                    itr.next()
                }
            }
        }
    }

    fun atomicWrite(writeOption:WriteOptions= emptyWriteOptions, fn:IRawRockWriter.()->Boolean) : Boolean {
        val writer = DefaultBatchWriter()
        return if(writer.fn()) {
            db.write(writeOption, writer.batch)
            true
        } else false
    }

    fun atomicWrite(writeOption:WriteOptions= emptyWriteOptions, cf:String, fn:IRawRockWriter.()->Boolean):Boolean {
        val writer = CFBatchWriter(handleMap[cf]!!)
        return if(writer.fn()) {
            db.write(writeOption, writer.batch)
            true
        } else false
    }

    // region transaction

    /**
     * Transaction用 ReaderWriter の基底クラス
     */
    private abstract class IterableReaderBase : IRawRockReaderWriter {
        protected abstract fun createRockIterator(): RocksIterator

        override fun list(): Sequence<Pair<ByteArray, ByteArray>>
            = list { key, value -> key to value }

        override fun <K,V> list(mapper:(ByteArray,ByteArray)->Pair<K,V>): Sequence<Pair<K, V>> {
            return sequence {
                createRockIterator().use { itr->
                    itr.seekToFirst()
                    while (itr.isValid) {
                        val key = itr.key()
                        val value = itr.value()
                        itr.next()
                        yield(mapper(key, value))
                    }
                }
            }
        }
    }

    /**
     * Transaction + デフォルトカラム用 ReaderWriter
     */
    private open inner class DefaultTxnWriter(val txn:Transaction, val readOptions:ReadOptions): IterableReaderBase() {
        override fun put(key: ByteArray, value: ByteArray) {
            txn.put(key, value)
        }
        override fun delete(key: ByteArray) {
            txn.delete(key)
        }

        override fun clean() {
            list().forEach {
                txn.delete(it.first)
            }
        }

        override fun get(key: ByteArray): ByteArray? {
            return txn.get(readOptions, key)
        }

        override fun gets(keys: List<ByteArray>): List<ByteArray> {
            return txn.multiGetAsList(readOptions, keys)
        }

        override fun createRockIterator(): RocksIterator {
            return txn.getIterator(readOptions)
        }

        override fun column(cf: String?): IRawRockReaderWriter {
            return if(cf==null) {
                this
            } else {
                CFTxnWriter(txn, handleMap[cf]!!, readOptions)
            }
        }

        override fun columns(includeDefault:Boolean): List<IRawRockReaderWriter> {
            return if(!includeDefault) {
                handleMap.map { CFTxnWriter(txn, it.value, readOptions) }
            } else {
                listOf(DefaultTxnWriter(txn, readOptions)) + handleMap.map { CFTxnWriter(txn, it.value, readOptions) }
            }
        }

        // txnは keyExists/keyMayExist を持たない
        override fun isExist(key: ByteArray): Boolean {
            throw UnsupportedOperationException("Transaction does not support isExist")
        }

        override fun isExist2(key: ByteArray): Boolean {
            throw UnsupportedOperationException("Transaction does not support isExist2")
        }
    }

    /**
     * Transaction + ColumnFamily用 ReaderWriter
     */
    private inner class CFTxnWriter(txn:Transaction, val hcf:ColumnFamilyHandle, readOptions:ReadOptions): DefaultTxnWriter(txn, readOptions) {
        override fun put(key: ByteArray, value: ByteArray) {
            txn.put(hcf, key, value)
        }

        override fun delete(key: ByteArray) {
            txn.delete(hcf, key)
        }

        override fun clean() {
            list().forEach {
                txn.delete(hcf, it.first)
            }
        }

        override fun get(key: ByteArray): ByteArray? {
            return txn.get(readOptions, hcf, key)
        }

        override fun gets(keys: List<ByteArray>): List<ByteArray> {
            return txn.multiGetAsList(readOptions, listOf(hcf), keys)
        }

        override fun createRockIterator(): RocksIterator {
            return txn.getIterator(readOptions, hcf)
        }
        override fun column(cf: String?): IRawRockReaderWriter {
            return if(cf==null) {
                DefaultTxnWriter(txn, readOptions)
            } else if(hcf == handleMap[cf]) {
                this
            } else {
                CFTxnWriter(txn, handleMap[cf]!!, readOptions)
            }
        }
    }

    /**
     * デフォルトカラムに対するトランザクションを実行する
     * 起点がデフォルトカラムというだけで、column()/columns() で、すべてのColumnFamilyにアクセスできる
     */
    fun transaction(writeOption:WriteOptions= emptyWriteOptions, readOptions:ReadOptions= emptyReadOptions, fn:IRawRockReaderWriter.()->Boolean):Boolean {
        val txn = when(db) {
            is TransactionDB -> db.beginTransaction(writeOption)
            is OptimisticTransactionDB -> db.beginTransaction(writeOption)
            else -> throw IllegalStateException("Transaction is not supported")
        }
        return if(DefaultTxnWriter(txn, readOptions).fn()) {
            txn.commit()
            true
        } else {
            txn.rollback()
            false
        }
    }

    /**
     * 特定のColumnFamilyに対するトランザクションを実行する
     * 起点を指定するだけで、column()/columns() で、すべてのColumnFamilyにアクセスできる
     */
    fun transaction(cf:String, writeOption:WriteOptions= emptyWriteOptions, readOptions:ReadOptions= emptyReadOptions, fn:IRawRockReaderWriter.()->Boolean):Boolean {
        val txn = when(db) {
            is TransactionDB -> db.beginTransaction(writeOption)
            is OptimisticTransactionDB -> db.beginTransaction(writeOption)
            else -> throw IllegalStateException("Transaction is not supported")
        }
        return if(CFTxnWriter(txn, handleMap[cf]!!, readOptions).fn()) {
            txn.commit()
            true
        } else {
            txn.rollback()
            false
        }
    }
    // endregion

    // region 通常アクセス

    /**
     * デフォルトカラム用 ReaderWriter
     */
    private open inner class DefaultReaderWriter(val writeOption:WriteOptions, val readOptions:ReadOptions) : IterableReaderBase(), IRawRockReaderWriter {
        override fun put(key: ByteArray, value: ByteArray) {
            put(key, value, writeOption)
        }

        override fun delete(key: ByteArray) {
            delete(key, writeOption)
        }

        override fun clean() {
            clean(writeOption)
        }

        override fun get(key: ByteArray): ByteArray? {
            return get(key, readOptions)
        }

        override fun gets(keys: List<ByteArray>): List<ByteArray> {
            return gets(keys, readOptions)
        }

        override fun createRockIterator(): RocksIterator {
            return db.newIterator(readOptions)
        }

        override fun column(cf: String?): IRawRockReaderWriter {
            return if(cf==null) {
                this
            } else {
                CFReaderWriter(handleMap[cf]!!, writeOption, readOptions)
            }
        }

        override fun columns(includeDefault: Boolean): List<IRawRockReaderWriter> {
            return handleMap.map { CFReaderWriter(it.value, writeOption, readOptions) }.apply {
                if(includeDefault) {
                    plus(DefaultReaderWriter(writeOption, readOptions))
                }
            }
        }

        override fun isExist(key: ByteArray): Boolean {
            return this@RockStadium.isExist(key)
        }

        override fun isExist2(key: ByteArray): Boolean {
            return this@RockStadium.isExist2(key)
        }
    }

    /**
     * ColumnFamily用 ReaderWriter
     */
    private inner class CFReaderWriter(val hcf:ColumnFamilyHandle, writeOption:WriteOptions, readOptions:ReadOptions) : DefaultReaderWriter(writeOption, readOptions) {
        override fun put(key: ByteArray, value: ByteArray) {
            db.put(hcf, writeOption, key, value)
        }

        override fun delete(key: ByteArray) {
            db.delete(hcf, writeOption, key)
        }

        override fun clean() {
            clean(hcf, writeOption)
        }

        override fun get(key: ByteArray): ByteArray? {
            return db.get(hcf, readOptions, key)
        }

        override fun gets(keys: List<ByteArray>): List<ByteArray> {
            return db.multiGetAsList(readOptions, listOf(hcf), keys)
        }

        override fun createRockIterator(): RocksIterator {
            return db.newIterator(hcf, readOptions)
        }

        override fun isExist(key: ByteArray): Boolean {
            return db.keyExists(hcf, key)
        }
        override fun isExist2(key: ByteArray): Boolean {
            return db.keyMayExist(hcf,key,null) || db.keyExists(hcf, key)
        }

        override fun column(cf: String?): IRawRockReaderWriter {
            return if(cf==null) {
                DefaultReaderWriter(writeOption,readOptions)
            } else if(hcf == handleMap[cf]) {
                this
            } else {
                CFReaderWriter(handleMap[cf]!!, writeOption, readOptions)
            }
        }

    }

//    fun readWrite(writeOption:WriteOptions, readOptions:ReadOptions, fn:(IRawRockReaderWriter)->Unit) {
//        fn(DefaultReaderWriter(writeOption, readOptions))
//    }

    /**
     * デフォルトカラム用 ReaderWriter を取得する
     */
    fun getReaderWriter(writeOption:WriteOptions= emptyWriteOptions, readOptions:ReadOptions= emptyReadOptions): IRawRockReaderWriter {
        return DefaultReaderWriter(writeOption, readOptions)
    }

    /**
     * ColumnFamily用 ReaderWriter を取得する
     */
    fun getReaderWriter(cf:String, writeOption:WriteOptions= emptyWriteOptions, readOptions:ReadOptions= emptyReadOptions): IRawRockReaderWriter {
        return CFReaderWriter(handleMap[cf]!!, writeOption, readOptions)
    }

    fun compact() {
        db.compactRange()
    }

    override fun close() {
        db.close()
    }
}