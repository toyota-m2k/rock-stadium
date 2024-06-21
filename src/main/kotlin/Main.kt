package io.github.toyota32k

import io.github.toyota32k.rock.RockStadium
import io.github.toyota32k.rock.toReaderWriter
import io.github.toyota32k.rock.withReaderWriter
import io.github.toyota32k.rock.withWriter
import org.rocksdb.Options
import org.rocksdb.RocksDB
import org.rocksdb.WriteOptions
import java.io.File
import java.sql.Connection
import java.sql.DriverManager
import kotlin.random.Random
import kotlin.system.measureTimeMillis

data class Result(
    val creationTime: Long,
    val writeTime: Long,
    val readTime: Long,
    val updateTime: Long,
    val deleteTime: Long,
    val closeTime: Long,
){
    val total:Long get() =  creationTime + writeTime + readTime + updateTime + deleteTime + closeTime
}

data class TestEntry(val target:String, val options:String) {
    val results = mutableListOf<Result>()
    val avg: Result get() {
        val count = results.size
        return Result(
            results.map { it.creationTime }.sum()/count,
            results.map { it.writeTime }.sum()/count,
            results.map { it.readTime }.sum()/count,
            results.map { it.updateTime }.sum()/count,
            results.map { it.deleteTime }.sum()/count,
            results.map { it.closeTime }.sum()/count,
        )
    }
}

fun main() {
    val repeat = 3
    val numberOfEntries = 10000
    val keys = generateRandomStrings(numberOfEntries, 20)
    val values = generateRandomStrings(numberOfEntries, 1000)
    val entries = listOf(
        TestEntry("SQLite", "Journal"),
        TestEntry("SQLite", "WAL"),
        TestEntry("SQLite", "Transaction Journal"),
        TestEntry("SQLite", "Transaction WAL"),
        TestEntry("RocksDB", "Raw"),
        TestEntry("RocksDB", "Stadium None"),
        TestEntry("RocksDB", "Stadium Transaction"),
        TestEntry("RocksDB", "Stadium Batch"),
    )
    for(i in 0 until 3) {
        entries[0].results.add(measureSqlite(keys, values, false, false))
        entries[1].results.add(measureSqlite(keys, values, false, true))
        entries[2].results.add(measureSqlite(keys, values, true, false))
        entries[3].results.add(measureSqlite(keys, values, true, true))
        entries[4].results.add(measureRocksDB(keys, values))
        entries[5].results.add(measureRocksDBonStadium(keys, values, RMode.NONE))
        entries[6].results.add(measureRocksDBonStadium(keys, values, RMode.TRANSACTION))
        entries[7].results.add(measureRocksDBonStadium(keys, values, RMode.BATCH))
    }
    for(entry in entries) {
        println("${entry.target} ${entry.options}")
        for(i in 0 until 3) {
            println("$i : ${entry.results[i].creationTime}\t${entry.results[i].writeTime}\t${entry.results[i].readTime}\t${entry.results[i].updateTime}\t${entry.results[i].deleteTime}\t${entry.results[i].closeTime}\t${entry.results[i].total}")
        }
        val avg = entry.avg
        println("av: ${avg.creationTime}\t${avg.writeTime}\t${avg.readTime}\t${avg.updateTime}\t${avg.deleteTime}\t${avg.closeTime}\t${avg.total}")
    }

    // 後始末
    safeDeleteFile("test.sqlite")
    RocksDB.destroyDB("rocksdb", Options())
    RocksDB.destroyDB("stadium.db", Options())
}

fun safeDeleteFile(path:String) {
    try {
        File(path).delete()
    } catch (_:Exception) {}
}

fun measureSqlite(keys:List<String>, values:List<String>, useSqliteTransaction:Boolean, useSqliteWAL:Boolean) : Result {
    println("SQLite: ${if(useSqliteTransaction) "Transaction" else "AutoCommit"} ${if(useSqliteWAL) "WAL" else "Journal"}")
    // データベース接続
    val url = "jdbc:sqlite:test.sqlite"
    safeDeleteFile("test.sqlite")

    var conn: Connection
    val sqliteCreationTime = measureTimeMillis {
        conn = DriverManager.getConnection(url).apply {
            createStatement().use { statement ->
                statement.executeUpdate("CREATE TABLE IF NOT EXISTS test_table (key TEXT PRIMARY KEY, value TEXT)")
            }
            if(useSqliteWAL) {
                createStatement().use { statement ->
                    statement.executeUpdate("PRAGMA journal_mode=WAL")
                    statement.executeUpdate("PRAGMA synchronous=NORMAL")    // WALの場合は、synchronousをNORMALにすると、かなり速くなる
                }
            }
        }
    }

    if (useSqliteTransaction) {
        conn.autoCommit = false
    }

    // データ書き込み
    val sqliteWriteTime = measureTimeMillis {
        conn.prepareStatement("INSERT INTO test_table (key, value) VALUES (?, ?)").use { preparedStatement ->
            for (i in keys.indices) {
                preparedStatement.setString(1, keys[i])
                preparedStatement.setString(2, values[i])
                preparedStatement.executeUpdate()
            }
        }
        if (useSqliteTransaction) {
            conn.commit()
        }
    }

    // データ読み出し
    val sqliteReadTime = measureTimeMillis {
        conn.prepareStatement("SELECT * FROM test_table").use { preparedStatement ->
            preparedStatement.executeQuery().use { resultSet ->
                while (resultSet.next()) {
                    val key = resultSet.getString("key")
                    val value = resultSet.getString("value")
                    // ここで読み出したデータを使用する
                }
            }
        }
    }

    val sqliteUpdateTime = measureTimeMillis {
        conn.prepareStatement("UPDATE test_table SET value = ? WHERE key = ?").use { preparedStatement ->
            for (i in keys.indices) {
                preparedStatement.setString(1, values[i])
                preparedStatement.setString(2, keys[i])
                preparedStatement.executeUpdate()
            }
        }
        if(useSqliteTransaction) {
            conn.commit()
        }
    }

    val sqliteDeleteTime = measureTimeMillis {
        conn.prepareStatement("DELETE FROM test_table WHERE key = ?").use { preparedStatement ->
            for (i in keys.indices) {
                preparedStatement.setString(1, keys[i])
                preparedStatement.executeUpdate()
            }
        }
        if(useSqliteTransaction) {
            conn.commit()
        }
    }

    val sqliteCloseTime = measureTimeMillis {
        conn.close()
    }
    println("SQLite作成時間: $sqliteCreationTime ms")
    println("SQLite書き込み時間: $sqliteWriteTime ms")
    println("SQLite読み出し時間: $sqliteReadTime ms")
    println("SQLite更新時間: $sqliteUpdateTime ms")
    println("SQLite削除時間: $sqliteDeleteTime ms")
    println("SQLiteクローズ時間: $sqliteCloseTime ms")
    return Result(sqliteCreationTime, sqliteWriteTime, sqliteReadTime, sqliteUpdateTime, sqliteDeleteTime, sqliteCloseTime)
}

fun measureRocksDB(keys:List<String>, values:List<String>):Result {
    println("RocksDB: Raw")
    RocksDB.loadLibrary()
    val writeOptions = WriteOptions()
    val dbPath = "rocksdb"
    var db:RocksDB
    RocksDB.destroyDB(dbPath, Options())
    val rocksCreationTime = measureTimeMillis {
        db = RocksDB.open(dbPath)
    }

    val rocksWriteTime = measureTimeMillis {
        for (i in keys.indices) {
            db.put(keys[i].toByteArray(), values[i].toByteArray())
        }
    }

    val rocksReadTime = measureTimeMillis {
        db.newIterator().use { iterator ->
            iterator.seekToFirst()
            while (iterator.isValid) {
                val key = String(iterator.key())
                val value = String(iterator.value())
                // ここで読み出したデータを使用する
                iterator.next()
            }
        }
    }
    val rocksUpdateTime = measureTimeMillis {
        for (i in keys.indices) {
            db.put(keys[i].toByteArray(), values[i].toByteArray())
        }
    }
    val rocksDeleteTime = measureTimeMillis {
        for (i in keys.indices) {
            db.delete(keys[i].toByteArray())
        }
    }

    val rocksCloseTime = measureTimeMillis {
        db.close()
    }

    println("RocksDB作成時間: $rocksCreationTime ms")
    println("RocksDB書き込み時間: $rocksWriteTime ms")
    println("RocksDB読み出し時間: $rocksReadTime ms")
    println("RocksDB更新時間: $rocksUpdateTime ms")
    println("RocksDB削除時間: $rocksDeleteTime ms")
    println("RocksDBクローズ時間: $rocksCloseTime ms")
    return Result(rocksCreationTime, rocksWriteTime, rocksReadTime, rocksUpdateTime, rocksDeleteTime, rocksCloseTime)
}

enum class RMode {
    NONE, TRANSACTION, BATCH,
}
fun measureRocksDBonStadium(keys:List<String>, values:List<String>, mode:RMode):Result {
    println("RocksDB: Stadium ${mode.name}")
    RocksDB.destroyDB("stadium.db", Options())
    var stadium: RockStadium
    val rocksCreationTime = measureTimeMillis {
        stadium = RockStadium.constract {
            dbPath("stadium.db")
            destroyBeforehand(true)
            if(mode==RMode.TRANSACTION) {
                transaction(RockStadium.TransactionType.FULL)
            }
        }
    }
    var rocksWriteTime = 0L
    var rocksReadTime = 0L
    var rocksUpdateTime = 0L
    var rocksDeleteTime = 0L

    when(mode) {
        RMode.NONE -> {
            rocksWriteTime = measureTimeMillis {
                stadium.getReaderWriter().withReaderWriter {
                    for (i in keys.indices) {
                        put(keys[i], values[i])
                    }
                }
            }
            rocksReadTime = measureTimeMillis {
                stadium.getReaderWriter().withReaderWriter {
                    for (i in keys.indices) {
                        getString(keys[i])
                    }
                }
            }
            rocksUpdateTime = measureTimeMillis {
                stadium.getReaderWriter().withReaderWriter {
                    for (i in keys.indices) {
                        put(keys[i], values[i])
                    }
                }
            }
            rocksDeleteTime = measureTimeMillis {
                stadium.getReaderWriter().withReaderWriter {
                    for (i in keys.indices) {
                        delete(keys[i])
                    }
                }
            }
        }
        RMode.TRANSACTION -> {
            rocksWriteTime = measureTimeMillis {
                stadium.transaction {
                    withReaderWriter {
                        for (i in keys.indices) {
                            put(keys[i], values[i])
                        }
                        true
                    }
                }
            }
            rocksReadTime = measureTimeMillis {
                stadium.transaction {
                    withReaderWriter {
                        for (i in keys.indices) {
                            getString(keys[i])
                        }
                        false
                    }
                }
            }
            rocksUpdateTime = measureTimeMillis {
                stadium.transaction {
                    withReaderWriter {
                        for (i in keys.indices) {
                            put(keys[i], values[i])
                        }
                        true
                    }
                }
            }
            rocksDeleteTime = measureTimeMillis {
                stadium.transaction {
                    withReaderWriter {
                        for (i in keys.indices) {
                            delete(keys[i])
                        }
                        true
                    }
                }
            }
        }
        RMode.BATCH -> {
            rocksWriteTime = measureTimeMillis {
                stadium.atomicWrite {
                    withWriter {
                        for (i in keys.indices) {
                            put(keys[i], values[i])
                        }
                        true
                    }
                }
            }
            rocksReadTime = measureTimeMillis {
                stadium.getReaderWriter().withReaderWriter {
                    for (i in keys.indices) {
                        getString(keys[i])
                    }
                }
            }
            rocksUpdateTime = measureTimeMillis {
                stadium.atomicWrite {
                    withWriter {
                        for (i in keys.indices) {
                            put(keys[i], values[i])
                        }
                        true
                    }
                }
            }
            rocksDeleteTime = measureTimeMillis {
                stadium.atomicWrite {
                    withWriter {
                        for (i in keys.indices) {
                            delete(keys[i])
                        }
                        true
                    }
                }
            }
        }
    }

    val rocksCloseTime = measureTimeMillis {
        stadium.close()
    }

    println("RocksDB作成時間: $rocksCreationTime ms")
    println("RocksDB書き込み時間: $rocksWriteTime ms")
    println("RocksDB読み出し時間: $rocksReadTime ms")
    println("RocksDB更新時間: $rocksUpdateTime ms")
    println("RocksDB削除時間: $rocksDeleteTime ms")
    println("RocksDBクローズ時間: $rocksCloseTime ms")

    return Result(rocksCreationTime, rocksWriteTime, rocksReadTime, rocksUpdateTime, rocksDeleteTime, rocksCloseTime)
}



//fun measureRocksDBOpenClose(keys:List<String>, values:List<String>) {
//    RocksDB.loadLibrary()
//    val dbPath = "test_rocksdb"
//    val writeOptions = WriteOptions()
//
//    val rocksWriteTime = measureTimeMillis {
//        for (i in keys.indices) {
//            RocksDB.open(dbPath).use { db ->
//                db.put(keys[i].toByteArray(), values[i].toByteArray())
//            }
//        }
//    }
//
//    val rocksReadTime = measureTimeMillis {
//        for(i in keys.indices) {
//            RocksDB.open(dbPath).use { db ->
//                val value = db.get(keys[i].toByteArray())
//            }
//        }
//    }
//    val rocksUpdateTime = measureTimeMillis {
//        for (i in keys.indices) {
//            RocksDB.open(dbPath).use { db ->
//                db.put(keys[i].toByteArray(), values[i].toByteArray())
//            }
//        }
//    }
//    val rocksDeleteTime = measureTimeMillis {
//        for (i in keys.indices) {
//            RocksDB.open(dbPath).use { db ->
//                db.delete(keys[i].toByteArray())
//            }
//        }
//    }
//
//    println("RocksDB書き込み時間: $rocksWriteTime ms")
//    println("RocksDB読み出し時間: $rocksReadTime ms")
//    println("RocksDB更新時間: $rocksUpdateTime ms")
//    println("RocksDB削除時間: $rocksDeleteTime ms")
//}
//
fun generateRandomStrings(count: Int, length: Int): List<String> {
    val random = Random.Default
    return List(count) {
        random.nextString(length)
    }
}

fun Random.nextString(length: Int): String {
    val allowedChars = ('A'..'Z') + ('a'..'z') + ('0'..'9')
    return (1..length)
        .map { allowedChars.random(this) }
        .joinToString("")
}