import io.github.toyota32k.rock.*
import io.kotest.core.spec.style.DescribeSpec
import io.kotest.matchers.shouldBe

class RockTest : DescribeSpec( {
    describe("Single Column") {
        it("Put/Get/Delete") {
            RockStadium.constract {
                dbPath("single.db")
            }.use { stadium ->
                stadium.getReaderWriter().toReaderWriter().apply {
                    put("key1", "value1")
                    put("key2", 2)
                    put("key3", 3L)
                    put("key4", 4.0)
                    put("key5", 5.0f)
                    put("key6", true)
                    put("key7", byteArrayOf(1, 2, 3))

                    getString("key1") shouldBe "value1"
                    getInt("key2") shouldBe 2
                    getLong("key3") shouldBe 3L
                    getDouble("key4") shouldBe 4.0
                    getFloat("key5") shouldBe 5.0f
                    getBoolean("key6") shouldBe true
                    getBlob("key7") shouldBe byteArrayOf(1, 2, 3)

                    put("key1", "value11")
                    put("key2", 22)
                    put("key3", 33L)
                    put("key4", 4.4)
                    put("key5", 5.5f)
                    put("key6", false)
                    put("key7", byteArrayOf(4, 5, 6))

                    getString("key1") shouldBe "value11"
                    getInt("key2") shouldBe 22
                    getLong("key3") shouldBe 33L
                    getDouble("key4") shouldBe 4.4
                    getFloat("key5") shouldBe 5.5f
                    getBoolean("key6") shouldBe false
                    getBlob("key7") shouldBe byteArrayOf(4, 5, 6)

                    isExist2("key1") shouldBe true
                    isExist("key1") shouldBe true

                    delete("key1")
                    isExist2("key1") shouldBe false
                    isExist("key1") shouldBe false
                    isExist2("key2") shouldBe true
                    isExist("key2") shouldBe true
                    delete("key2")

                    isExist2("key2") shouldBe false
                    isExist("key2") shouldBe false
                    isExist2("key3") shouldBe true
                    isExist("key3") shouldBe true
                    delete("key3")

                    isExist2("key3") shouldBe false
                    isExist("key3") shouldBe false
                    isExist2("key4") shouldBe true
                    isExist("key4") shouldBe true
                    delete("key4")

                    isExist2("key4") shouldBe false
                    isExist("key4") shouldBe false
                    isExist2("key5") shouldBe true
                    isExist("key5") shouldBe true
                    delete("key5")

                    isExist2("key5") shouldBe false
                    isExist("key5") shouldBe false
                    isExist2("key6") shouldBe true
                    isExist("key6") shouldBe true
                    delete("key6")

                    isExist2("key6") shouldBe false
                    isExist("key6") shouldBe false
                    isExist2("key7") shouldBe true
                    isExist("key7") shouldBe true
                    delete("key7")

                    isExist2("key7") shouldBe false
                    isExist("key7") shouldBe false
                }
            }
        }
        it("Iterator") {
            RockStadium.constract {
                dbPath("single.db")
            }.use { stadium ->
                stadium.getReaderWriter().toReaderWriter().apply {
                    clean()
                    val source = listOf(
                        "key1" to "value1",
                        "key2" to "value2",
                        "key3" to "value3",
                        "key4" to "value4",
                        "key5" to "value5",
                        "key6" to "value6",
                    )
                    source.forEach { (k, v) ->
                        put(k, v)
                    }
                    val list = listAsString().toList()
                    list.size shouldBe source.size
                    list shouldBe source
                    clean()
                }
            }
        }
        it("Atomic Write") {
            RockStadium.constract {
                dbPath("single.db")
            }.use { stadium ->
                val source = listOf(
                    "key1" to "value1",
                    "key2" to "value2",
                    "key3" to "value3",
                    "key4" to "value4",
                    "key5" to "value5",
                    "key6" to "value6",
                )
                stadium.clean(RockStadium.emptyWriteOptions)
                stadium.atomicWrite {
                    withWriter {
                        source.forEach { (k, v) ->
                            put(k, v)
                        }
                    }
                    true
                }
                stadium.getReaderWriter().toReaderWriter().apply {
                    val list = listAsString().toList()
                    list.size shouldBe source.size
                    list shouldBe source
                    clean()
                }
            }
        }
        it("Transaction (FULL)") {
            RockStadium.constract {
                dbPath("single.db")
                transaction(RockStadium.TransactionType.FULL)
            }.use { stadium ->
                val source = listOf(
                    "key1" to "value1",
                    "key2" to "value2",
                    "key3" to "value3",
                    "key4" to "value4",
                    "key5" to "value5",
                    "key6" to "value6",
                )
                stadium.transaction {
                    withReaderWriter {
                        clean()
                        source.forEach { (k, v) ->
                            put(k, v)
                        }
                    }
                    true
                }
                stadium.getReaderWriter().toReaderWriter().apply {
                    val list = listAsString().toList()
                    list.size shouldBe source.size
                    list shouldBe source
                    clean()
                }
            }
        }
        it("Transaction (OPTIMISTIC)") {
            RockStadium.constract {
                dbPath("single.db")
                transaction(RockStadium.TransactionType.OPTIMISTIC)
            }.use { stadium ->
                val source = listOf(
                    "key1" to "value1",
                    "key2" to "value2",
                    "key3" to "value3",
                    "key4" to "value4",
                    "key5" to "value5",
                    "key6" to "value6",
                )
                stadium.transaction {
                    withReaderWriter {
                        clean()
                        source.forEach { (k, v) ->
                            put(k, v)
                        }
                    }
                    true
                }
                stadium.getReaderWriter().toReaderWriter().apply {
                    val list = listAsString().toList()
                    list.size shouldBe source.size
                    list shouldBe source
                    clean()
                }
            }
        }
    }
    describe("Multi Column") {
        it("PUT/GET/DELETE") {
            RockStadium.constract {
                dbPath("multi.db")
                column { name("cf1") }
                column { name("cf2") }
                column { name("cf3") }
            }.use { stadium ->
                stadium.getReaderWriter().toReaderWriter().apply {
                    clean()
                    column("cf1").clean()
                    columns().forEach { it.clean() }

                    put("key1", "value1")
                    getString("key1") shouldBe "value1"
                    isExist("key1") shouldBe true
                    isExist2("key2") shouldBe false
                    stadium.db.get("key1".toByteArray()) shouldBe "value1".toByteArray()
                    // ColumnFamily サポート用のAPIでは、ちゃんと "default" 用のHandleを渡さないと、nullとかemptyArrayとかではアクセスできない模様
//                stadium.db.get(byteArrayOf(), "key1".toByteArray()) shouldBe "value1".toByteArray()
//                stadium.db.get(null, "key1".toByteArray()) shouldBe "value1".toByteArray()

                    column("cf1").apply {
                        isExist("key1") shouldBe false
                        isExist("key2") shouldBe false
                        isExist("key3") shouldBe false
                        isExist("key4") shouldBe false
                        put("key1", "value-cf1-1")
                        put("key2", "value-cf1-2")
                        put("key3", "value-cf1-3")
                        put("key4", "value-cf1-4")
                        getString("key1") shouldBe "value-cf1-1"
                        getString("key2") shouldBe "value-cf1-2"
                        getString("key3") shouldBe "value-cf1-3"
                        getString("key4") shouldBe "value-cf1-4"
                        isExist("key1") shouldBe true
                        isExist("key2") shouldBe true
                        isExist("key3") shouldBe true
                        isExist("key4") shouldBe true
                        isExist2("key1") shouldBe true
                        isExist2("key2") shouldBe true
                        isExist2("key3") shouldBe true
                        isExist2("key4") shouldBe true
                        isExist("key5") shouldBe false
                        isExist2("key5") shouldBe false
                    }
                    column("cf2").apply {
                        isExist("key1") shouldBe false
                        isExist("key2") shouldBe false
                        isExist("key3") shouldBe false
                        isExist("key4") shouldBe false
                        put("key1", "value-cf2-1")
                        put("key2", "value-cf2-2")
                        put("key3", "value-cf2-3")
                        put("key4", "value-cf2-4")
                        getString("key1") shouldBe "value-cf2-1"
                        getString("key2") shouldBe "value-cf2-2"
                        getString("key3") shouldBe "value-cf2-3"
                        getString("key4") shouldBe "value-cf2-4"
                        isExist("key1") shouldBe true
                        isExist("key2") shouldBe true
                        isExist("key3") shouldBe true
                        isExist("key4") shouldBe true
                        isExist2("key1") shouldBe true
                        isExist2("key2") shouldBe true
                        isExist2("key3") shouldBe true
                        isExist2("key4") shouldBe true
                        isExist("key5") shouldBe false
                        isExist2("key5") shouldBe false
                    }
                    column("cf3").apply {
                        isExist("key1") shouldBe false
                        isExist("key2") shouldBe false
                        isExist("key3") shouldBe false
                        isExist("key4") shouldBe false
                        put("key1", "value-cf3-1")
                        put("key2", "value-cf3-2")
                        put("key3", "value-cf3-3")
                        put("key4", "value-cf3-4")
                        getString("key1") shouldBe "value-cf3-1"
                        getString("key2") shouldBe "value-cf3-2"
                        getString("key3") shouldBe "value-cf3-3"
                        getString("key4") shouldBe "value-cf3-4"
                        isExist("key1") shouldBe true
                        isExist("key2") shouldBe true
                        isExist("key3") shouldBe true
                        isExist("key4") shouldBe true
                        isExist2("key1") shouldBe true
                        isExist2("key2") shouldBe true
                        isExist2("key3") shouldBe true
                        isExist2("key4") shouldBe true
                        isExist("key5") shouldBe false
                        isExist2("key5") shouldBe false
                    }
                }
                stadium.getReaderWriter("cf1").toReaderWriter().apply {
                    getString("key1") shouldBe "value-cf1-1"
                    getString("key2") shouldBe "value-cf1-2"
                    getString("key3") shouldBe "value-cf1-3"
                    getString("key4") shouldBe "value-cf1-4"

                    listAsString().toList() shouldBe listOf(
                        "key1" to "value-cf1-1",
                        "key2" to "value-cf1-2",
                        "key3" to "value-cf1-3",
                        "key4" to "value-cf1-4",
                    )

                    delete("key1")
                    isExist("key1") shouldBe false
                    isExist("key2") shouldBe true
                    isExist("key3") shouldBe true
                    isExist("key4") shouldBe true

                    delete("key2")
                    isExist("key1") shouldBe false
                    isExist("key2") shouldBe false
                    isExist("key3") shouldBe true
                    isExist("key4") shouldBe true

                    clean()
                    isExist("key1") shouldBe false
                    isExist("key2") shouldBe false
                    isExist("key3") shouldBe false
                    isExist("key4") shouldBe false

                }
                stadium.getReaderWriter("cf2").toReaderWriter().apply {
                    getString("key1") shouldBe "value-cf2-1"
                    getString("key2") shouldBe "value-cf2-2"
                    getString("key3") shouldBe "value-cf2-3"
                    getString("key4") shouldBe "value-cf2-4"

                    listAsString().toList() shouldBe listOf(
                        "key1" to "value-cf2-1",
                        "key2" to "value-cf2-2",
                        "key3" to "value-cf2-3",
                        "key4" to "value-cf2-4",
                    )

                    delete("key1")
                    isExist("key1") shouldBe false
                    isExist("key2") shouldBe true
                    isExist("key3") shouldBe true
                    isExist("key4") shouldBe true

                    delete("key2")
                    isExist("key1") shouldBe false
                    isExist("key2") shouldBe false
                    isExist("key3") shouldBe true
                    isExist("key4") shouldBe true

                    clean()
                    isExist("key1") shouldBe false
                    isExist("key2") shouldBe false
                    isExist("key3") shouldBe false
                    isExist("key4") shouldBe false
                }
                stadium.getReaderWriter("cf3").toReaderWriter().apply {
                    getString("key1") shouldBe "value-cf3-1"
                    getString("key2") shouldBe "value-cf3-2"
                    getString("key3") shouldBe "value-cf3-3"
                    getString("key4") shouldBe "value-cf3-4"

                    listAsString().toList() shouldBe listOf(
                        "key1" to "value-cf3-1",
                        "key2" to "value-cf3-2",
                        "key3" to "value-cf3-3",
                        "key4" to "value-cf3-4",
                    )

                    delete("key1")
                    isExist("key1") shouldBe false
                    isExist("key2") shouldBe true
                    isExist("key3") shouldBe true
                    isExist("key4") shouldBe true

                    delete("key2")
                    isExist("key1") shouldBe false
                    isExist("key2") shouldBe false
                    isExist("key3") shouldBe true
                    isExist("key4") shouldBe true

                    clean()
                    isExist("key1") shouldBe false
                    isExist("key2") shouldBe false
                    isExist("key3") shouldBe false
                    isExist("key4") shouldBe false
                }
                stadium.atomicWrite {
                    withWriter {
                        column("cf1").put("key1", "value2-cf1-1")
                        column("cf2").put("key1", "value2-cf2-1")
                        column("cf3").put("key1", "value2-cf3-1")
                    }
                    true
                }
                stadium.getReaderWriter("cf1").withReaderWriter {
                    getString("key1") shouldBe "value2-cf1-1"
                }
                stadium.getReaderWriter("cf2").withReaderWriter {
                    getString("key1") shouldBe "value2-cf2-1"
                }
                stadium.getReaderWriter("cf3").withReaderWriter {
                    getString("key1") shouldBe "value2-cf3-1"
                }
                stadium.atomicWrite {
                    withWriter {
                        columns().forEach { it.clean() }
                    }
                    true
                }
                stadium.getReaderWriter("cf1").withReaderWriter {
                    getString("key1") shouldBe null
                    isExist("key1") shouldBe false
                }
                stadium.getReaderWriter("cf2").withReaderWriter {
                    getString("key1") shouldBe null
                    isExist("key1") shouldBe false
                }
                stadium.getReaderWriter("cf3").withReaderWriter {
                    getString("key1") shouldBe null
                    isExist("key1") shouldBe false
                }
            }
        }
        it("Atomic Write") {
            RockStadium.constract {
                dbPath("multi.db")
                column { name("cf1") }
                column { name("cf2") }
                column { name("cf3") }
            }.use { stadium ->
                stadium.getReaderWriter().withReaderWriter {
                    columns(true).forEach { it.clean() }
                }
                stadium.atomicWrite {
                    withWriter {
                        put("key1", "value1")
                        put("key2", "value2")
                        put("key3", "value3")
                        put("key4", "value4")

                        column("cf1").apply {
                            put("key1", "value1-cf1")
                            put("key2", "value2-cf1")
                            put("key3", "value3-cf1")
                            put("key4", "value4-cf1")
                        }
                        column("cf2").apply {
                            put("key1", "value1-cf2")
                            put("key2", "value2-cf2")
                            put("key3", "value3-cf2")
                            put("key4", "value4-cf2")
                        }
                        column("cf3").apply {
                            put("key1", "value1-cf3")
                            put("key2", "value2-cf3")
                            put("key3", "value3-cf3")
                            put("key4", "value4-cf3")
                        }
                    }
                    true
                }
                stadium.getReaderWriter().withReaderWriter {
                    getString("key1") shouldBe "value1"
                    getString("key2") shouldBe "value2"
                    getString("key3") shouldBe "value3"
                    getString("key4") shouldBe "value4"
                    column("cf1").apply {
                        getString("key1") shouldBe "value1-cf1"
                        getString("key2") shouldBe "value2-cf1"
                        getString("key3") shouldBe "value3-cf1"
                        getString("key4") shouldBe "value4-cf1"
                    }
                    column("cf2").apply {
                        getString("key1") shouldBe "value1-cf2"
                        getString("key2") shouldBe "value2-cf2"
                        getString("key3") shouldBe "value3-cf2"
                        getString("key4") shouldBe "value4-cf2"
                    }
                    column("cf3").apply {
                        getString("key1") shouldBe "value1-cf3"
                        getString("key2") shouldBe "value2-cf3"
                        getString("key3") shouldBe "value3-cf3"
                        getString("key4") shouldBe "value4-cf3"
                    }
                }
                stadium.atomicWrite {
                    withWriter {
                        put("key1", "value11")
                        put("key2", "value22")
                        put("key3", "value33")
                        put("key4", "value44")

                        column("cf1").apply {
                            put("key1", "value11-cf1")
                            put("key2", "value22-cf1")
                            put("key3", "value33-cf1")
                            put("key4", "value44-cf1")
                        }
                        column("cf2").apply {
                            put("key1", "value11-cf2")
                            put("key2", "value22-cf2")
                            put("key3", "value33-cf2")
                            put("key4", "value44-cf2")
                        }
                        column("cf3").apply {
                            put("key1", "value11-cf3")
                            put("key2", "value22-cf3")
                            put("key3", "value33-cf3")
                            put("key4", "value44-cf3")
                        }
                        true
                    }
                }
                stadium.getReaderWriter().withReaderWriter {
                    getString("key1") shouldBe "value11"
                    getString("key2") shouldBe "value22"
                    getString("key3") shouldBe "value33"
                    getString("key4") shouldBe "value44"
                    column("cf1").apply {
                        getString("key1") shouldBe "value11-cf1"
                        getString("key2") shouldBe "value22-cf1"
                        getString("key3") shouldBe "value33-cf1"
                        getString("key4") shouldBe "value44-cf1"
                    }
                    column("cf2").apply {
                        getString("key1") shouldBe "value11-cf2"
                        getString("key2") shouldBe "value22-cf2"
                        getString("key3") shouldBe "value33-cf2"
                        getString("key4") shouldBe "value44-cf2"
                    }
                    column("cf3").apply {
                        getString("key1") shouldBe "value11-cf3"
                        getString("key2") shouldBe "value22-cf3"
                        getString("key3") shouldBe "value33-cf3"
                        getString("key4") shouldBe "value44-cf3"
                    }
                }
            }
        }
        it("Transaction (FULL)") {
            RockStadium.constract {
                dbPath("multi.db")
                transaction(RockStadium.TransactionType.FULL)
                column { name("cf1") }
                column { name("cf2") }
                column { name("cf3") }
            }.use { stadium ->
                stadium.transaction {
                    withReaderWriter {
                        columns(true).forEach { it.clean() }
                    }
                    true
                }
                stadium.transaction {
                    withReaderWriter {
                        put("key1", "value1")
                        put("key2", "value2")
                        put("key3", "value3")
                        put("key4", "value4")

                        column("cf1").apply {
                            put("key1", "value1-cf1")
                            put("key2", "value2-cf1")
                            put("key3", "value3-cf1")
                            put("key4", "value4-cf1")
                        }
                        column("cf2").apply {
                            put("key1", "value1-cf2")
                            put("key2", "value2-cf2")
                            put("key3", "value3-cf2")
                            put("key4", "value4-cf2")
                        }
                        column("cf3").apply {
                            put("key1", "value1-cf3")
                            put("key2", "value2-cf3")
                            put("key3", "value3-cf3")
                            put("key4", "value4-cf3")
                        }

                        getString("key1") shouldBe "value1"
                        getString("key2") shouldBe "value2"
                        getString("key3") shouldBe "value3"
                        getString("key4") shouldBe "value4"

                        column("cf1").apply {
                            getString("key1") shouldBe "value1-cf1"
                            getString("key2") shouldBe "value2-cf1"
                            getString("key3") shouldBe "value3-cf1"
                            getString("key4") shouldBe "value4-cf1"
                        }
                        column("cf2").apply {
                            getString("key1") shouldBe "value1-cf2"
                            getString("key2") shouldBe "value2-cf2"
                            getString("key3") shouldBe "value3-cf2"
                            getString("key4") shouldBe "value4-cf2"
                        }
                        column("cf3").apply {
                            getString("key1") shouldBe "value1-cf3"
                            getString("key2") shouldBe "value2-cf3"
                            getString("key3") shouldBe "value3-cf3"
                            getString("key4") shouldBe "value4-cf3"
                        }
                    }
                    true
                }
                stadium.getReaderWriter().withReaderWriter {
                    getString("key1") shouldBe "value1"
                    getString("key2") shouldBe "value2"
                    getString("key3") shouldBe "value3"
                    getString("key4") shouldBe "value4"
                    column("cf1").apply {
                        getString("key1") shouldBe "value1-cf1"
                        getString("key2") shouldBe "value2-cf1"
                        getString("key3") shouldBe "value3-cf1"
                        getString("key4") shouldBe "value4-cf1"
                    }
                    column("cf2").apply {
                        getString("key1") shouldBe "value1-cf2"
                        getString("key2") shouldBe "value2-cf2"
                        getString("key3") shouldBe "value3-cf2"
                        getString("key4") shouldBe "value4-cf2"
                    }
                    column("cf3").apply {
                        getString("key1") shouldBe "value1-cf3"
                        getString("key2") shouldBe "value2-cf3"
                        getString("key3") shouldBe "value3-cf3"
                        getString("key4") shouldBe "value4-cf3"
                    }
                }

                stadium.transaction {
                    withReaderWriter {
                        getString("key1") shouldBe "value1"
                        getString("key2") shouldBe "value2"
                        getString("key3") shouldBe "value3"
                        getString("key4") shouldBe "value4"
                        column("cf1").apply {
                            getString("key1") shouldBe "value1-cf1"
                            getString("key2") shouldBe "value2-cf1"
                            getString("key3") shouldBe "value3-cf1"
                            getString("key4") shouldBe "value4-cf1"
                        }
                        column("cf2").apply {
                            getString("key1") shouldBe "value1-cf2"
                            getString("key2") shouldBe "value2-cf2"
                            getString("key3") shouldBe "value3-cf2"
                            getString("key4") shouldBe "value4-cf2"
                        }
                        column("cf3").apply {
                            getString("key1") shouldBe "value1-cf3"
                            getString("key2") shouldBe "value2-cf3"
                            getString("key3") shouldBe "value3-cf3"
                            getString("key4") shouldBe "value4-cf3"
                        }


                        put("key1", "value11")
                        put("key2", "value22")
                        put("key3", "value33")
                        put("key4", "value44")

                        column("cf1").apply {
                            put("key1", "value11-cf1")
                            put("key2", "value22-cf1")
                            put("key3", "value33-cf1")
                            put("key4", "value44-cf1")
                        }
                        column("cf2").apply {
                            put("key1", "value11-cf2")
                            put("key2", "value22-cf2")
                            put("key3", "value33-cf2")
                            put("key4", "value44-cf2")
                        }
                        column("cf3").apply {
                            put("key1", "value11-cf3")
                            put("key2", "value22-cf3")
                            put("key3", "value33-cf3")
                            put("key4", "value44-cf3")
                        }

                        getString("key1") shouldBe "value11"
                        getString("key2") shouldBe "value22"
                        getString("key3") shouldBe "value33"
                        getString("key4") shouldBe "value44"

                        column("cf1").apply {
                            getString("key1") shouldBe "value11-cf1"
                            getString("key2") shouldBe "value22-cf1"
                            getString("key3") shouldBe "value33-cf1"
                            getString("key4") shouldBe "value44-cf1"
                        }
                        column("cf2").apply {
                            getString("key1") shouldBe "value11-cf2"
                            getString("key2") shouldBe "value22-cf2"
                            getString("key3") shouldBe "value33-cf2"
                            getString("key4") shouldBe "value44-cf2"
                        }
                        column("cf3").apply {
                            getString("key1") shouldBe "value11-cf3"
                            getString("key2") shouldBe "value22-cf3"
                            getString("key3") shouldBe "value33-cf3"
                            getString("key4") shouldBe "value44-cf3"
                        }
                    }
                    false   // rollback
                }
                stadium.getReaderWriter().withReaderWriter {
                    getString("key1") shouldBe "value1"
                    getString("key2") shouldBe "value2"
                    getString("key3") shouldBe "value3"
                    getString("key4") shouldBe "value4"
                    column("cf1").apply {
                        getString("key1") shouldBe "value1-cf1"
                        getString("key2") shouldBe "value2-cf1"
                        getString("key3") shouldBe "value3-cf1"
                        getString("key4") shouldBe "value4-cf1"
                    }
                    column("cf2").apply {
                        getString("key1") shouldBe "value1-cf2"
                        getString("key2") shouldBe "value2-cf2"
                        getString("key3") shouldBe "value3-cf2"
                        getString("key4") shouldBe "value4-cf2"
                    }
                    column("cf3").apply {
                        getString("key1") shouldBe "value1-cf3"
                        getString("key2") shouldBe "value2-cf3"
                        getString("key3") shouldBe "value3-cf3"
                        getString("key4") shouldBe "value4-cf3"
                    }
                }
                stadium.transaction {
                    withReaderWriter {
                        getString("key1") shouldBe "value1"
                        getString("key2") shouldBe "value2"
                        getString("key3") shouldBe "value3"
                        getString("key4") shouldBe "value4"
                        column("cf1").apply {
                            getString("key1") shouldBe "value1-cf1"
                            getString("key2") shouldBe "value2-cf1"
                            getString("key3") shouldBe "value3-cf1"
                            getString("key4") shouldBe "value4-cf1"
                        }
                        column("cf2").apply {
                            getString("key1") shouldBe "value1-cf2"
                            getString("key2") shouldBe "value2-cf2"
                            getString("key3") shouldBe "value3-cf2"
                            getString("key4") shouldBe "value4-cf2"
                        }
                        column("cf3").apply {
                            getString("key1") shouldBe "value1-cf3"
                            getString("key2") shouldBe "value2-cf3"
                            getString("key3") shouldBe "value3-cf3"
                            getString("key4") shouldBe "value4-cf3"
                        }


                        put("key1", "value11")
                        put("key2", "value22")
                        put("key3", "value33")
                        put("key4", "value44")

                        column("cf1").apply {
                            put("key1", "value11-cf1")
                            put("key2", "value22-cf1")
                            put("key3", "value33-cf1")
                            put("key4", "value44-cf1")
                        }
                        column("cf2").apply {
                            put("key1", "value11-cf2")
                            put("key2", "value22-cf2")
                            put("key3", "value33-cf2")
                            put("key4", "value44-cf2")
                        }
                        column("cf3").apply {
                            put("key1", "value11-cf3")
                            put("key2", "value22-cf3")
                            put("key3", "value33-cf3")
                            put("key4", "value44-cf3")
                        }

                        getString("key1") shouldBe "value11"
                        getString("key2") shouldBe "value22"
                        getString("key3") shouldBe "value33"
                        getString("key4") shouldBe "value44"

                        column("cf1").apply {
                            getString("key1") shouldBe "value11-cf1"
                            getString("key2") shouldBe "value22-cf1"
                            getString("key3") shouldBe "value33-cf1"
                            getString("key4") shouldBe "value44-cf1"
                        }
                        column("cf2").apply {
                            getString("key1") shouldBe "value11-cf2"
                            getString("key2") shouldBe "value22-cf2"
                            getString("key3") shouldBe "value33-cf2"
                            getString("key4") shouldBe "value44-cf2"
                        }
                        column("cf3").apply {
                            getString("key1") shouldBe "value11-cf3"
                            getString("key2") shouldBe "value22-cf3"
                            getString("key3") shouldBe "value33-cf3"
                            getString("key4") shouldBe "value44-cf3"
                        }
                    }
                    true
                }
                stadium.transaction {
                    withReaderWriter {
                        put("key1", "value11")
                        put("key2", "value22")
                        put("key3", "value33")
                        delete("key4")

                        column("cf1").apply {
                            put("key1", "value11-cf1")
                            put("key2", "value22-cf1")
                            delete("key3")
                            put("key4", "value44-cf1")
                        }
                        column("cf2").apply {
                            put("key1", "value11-cf2")
                            delete("key2")
                            put("key3", "value33-cf2")
                            put("key4", "value44-cf2")
                        }
                        column("cf3").apply {
                            delete("key1")
                            put("key2", "value22-cf3")
                            put("key3", "value33-cf3")
                            put("key4", "value44-cf3")
                        }
                    }
                    true
                }
                stadium.getReaderWriter().withReaderWriter {
                    getString("key1") shouldBe "value11"
                    getString("key2") shouldBe "value22"
                    getString("key3") shouldBe "value33"
                    getString("key4") shouldBe null
                    column("cf1").apply {
                        getString("key1") shouldBe "value11-cf1"
                        getString("key2") shouldBe "value22-cf1"
                        getString("key3") shouldBe null
                        getString("key4") shouldBe "value44-cf1"
                    }
                    column("cf2").apply {
                        getString("key1") shouldBe "value11-cf2"
                        getString("key2") shouldBe null
                        getString("key3") shouldBe "value33-cf2"
                        getString("key4") shouldBe "value44-cf2"
                    }
                    column("cf3").apply {
                        getString("key1") shouldBe null
                        getString("key2") shouldBe "value22-cf3"
                        getString("key3") shouldBe "value33-cf3"
                        getString("key4") shouldBe "value44-cf3"
                    }
                }
            }
        }
        it("Transaction (OPTIMISTIC)") {
            RockStadium.constract {
                dbPath("multi.db")
                transaction(RockStadium.TransactionType.OPTIMISTIC)
                column { name("cf1") }
                column { name("cf2") }
                column { name("cf3") }
            }.use { stadium ->
                stadium.transaction {
                    withReaderWriter {
                        columns(true).forEach { it.clean() }
                    }
                    true
                }
                stadium.transaction {
                    withReaderWriter {
                        put("key1", "value1")
                        put("key2", "value2")
                        put("key3", "value3")
                        put("key4", "value4")

                        column("cf1").apply {
                            put("key1", "value1-cf1")
                            put("key2", "value2-cf1")
                            put("key3", "value3-cf1")
                            put("key4", "value4-cf1")
                        }
                        column("cf2").apply {
                            put("key1", "value1-cf2")
                            put("key2", "value2-cf2")
                            put("key3", "value3-cf2")
                            put("key4", "value4-cf2")
                        }
                        column("cf3").apply {
                            put("key1", "value1-cf3")
                            put("key2", "value2-cf3")
                            put("key3", "value3-cf3")
                            put("key4", "value4-cf3")
                        }

                        getString("key1") shouldBe "value1"
                        getString("key2") shouldBe "value2"
                        getString("key3") shouldBe "value3"
                        getString("key4") shouldBe "value4"

                        column("cf1").apply {
                            getString("key1") shouldBe "value1-cf1"
                            getString("key2") shouldBe "value2-cf1"
                            getString("key3") shouldBe "value3-cf1"
                            getString("key4") shouldBe "value4-cf1"
                        }
                        column("cf2").apply {
                            getString("key1") shouldBe "value1-cf2"
                            getString("key2") shouldBe "value2-cf2"
                            getString("key3") shouldBe "value3-cf2"
                            getString("key4") shouldBe "value4-cf2"
                        }
                        column("cf3").apply {
                            getString("key1") shouldBe "value1-cf3"
                            getString("key2") shouldBe "value2-cf3"
                            getString("key3") shouldBe "value3-cf3"
                            getString("key4") shouldBe "value4-cf3"
                        }
                    }
                    true
                }
                stadium.getReaderWriter().withReaderWriter {
                    getString("key1") shouldBe "value1"
                    getString("key2") shouldBe "value2"
                    getString("key3") shouldBe "value3"
                    getString("key4") shouldBe "value4"
                    column("cf1").apply {
                        getString("key1") shouldBe "value1-cf1"
                        getString("key2") shouldBe "value2-cf1"
                        getString("key3") shouldBe "value3-cf1"
                        getString("key4") shouldBe "value4-cf1"
                    }
                    column("cf2").apply {
                        getString("key1") shouldBe "value1-cf2"
                        getString("key2") shouldBe "value2-cf2"
                        getString("key3") shouldBe "value3-cf2"
                        getString("key4") shouldBe "value4-cf2"
                    }
                    column("cf3").apply {
                        getString("key1") shouldBe "value1-cf3"
                        getString("key2") shouldBe "value2-cf3"
                        getString("key3") shouldBe "value3-cf3"
                        getString("key4") shouldBe "value4-cf3"
                    }
                }

                stadium.transaction {
                    withReaderWriter {
                        getString("key1") shouldBe "value1"
                        getString("key2") shouldBe "value2"
                        getString("key3") shouldBe "value3"
                        getString("key4") shouldBe "value4"
                        column("cf1").apply {
                            getString("key1") shouldBe "value1-cf1"
                            getString("key2") shouldBe "value2-cf1"
                            getString("key3") shouldBe "value3-cf1"
                            getString("key4") shouldBe "value4-cf1"
                        }
                        column("cf2").apply {
                            getString("key1") shouldBe "value1-cf2"
                            getString("key2") shouldBe "value2-cf2"
                            getString("key3") shouldBe "value3-cf2"
                            getString("key4") shouldBe "value4-cf2"
                        }
                        column("cf3").apply {
                            getString("key1") shouldBe "value1-cf3"
                            getString("key2") shouldBe "value2-cf3"
                            getString("key3") shouldBe "value3-cf3"
                            getString("key4") shouldBe "value4-cf3"
                        }


                        put("key1", "value11")
                        put("key2", "value22")
                        put("key3", "value33")
                        put("key4", "value44")

                        column("cf1").apply {
                            put("key1", "value11-cf1")
                            put("key2", "value22-cf1")
                            put("key3", "value33-cf1")
                            put("key4", "value44-cf1")
                        }
                        column("cf2").apply {
                            put("key1", "value11-cf2")
                            put("key2", "value22-cf2")
                            put("key3", "value33-cf2")
                            put("key4", "value44-cf2")
                        }
                        column("cf3").apply {
                            put("key1", "value11-cf3")
                            put("key2", "value22-cf3")
                            put("key3", "value33-cf3")
                            put("key4", "value44-cf3")
                        }

                        getString("key1") shouldBe "value11"
                        getString("key2") shouldBe "value22"
                        getString("key3") shouldBe "value33"
                        getString("key4") shouldBe "value44"

                        column("cf1").apply {
                            getString("key1") shouldBe "value11-cf1"
                            getString("key2") shouldBe "value22-cf1"
                            getString("key3") shouldBe "value33-cf1"
                            getString("key4") shouldBe "value44-cf1"
                        }
                        column("cf2").apply {
                            getString("key1") shouldBe "value11-cf2"
                            getString("key2") shouldBe "value22-cf2"
                            getString("key3") shouldBe "value33-cf2"
                            getString("key4") shouldBe "value44-cf2"
                        }
                        column("cf3").apply {
                            getString("key1") shouldBe "value11-cf3"
                            getString("key2") shouldBe "value22-cf3"
                            getString("key3") shouldBe "value33-cf3"
                            getString("key4") shouldBe "value44-cf3"
                        }
                    }
                    false   // rollback
                }
                stadium.getReaderWriter().withReaderWriter {
                    getString("key1") shouldBe "value1"
                    getString("key2") shouldBe "value2"
                    getString("key3") shouldBe "value3"
                    getString("key4") shouldBe "value4"
                    column("cf1").apply {
                        getString("key1") shouldBe "value1-cf1"
                        getString("key2") shouldBe "value2-cf1"
                        getString("key3") shouldBe "value3-cf1"
                        getString("key4") shouldBe "value4-cf1"
                    }
                    column("cf2").apply {
                        getString("key1") shouldBe "value1-cf2"
                        getString("key2") shouldBe "value2-cf2"
                        getString("key3") shouldBe "value3-cf2"
                        getString("key4") shouldBe "value4-cf2"
                    }
                    column("cf3").apply {
                        getString("key1") shouldBe "value1-cf3"
                        getString("key2") shouldBe "value2-cf3"
                        getString("key3") shouldBe "value3-cf3"
                        getString("key4") shouldBe "value4-cf3"
                    }
                }
                stadium.transaction {
                    withReaderWriter {
                        getString("key1") shouldBe "value1"
                        getString("key2") shouldBe "value2"
                        getString("key3") shouldBe "value3"
                        getString("key4") shouldBe "value4"
                        column("cf1").apply {
                            getString("key1") shouldBe "value1-cf1"
                            getString("key2") shouldBe "value2-cf1"
                            getString("key3") shouldBe "value3-cf1"
                            getString("key4") shouldBe "value4-cf1"
                        }
                        column("cf2").apply {
                            getString("key1") shouldBe "value1-cf2"
                            getString("key2") shouldBe "value2-cf2"
                            getString("key3") shouldBe "value3-cf2"
                            getString("key4") shouldBe "value4-cf2"
                        }
                        column("cf3").apply {
                            getString("key1") shouldBe "value1-cf3"
                            getString("key2") shouldBe "value2-cf3"
                            getString("key3") shouldBe "value3-cf3"
                            getString("key4") shouldBe "value4-cf3"
                        }


                        put("key1", "value11")
                        put("key2", "value22")
                        put("key3", "value33")
                        put("key4", "value44")

                        column("cf1").apply {
                            put("key1", "value11-cf1")
                            put("key2", "value22-cf1")
                            put("key3", "value33-cf1")
                            put("key4", "value44-cf1")
                        }
                        column("cf2").apply {
                            put("key1", "value11-cf2")
                            put("key2", "value22-cf2")
                            put("key3", "value33-cf2")
                            put("key4", "value44-cf2")
                        }
                        column("cf3").apply {
                            put("key1", "value11-cf3")
                            put("key2", "value22-cf3")
                            put("key3", "value33-cf3")
                            put("key4", "value44-cf3")
                        }

                        getString("key1") shouldBe "value11"
                        getString("key2") shouldBe "value22"
                        getString("key3") shouldBe "value33"
                        getString("key4") shouldBe "value44"

                        column("cf1").apply {
                            getString("key1") shouldBe "value11-cf1"
                            getString("key2") shouldBe "value22-cf1"
                            getString("key3") shouldBe "value33-cf1"
                            getString("key4") shouldBe "value44-cf1"
                        }
                        column("cf2").apply {
                            getString("key1") shouldBe "value11-cf2"
                            getString("key2") shouldBe "value22-cf2"
                            getString("key3") shouldBe "value33-cf2"
                            getString("key4") shouldBe "value44-cf2"
                        }
                        column("cf3").apply {
                            getString("key1") shouldBe "value11-cf3"
                            getString("key2") shouldBe "value22-cf3"
                            getString("key3") shouldBe "value33-cf3"
                            getString("key4") shouldBe "value44-cf3"
                        }
                    }
                    true
                }
                stadium.transaction {
                    withReaderWriter {
                        put("key1", "value11")
                        put("key2", "value22")
                        put("key3", "value33")
                        delete("key4")

                        column("cf1").apply {
                            put("key1", "value11-cf1")
                            put("key2", "value22-cf1")
                            delete("key3")
                            put("key4", "value44-cf1")
                        }
                        column("cf2").apply {
                            put("key1", "value11-cf2")
                            delete("key2")
                            put("key3", "value33-cf2")
                            put("key4", "value44-cf2")
                        }
                        column("cf3").apply {
                            delete("key1")
                            put("key2", "value22-cf3")
                            put("key3", "value33-cf3")
                            put("key4", "value44-cf3")
                        }
                    }
                    true
                }
                stadium.getReaderWriter().withReaderWriter {
                    getString("key1") shouldBe "value11"
                    getString("key2") shouldBe "value22"
                    getString("key3") shouldBe "value33"
                    getString("key4") shouldBe null
                    column("cf1").apply {
                        getString("key1") shouldBe "value11-cf1"
                        getString("key2") shouldBe "value22-cf1"
                        getString("key3") shouldBe null
                        getString("key4") shouldBe "value44-cf1"
                    }
                    column("cf2").apply {
                        getString("key1") shouldBe "value11-cf2"
                        getString("key2") shouldBe null
                        getString("key3") shouldBe "value33-cf2"
                        getString("key4") shouldBe "value44-cf2"
                    }
                    column("cf3").apply {
                        getString("key1") shouldBe null
                        getString("key2") shouldBe "value22-cf3"
                        getString("key3") shouldBe "value33-cf3"
                        getString("key4") shouldBe "value44-cf3"
                    }
                }
            }
        }
    }
})