package rock;

import org.rocksdb.RocksDB;

public class Preload {
    public static void execute() {
        RocksDB.loadLibrary();
    }
}
