package huadi;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.sql.*;
import java.util.Properties;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.concurrent.atomic.AtomicLong;

import static huadi.Const.*;

/**
 * Primary Key sequence generator.
 *
 * @author HUA Di
 */
@SuppressWarnings("unused")
public class PkSequence {
    private static final Logger logger = LoggerFactory.getLogger(PkSequence.class);

    private Properties databaseConfig = new Properties();


    public void init() {
        if (name == null) {
            throw new RuntimeException("No sequence name configured.");
        }
        try {
            Class.forName(databaseConfig.getProperty(PROP_DRIVER_NAME));
        } catch (ClassNotFoundException e) {
            throw new Error("No JDBC driver.", e);
        }
        preload();
    }

    private String sequenceTableName = DEFAULT_SEQUENCE_TABLE_NAME;
    private static final String KEY_COLUMN = "k";
    private static final String VALUE_COLUMN = "v";
    private static final String STEP_COLUMN = "step";

    private String name;

    // 初始化一个不能用的pool, 第一次使用会触发更新. 省得每次在get方法中做null判断.
    private SeqPool seqPool = new SeqPool(0, -1);
    private SeqPool preloadSeqPool;

    private AtomicBoolean onPreload = new AtomicBoolean();


    public Long get() {
        long primaryKey;
        if ((primaryKey = seqPool.next()) == SeqPool.INVALID_VALUE) {
            synchronized (this) {
                while ((primaryKey = seqPool.next()) == SeqPool.INVALID_VALUE) {
                    if (preloadSeqPool != null) {
                        seqPool = preloadSeqPool;
                        preloadSeqPool = null;
                    } else {
                        logger.warn("[PkSeq] Switch pool fail. " +
                                "It may be caused by heavy request, try to increase value of 'step' column.");
                    }
                }
            }
        }
        return primaryKey;
    }


    private ExecutorService preloadThreadPool = Executors
            .newSingleThreadExecutor(r -> new Thread(r, "PkSequencePreloadThread-" + name));

    private void preload() {
        if (preloadSeqPool != null || onPreload.get() || !onPreload.compareAndSet(false, true)) {
            return;
        }

        preloadThreadPool.submit(() -> {
            try (Connection c = getConn()) {
                while (true) {
                    Long oldValue, stepValue, newValue;
                    try (Statement s = c.createStatement()) {
                        String sql = "SELECT * FROM " + sequenceTableName + " WHERE " + KEY_COLUMN + "='" + name + "'";
                        try (ResultSet r = s.executeQuery(sql)) {
                            r.next();
                            stepValue = r.getLong(STEP_COLUMN);
                            if (stepValue <= 0) { // step不能为0, 会导致在get无限循环.
                                throw new RuntimeException("Step cannot be 0.");
                            }
                            oldValue = r.getLong(VALUE_COLUMN);
                            newValue = oldValue + stepValue; // 这里有可能溢出
                        }
                        sql = "UPDATE " + sequenceTableName + " SET " + VALUE_COLUMN + "=" + newValue +
                                " WHERE " + KEY_COLUMN + "='" + name + "' AND " + VALUE_COLUMN + "=" + oldValue;
                        if (s.executeUpdate(sql) != 0) {
                            logger.info("[PkSeq] SeqPool loaded. Min={}, max={}.", oldValue, newValue);
                            // 这里使用oldValue作为seqPool, 这样所有小于db中VALUE_COLUMN域的seq都可以认为是使用过的,
                            // 在应用运行过程中可以随时修改step大小.
                            preloadSeqPool = new SeqPool(oldValue + 1, newValue);
                        }
                    }
                    logger.warn("[PkSeq] SeqPool load conflict, retry. KeyName: \"{}\", old: {}, new: {}, step: {}.",
                            name, oldValue, newValue, stepValue);
                }
            } catch (SQLException e) {
                throw new RuntimeException("Exception on loading sequence.", e);
            } finally {
                onPreload.set(false);
            }
        });
    }

    private Connection getConn() throws SQLException {
        return DriverManager.getConnection(databaseConfig.getProperty(PROP_CONN_URL),
                databaseConfig.getProperty(PROP_CONN_USERNAME), databaseConfig.getProperty(PROP_CONN_PASSWORD));
    }

    // 对于get方法来讲, pk和max必须保证原子更新, 所以这里用一个class封装.
    private class SeqPool {
        // 返回此值时，表示此pool的seq已经消耗完，需要执行switch
        static final long INVALID_VALUE = Long.MIN_VALUE;
        // preload的seq值，达到此seq时，执行preload
        long preloadSeq;
        // 用来评估preloadSeq的时间戳
        long createTimestamp = System.currentTimeMillis();


        AtomicLong seq;
        long max;

        SeqPool(long seq, long max) {
            this.seq = new AtomicLong(seq);
            this.max = max;

            preloadSeq = seq + (long) ((max - seq) * 0.8);
        }

        long next() {
            long v = seq.getAndIncrement();

            if (v > preloadSeq) {
                preload();
            }

            return v > max ? INVALID_VALUE : v;
        }
    }


    public void setDatabaseConfig(Properties databaseConfig) {
        this.databaseConfig = databaseConfig;
    }

    public void setName(String name) {
        this.name = name;
    }

    public void setSequenceTableName(String sequenceTableName) {
        this.sequenceTableName = sequenceTableName;
    }
}
