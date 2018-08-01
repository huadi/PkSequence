package huadi;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.sql.*;
import java.util.Properties;
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
    }

    private String sequenceTableName = DEFAULT_SEQUENCE_TABLE_NAME;
    private static final String KEY_COLUMN = "k";
    private static final String VALUE_COLUMN = "v";
    private static final String STEP_COLUMN = "step";

    private String name;

    // 初始化一个不能用的pool, 第一次使用会触发更新. 省得每次在get方法中做null判断.
    private SeqPool seqPool = new SeqPool(0, -1);


    public Long get() {
        long primaryKey;
        if ((primaryKey = seqPool.next()) == SeqPool.INVALID_VALUE) {
            synchronized (this) {
                while ((primaryKey = seqPool.next()) == SeqPool.INVALID_VALUE) {
                    seqPool = load();
                }
            }
        }
        return primaryKey;
    }


    private SeqPool load() {
        // 高并发情况下, 优先考虑增大step减少load次数, 所以这里并不需要强制使用连接池.
        try (Connection c = DriverManager.getConnection(databaseConfig.getProperty(PROP_CONN_URL),
                databaseConfig.getProperty(PROP_CONN_USERNAME), databaseConfig.getProperty(PROP_CONN_PASSWORD))) {
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
                        return new SeqPool(oldValue + 1, newValue);
                    }
                }
                logger.warn("[PkSeq] SeqPool load conflict, retry. KeyName: \"{}\", old: {}, new: {}, step: {}.",
                        name, oldValue, newValue, stepValue);
            }
        } catch (SQLException e) {
            throw new RuntimeException("Exception on loading sequence.", e);
        }
    }

    // 对于get方法来讲, pk和max必须保证原子更新, 所以这里用一个class封装.
    private static class SeqPool {
        static final long INVALID_VALUE = Long.MIN_VALUE;

        AtomicLong seq;
        long max;

        SeqPool(long seq, long max) {
            this.seq = new AtomicLong(seq);
            this.max = max;
        }

        long next() {
            long v = seq.getAndIncrement();
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
