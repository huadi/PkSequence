package huadi;

import java.util.Map;
import java.util.Properties;
import java.util.concurrent.ConcurrentHashMap;

import static huadi.Const.DEFAULT_SEQUENCE_TABLE_NAME;
import static huadi.Const.PROP_DRIVER_NAME;

/**
 * For ones want to use multiple PkSequence in one place, this is what you want.
 * <p>
 * Config it as {@link PkSequence}, use {@link #get(String)} instead of {@link PkSequence#get()}.
 * Be careful the string passed to {@link #get(String)} must in pk_sequence table.
 *
 * @author HUA Di
 */
@SuppressWarnings("unused")
public class MultiPkSequence {
    private Properties databaseConfig = new Properties();

    private String sequenceTableName = DEFAULT_SEQUENCE_TABLE_NAME;

    public void init() {
        try {
            Class.forName(databaseConfig.getProperty(PROP_DRIVER_NAME));
        } catch (ClassNotFoundException e) {
            throw new Error("No JDBC driver.", e);
        }
    }

    private Map<String, PkSequence> sequences = new ConcurrentHashMap<>();

    public Long get(String name) {
        if (!sequences.containsKey(name)) {
            synchronized (this) {
                if (!sequences.containsKey(name)) {
                    PkSequence sequence = new PkSequence();
                    sequence.setName(name);
                    sequence.setDatabaseConfig(databaseConfig);
                    sequence.setSequenceTableName(sequenceTableName);
                    sequence.init();
                    sequences.put(name, sequence);
                }
            }
        }
        return sequences.get(name).get();
    }


    public void setDatabaseConfig(Properties databaseConfig) {
        this.databaseConfig = databaseConfig;
    }

    public void setSequenceTableName(String sequenceTableName) {
        this.sequenceTableName = sequenceTableName;
    }
}
