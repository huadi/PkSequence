# PkSequence

A high throughput distributed Primary Key sequence generator. Based on relational database.

It can generate global unique (approximate increment) ID.
You can combine other strategy to build your own ID generate algorithm(e.g. adding timestamp, machine id)

All you need is only a relational database which support JDBC. No others(like Redis, Zookeeper) are needed.

## Usage:
1. Create table for sequence persistence:
```sql
CREATE TABLE `pk_sequence` (
  `id` int(11) NOT NULL AUTO_INCREMENT,
  `k` varchar(16) NOT NULL,
  `v` bigint(20) NOT NULL,
  `step` int(10) unsigned NOT NULL,
  `modify_time` datetime NOT NULL DEFAULT CURRENT_TIMESTAMP ON UPDATE CURRENT_TIMESTAMP,
  PRIMARY KEY (`id`),
  UNIQUE KEY `IDX_k_UNIQUE` (`k`)
)
```
This is the default config. If you need to specify other table name, please remember config them in spring described below.

2. Init sequence counter. one sequence, one row:
```sql
INSERT INTO pk_sequence(k, v, step) values ('user_id', 0, 1000);
INSERT INTO pk_sequence(k, v, step) values ('order_id', 0, 1000);
```
Note that the value of step cannot be 0. Also it cannot be less than 0. Since the table DDL init step column to unsigned, it prevents you inserting step with negative values.  
It's value depends on your application's load. More requests, more step.

3. Init `PkSequence` in your code:
```java
PkSequence sequence = new PkSequence();
sequence.setName("user_id");
Properties config = new Properties();
config.setProperty("driverName", "com.mysql.cj.jdbc.Driver");
config.setProperty("url", "jdbc:mysql://localhost:3306/pksequence");
config.setProperty("username", "root");
config.setProperty("password", "root");
sequence.setDatabaseConfig(config);
sequence.init();
```

OR

Config in spring, remember change configs to your own:
```xml
<bean id="sequence" class="huadi.PkSequence" init-method="init">
  <!-- value of name prop is equals to the record's column k in the table pk_sequence. -->
  <property name="name" value="user_id"/>
  <property name="databaseConfig">
    <props>
      <prop key="driverName">com.mysql.cj.jdbc.Driver</prop>
      <prop key="url">jdbc:mysql://localhost:3306/pksequence</prop>
      <prop key="username">root</prop>
      <prop key="password">root</prop>
    </props>
  </property>
  <!-- dataSource has higher priority than databaseConfig. If it is set, the databaseConfig will be ignored. -->
  <property name="dataSource" ref="yourOwnDataSource"/>
  <!-- Config it when you do NOT use default table config. -->
  <property name="sequenceTableName" value="yourOwnSeqTableName"/>
</bean>
```
4. Inject into your bean and get seq in your code:
```java
public class Demo {
    @Autowired
    private PkSequence pkSequence;

    public long getPk() {
        return pkSequence.get();
    }
}
```
5. If you want to use one sequence for all tables, use `MultiPkSequence`.
```xml
<bean id="sequence" class="huadi.MultiPkSequence" init-method="init">
  <property name="databaseConfig">
    <props>
      <prop key="driverName">com.mysql.cj.jdbc.Driver</prop>
      <prop key="url">jdbc:mysql://localhost:3306/pksequence</prop>
      <prop key="username">root</prop>
      <prop key="password">root</prop>
    </props>
  </property>
  <!-- Config it when you do NOT use default table config. -->
  <property name="sequenceTableName" value="yourOwnSeqTableName"/>
</bean>
```
6. Use `MultiPkSequence.get(String)` instead of `PkSequence.get()`:
```java
public class Demo {
    @Autowired
    private MultiPkSequence multiPkSequence;

    public long getUserPk() {
        return multiPkSequence.get("user_id");
    }

    public long getOrderPk() {
        return multiPkSequence.get("order_id");
    }
}
```
Be careful, the string passed to `MultiPkSequence.get(String)` must be exists in the table `pk_sequence`(or your own sequence persistence table).

## Log
You can use `grep PkSeq your-app-log-file.log` to see all the log made by PkSequence.

## Change step
If you find your application's gotten too many load action, just increase the `step` column value on that key in your sequence persistence table directly.
Also feel free to decrease the step at any time. 
