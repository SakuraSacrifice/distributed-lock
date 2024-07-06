package com.lee.learn.distributed.lock;

import com.alibaba.ttl.TransmittableThreadLocal;
import com.zaxxer.hikari.HikariDataSource;
import lombok.extern.slf4j.Slf4j;

import javax.sql.DataSource;
import java.sql.Connection;
import java.sql.SQLException;
import java.sql.Statement;
import java.util.Map;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.atomic.AtomicInteger;

@Slf4j
public class DistributedLockUtilImpl1 {

    private static final String USER_NAME = "root";
    private static final String PASS_WORD = "root";
    private static final String JDBC_URL = "jdbc:mysql://localhost:3306/test";
    private static final String DRIVER_CLASS_NAME = "com.mysql.cj.jdbc.Driver";

    private static DataSource dataSource = null;

    private static final String SQL_SELECT_FOR_UPDATE = "SELECT id,lock_key FROM distributed_locks_impl_1 WHERE lock_key=\"%s\" FOR UPDATE;";

    private static final Map<String, LockManager> LOCK_MANAGER_MAP = new ConcurrentHashMap<>();

    private static final int SECONDS_CONTINUE_WAIT = 0;
    private static final int SECONDS_BRIEF_WAIT = 1;

    static {
        LOCK_MANAGER_MAP.put(LockKey.COMMON_KEY.getKey(), new LockManager());
        LOCK_MANAGER_MAP.put(LockKey.EXTEND_KEY.getKey(), new LockManager());
    }

    /**
     * 加指定{@link LockKey}的锁。<br/>
     * 如果获取不到锁，会持续等待。
     *
     * @param lockKey 见{@link LockKey}。
     */
    public static void lock(LockKey lockKey) {
        lock(lockKey, SECONDS_CONTINUE_WAIT);
    }

    /**
     * 加指定{@link LockKey}的锁。<br/>
     *
     * @param lockKey 见{@link LockKey}。
     * @param waitSeconds 等待时间，单位s。如果设置为0，则表示持续等待，
     *                    不允许设置为小于0。
     * @return true表示加锁成功；
     *          false表示加锁失败。
     */
    public static boolean lock(LockKey lockKey, int waitSeconds) {
        if (waitSeconds < 0) {
            return false;
        }

        LockManager lockManager = LOCK_MANAGER_MAP.get(lockKey.getKey());

        if (null == lockManager) {
            return false;
        }

        TransmittableThreadLocal<Connection> ttl = lockManager.getTtl();

        if (null != ttl.get()) {
            // 锁重入场景
            lockManager.reentrant();
            return true;
        }

        Connection connection = null;
        try {
            connection = getConnection();
            doLock(connection, lockKey, waitSeconds);
            ttl.set(connection);
            return true;
        } catch (Exception e1) {
            log.error("lock failed", e1);
            try {
                if (null != connection) {
                    connection.rollback();
                    connection.close();
                }
            } catch (Exception e2) {
                log.error("release connection failed", e2);
            } finally {
                ttl.remove();
            }
            return false;
        }
    }

    public static boolean tryLock(LockKey lockKey) {
        return lock(lockKey, SECONDS_BRIEF_WAIT);
    }

    /**
     * 释放锁。
     *
     * @param lockKey 见{@link LockKey}。
     * @return true表示释放成功；
     *          false表示释放失败或者未持有锁时释放。
     */
    public static boolean unLock(LockKey lockKey) {
        LockManager lockManager = LOCK_MANAGER_MAP.get(lockKey.getKey());

        if (null == lockManager) {
            return false;
        }

        TransmittableThreadLocal<Connection> ttl = lockManager.getTtl();
        Connection connection = ttl.get();
        if (connection == null) {
            return false;
        }

        AtomicInteger lockCount = lockManager.getReentrantCount();
        if (lockCount.get() > 0) {
            // 释放重入锁场景
            lockManager.unReentrant();
            return true;
        }

        try {
            connection.commit();
            return true;
        } catch (Exception e) {
            log.error("release lock failed", e);
            return false;
        } finally {
            try {
                connection.close();
            } catch (Exception e) {
                log.error("release connection failed", e);
            }
            ttl.remove();
        }
    }

    private static void doLock(Connection connection, LockKey lockKey, int queryTimeoutSeconds) throws SQLException {
        connection.setAutoCommit(false);
        try (Statement statement = connection.createStatement()) {
            statement.setQueryTimeout(queryTimeoutSeconds);
            statement.executeQuery(String.format(SQL_SELECT_FOR_UPDATE, lockKey.getKey()));
        }
    }

    private static Connection getConnection() throws SQLException {
        if (null == dataSource) {
            synchronized (DistributedLockUtilImpl1.class) {
                if (null == dataSource) {
                    dataSource = new HikariDataSource();
                    ((HikariDataSource) dataSource).setUsername(USER_NAME);
                    ((HikariDataSource) dataSource).setPassword(PASS_WORD);
                    ((HikariDataSource) dataSource).setJdbcUrl(JDBC_URL);
                    ((HikariDataSource) dataSource).setDriverClassName(DRIVER_CLASS_NAME);
                    return dataSource.getConnection();
                }
            }
        }
        return dataSource.getConnection();
    }

    public static class LockManager {
        private final TransmittableThreadLocal<Connection> ttl;
        private final AtomicInteger reentrantCount;

        public LockManager() {
            ttl = new TransmittableThreadLocal<>();
            reentrantCount = new AtomicInteger(0);
        }

        public TransmittableThreadLocal<Connection> getTtl() {
            return ttl;
        }

        public AtomicInteger getReentrantCount() {
            return reentrantCount;
        }

        public void reentrant() {
            reentrantCount.incrementAndGet();
        }

        public void unReentrant() {
            reentrantCount.decrementAndGet();
        }
    }

    public enum LockKey {
        COMMON_KEY("common_key"),
        EXTEND_KEY("extend_key");

        private final String key;

        LockKey(String key) {
            this.key = key;
        }

        public String getKey() {
            return key;
        }
    }

}