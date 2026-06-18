package com.kaizen.flink.config;

import org.ini4j.Ini;

import java.io.IOException;
import java.io.InputStream;
import java.io.Serializable;

/**
 * Đọc config từ classpath (config.ini được đóng gói trong shadow JAR).
 * Fallback sang filesystem nếu không tìm thấy trong classpath.
 */
public class FlinkConfig implements Serializable {

    private final String configResource;
    private transient Ini ini;

    public FlinkConfig(String configResource) {
        this.configResource = configResource;
        init();
    }

    private void init() {
        // Thử load từ classpath trước (khi chạy từ shadow JAR)
        InputStream is = getClass().getClassLoader().getResourceAsStream(configResource);
        try {
            if (is != null) {
                this.ini = new Ini(is);
            } else {
                // Fallback: load từ filesystem (khi chạy local / debug)
                this.ini = new Ini(new java.io.File(configResource));
            }
        } catch (IOException e) {
            throw new RuntimeException("Failed to load config: " + configResource, e);
        }
    }

    private Ini getIni() {
        if (ini == null) init();
        return ini;
    }

    public String get(String section, String key) {
        return getIni().get(section, key);
    }

    public String getOrDefault(String section, String key, String defaultValue) {
        String val = get(section, key);
        return (val != null && !val.isEmpty()) ? val : defaultValue;
    }

    public int getInt(String section, String key, int defaultValue) {
        try {
            return Integer.parseInt(get(section, key));
        } catch (Exception e) {
            return defaultValue;
        }
    }

    public long getLong(String section, String key, long defaultValue) {
        try {
            return Long.parseLong(get(section, key));
        } catch (Exception e) {
            return defaultValue;
        }
    }

    public org.apache.flink.streaming.api.environment.StreamExecutionEnvironment createEnv() {
        org.apache.flink.configuration.Configuration conf = new org.apache.flink.configuration.Configuration();
        conf.setString("state.checkpoints.dir", "s3a://flink/checkpoint");
        conf.setString("state.savepoints.dir", "s3a://flink/savepoint");

        String endpoint = get("minio", "endpoint");
        String accessKey = get("minio", "access_key");
        String secretKey = get("minio", "secret_key");

        conf.setString("s3.endpoint", endpoint);
        conf.setString("s3.access-key", accessKey);
        conf.setString("s3.secret-key", secretKey);
        conf.setString("s3.path.style.access", "true");

        conf.setString("fs.s3a.endpoint", endpoint);
        conf.setString("fs.s3a.access.key", accessKey);
        conf.setString("fs.s3a.secret.key", secretKey);
        conf.setString("fs.s3a.path.style.access", "true");
        conf.setString("fs.s3a.impl", "org.apache.hadoop.fs.s3a.S3AFileSystem");

        return org.apache.flink.streaming.api.environment.StreamExecutionEnvironment.getExecutionEnvironment(conf);
    }
}
