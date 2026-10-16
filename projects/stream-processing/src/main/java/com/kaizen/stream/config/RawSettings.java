package com.kaizen.stream.config;

import java.io.Serializable;
import java.util.Collections;
import java.util.Map;
import java.util.TreeMap;

/**
 * Flat, already-merged "section.key" → value map.
 * Bảng phẳng "section.key" → giá trị, đã gộp các nguồn theo thứ tự ưu tiên.
 */
public final class RawSettings implements Serializable {

    private static final long serialVersionUID = 1L;

    private final Map<String, String> values;

    public RawSettings(Map<String, String> values) {
        this.values = Collections.unmodifiableMap(new TreeMap<>(values));
    }

    public String get(String section, String key) {
        String value = values.get(section + "." + key);
        return value == null || value.trim().isEmpty() ? null : value.trim();
    }

    public String get(String section, String key, String defaultValue) {
        String value = get(section, key);
        return value != null ? value : defaultValue;
    }

    /** Fail fast with the exact key and env var to set. / Dừng sớm, chỉ rõ key và biến env cần đặt. */
    public String require(String section, String key) {
        String value = get(section, key);
        if (value == null) {
            throw new IllegalStateException(String.format(
                    "Missing setting [%s] %s (config.ini, env %s or --%s.%s)",
                    section, key, SettingsLoader.envName(section, key), section, key));
        }
        return value;
    }

    public int getInt(String section, String key, int defaultValue) {
        String value = get(section, key);
        if (value == null) {
            return defaultValue;
        }
        try {
            return Integer.parseInt(value);
        } catch (NumberFormatException e) {
            throw new IllegalStateException("Setting [" + section + "] " + key + " is not an int: " + value, e);
        }
    }

    public long getLong(String section, String key, long defaultValue) {
        String value = get(section, key);
        if (value == null) {
            return defaultValue;
        }
        try {
            return Long.parseLong(value);
        } catch (NumberFormatException e) {
            throw new IllegalStateException("Setting [" + section + "] " + key + " is not a long: " + value, e);
        }
    }

    public double getDouble(String section, String key, double defaultValue) {
        String value = get(section, key);
        if (value == null) {
            return defaultValue;
        }
        try {
            return Double.parseDouble(value);
        } catch (NumberFormatException e) {
            throw new IllegalStateException("Setting [" + section + "] " + key + " is not a number: " + value, e);
        }
    }

    /** Never print values: they may hold passwords. / Không in giá trị vì có thể chứa mật khẩu. */
    @Override
    public String toString() {
        return "RawSettings" + values.keySet();
    }
}
