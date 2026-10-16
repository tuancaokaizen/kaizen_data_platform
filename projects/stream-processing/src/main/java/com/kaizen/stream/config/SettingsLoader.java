package com.kaizen.stream.config;

import org.ini4j.Ini;
import org.ini4j.Profile;

import java.io.IOException;
import java.io.InputStream;
import java.util.HashMap;
import java.util.HashSet;
import java.util.Locale;
import java.util.Map;
import java.util.Set;

/**
 * Builds StreamSettings from: CLI args > env KAIZEN_* > config.ini (classpath) > code defaults.
 * Tạo StreamSettings theo thứ tự ưu tiên: tham số dòng lệnh > env KAIZEN_* > config.ini > mặc định trong code.
 */
public final class SettingsLoader {

    public static final String CONFIG_RESOURCE = "config.ini";
    static final String ENV_PREFIX = "KAIZEN_";

    private SettingsLoader() {
    }

    public static StreamSettings load(String[] args) {
        return new StreamSettings(merge(readIni(CONFIG_RESOURCE), System.getenv(), args));
    }

    /**
     * Merge the three layers; later layers win.
     * Gộp 3 lớp; lớp sau ghi đè lớp trước.
     */
    static RawSettings merge(Map<String, String> ini, Map<String, String> env, String[] args) {
        Map<String, String> merged = new HashMap<>(ini);
        Set<String> sections = new HashSet<>();
        ini.keySet().forEach(k -> sections.add(k.substring(0, k.indexOf('.'))));
        env.forEach((name, value) -> {
            String key = fromEnvName(name, sections);
            if (key != null) {
                merged.put(key, value);
            }
        });
        merged.putAll(parseArgs(args));
        return new RawSettings(merged);
    }

    /** KAIZEN_POSTGRES_PASSWORD ↔ postgres.password, KAIZEN_BUSINESS_REALTIME_HOT_DAYS ↔ business_realtime.hot_days. */
    public static String envName(String section, String key) {
        return ENV_PREFIX + (section + "_" + key).toUpperCase(Locale.ROOT);
    }

    /**
     * Env name → section.key. The longest known section (from config.ini) wins, so sections may contain "_";
     * names of unknown sections split at the first "_".
     * Tên env → section.key. Section dài nhất có trong config.ini được chọn nên tên section được chứa "_";
     * section không có trong config.ini thì tách ở dấu "_" đầu tiên.
     */
    static String fromEnvName(String name, Set<String> sections) {
        if (!name.startsWith(ENV_PREFIX)) {
            return null;
        }
        String rest = name.substring(ENV_PREFIX.length()).toLowerCase(Locale.ROOT);
        String best = null;
        for (String s : sections) {
            if (rest.length() > s.length() + 1 && rest.startsWith(s + "_")
                    && (best == null || s.length() > best.length())) {
                best = s;
            }
        }
        if (best != null) {
            return best + "." + rest.substring(best.length() + 1);
        }
        int sep = rest.indexOf('_');
        if (sep <= 0 || sep == rest.length() - 1) {
            return null;
        }
        return rest.substring(0, sep) + "." + rest.substring(sep + 1);
    }

    /**
     * Accepts "--section.key value" and "--section.key=value".
     * Nhận "--section.key value" và "--section.key=value".
     */
    static Map<String, String> parseArgs(String[] args) {
        Map<String, String> out = new HashMap<>();
        if (args == null) {
            return out;
        }
        for (int i = 0; i < args.length; i++) {
            String arg = args[i];
            if (!arg.startsWith("--")) {
                throw new IllegalArgumentException("Unexpected argument: " + arg);
            }
            String body = arg.substring(2);
            int eq = body.indexOf('=');
            String key;
            String value;
            if (eq >= 0) {
                key = body.substring(0, eq);
                value = body.substring(eq + 1);
            } else {
                if (i + 1 >= args.length) {
                    throw new IllegalArgumentException("Missing value for --" + body);
                }
                key = body;
                value = args[++i];
            }
            // Require section.key form so typos are caught / Bắt buộc dạng section.key để bắt lỗi gõ sai
            if (key.indexOf('.') <= 0) {
                throw new IllegalArgumentException("Argument must be --section.key: --" + key);
            }
            out.put(key, value);
        }
        return out;
    }

    static Map<String, String> readIni(String resource) {
        Map<String, String> out = new HashMap<>();
        try (InputStream is = SettingsLoader.class.getClassLoader().getResourceAsStream(resource)) {
            // Missing file is allowed: env/args/defaults still apply / Thiếu file vẫn chạy được bằng env/args/mặc định
            if (is == null) {
                return out;
            }
            Ini ini = new Ini(is);
            for (Profile.Section section : ini.values()) {
                for (Map.Entry<String, String> entry : section.entrySet()) {
                    out.put(section.getName() + "." + entry.getKey(), entry.getValue());
                }
            }
            return out;
        } catch (IOException e) {
            throw new IllegalStateException("Cannot read " + resource, e);
        }
    }
}
