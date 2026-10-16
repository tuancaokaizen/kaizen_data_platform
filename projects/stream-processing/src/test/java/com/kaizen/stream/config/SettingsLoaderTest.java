package com.kaizen.stream.config;

import org.junit.jupiter.api.Test;

import java.util.Arrays;
import java.util.Collections;
import java.util.HashMap;
import java.util.HashSet;
import java.util.Map;
import java.util.Set;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertNull;
import static org.junit.jupiter.api.Assertions.assertThrows;
import static org.junit.jupiter.api.Assertions.assertTrue;

class SettingsLoaderTest {

    private static Map<String, String> ini() {
        Map<String, String> ini = new HashMap<>();
        ini.put("postgres.host", "pg-from-ini");
        ini.put("postgres.database", "information_system");
        ini.put("postgres.username", "postgres");
        ini.put("flink.parallelism", "3");
        return ini;
    }

    @Test
    void argsBeatEnvBeatIni() {
        Map<String, String> env = new HashMap<>();
        env.put("KAIZEN_POSTGRES_HOST", "pg-from-env");
        env.put("KAIZEN_FLINK_PARALLELISM", "2");
        env.put("PATH", "/usr/bin");

        RawSettings raw = SettingsLoader.merge(ini(), env, new String[]{"--flink.parallelism", "1"});

        assertEquals("pg-from-env", raw.get("postgres", "host"));
        assertEquals(1, raw.getInt("flink", "parallelism", 9));
        assertEquals("information_system", raw.get("postgres", "database"));
        assertNull(raw.get("path", ""));
    }

    @Test
    void envNamesKeepUnderscoresInKey() {
        Set<String> none = Collections.emptySet();
        assertEquals("clickhouse.batch_size", SettingsLoader.fromEnvName("KAIZEN_CLICKHOUSE_BATCH_SIZE", none));
        assertEquals("KAIZEN_POSTGRES_PASSWORD", SettingsLoader.envName("postgres", "password"));
        assertNull(SettingsLoader.fromEnvName("KAIZEN_ONLYSECTION", none));
        assertNull(SettingsLoader.fromEnvName("HOME", none));
    }

    @Test
    void envNamesResolveSectionsWithUnderscores() {
        Set<String> sections = new HashSet<>(Arrays.asList("business", "business_realtime", "supply_chain_realtime"));
        assertEquals("business_realtime.hot_days",
                SettingsLoader.fromEnvName("KAIZEN_BUSINESS_REALTIME_HOT_DAYS", sections));
        assertEquals("supply_chain_realtime.batch_size",
                SettingsLoader.fromEnvName("KAIZEN_SUPPLY_CHAIN_REALTIME_BATCH_SIZE", sections));

        Map<String, String> ini = ini();
        ini.put("business_realtime.hot_days", "14");
        Map<String, String> env = new HashMap<>();
        env.put("KAIZEN_BUSINESS_REALTIME_HOT_DAYS", "7");
        RawSettings raw = SettingsLoader.merge(ini, env, new String[0]);
        assertEquals(7, raw.getInt("business_realtime", "hot_days", 0));
    }

    @Test
    void argsAcceptBothForms() {
        Map<String, String> args = SettingsLoader.parseArgs(new String[]{"--a.b=1", "--c.d", "2"});
        assertEquals("1", args.get("a.b"));
        assertEquals("2", args.get("c.d"));
        assertThrows(IllegalArgumentException.class, () -> SettingsLoader.parseArgs(new String[]{"--nosection", "x"}));
        assertThrows(IllegalArgumentException.class, () -> SettingsLoader.parseArgs(new String[]{"--a.b"}));
        assertThrows(IllegalArgumentException.class, () -> SettingsLoader.parseArgs(new String[]{"stray"}));
    }

    @Test
    void missingPasswordNamesTheEnvVar() {
        StreamSettings settings = new StreamSettings(SettingsLoader.merge(ini(), new HashMap<>(), new String[0]));
        IllegalStateException e = assertThrows(IllegalStateException.class, settings::postgres);
        assertTrue(e.getMessage().contains("KAIZEN_POSTGRES_PASSWORD"), e.getMessage());
    }

    @Test
    void groupsAreValidatedOnlyWhenUsed() {
        StreamSettings settings = new StreamSettings(SettingsLoader.merge(ini(), new HashMap<>(), new String[0]));
        assertEquals(3, settings.flink().getParallelism());
        assertEquals(3, settings.flink().getSourceParallelism());
    }

    @Test
    void passwordsNeverAppearInToString() {
        Map<String, String> env = new HashMap<>();
        env.put("KAIZEN_POSTGRES_PASSWORD", "s3cr3t-value");
        StreamSettings settings = new StreamSettings(SettingsLoader.merge(ini(), env, new String[0]));

        assertEquals("s3cr3t-value", settings.postgres().getPassword());
        assertFalse(settings.postgres().toString().contains("s3cr3t-value"));
        assertFalse(settings.toString().contains("s3cr3t-value"));
    }

    @Test
    void badNumberFailsLoudly() {
        RawSettings raw = SettingsLoader.merge(ini(), new HashMap<>(), new String[]{"--flink.parallelism", "three"});
        assertThrows(IllegalStateException.class, () -> raw.getInt("flink", "parallelism", 1));
    }
}
