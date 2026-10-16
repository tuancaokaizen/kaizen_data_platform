package com.kaizen.stream.utils.event;

import java.nio.ByteBuffer;
import java.nio.charset.StandardCharsets;
import java.security.MessageDigest;
import java.security.NoSuchAlgorithmException;
import java.util.UUID;

/**
 * Deterministic event ids: UUIDv5(namespace, aggregateId|eventType|version).
 * eventId tất định: UUIDv5(namespace, aggregateId|eventType|version).
 */
public final class EventIds {

    /** Fixed Kaizen namespace; changing it changes every eventId. / Đổi giá trị này sẽ đổi mọi eventId. */
    public static final UUID NAMESPACE = UUID.fromString("6f1c3c52-3c1e-5a4b-9d7e-6b61697a656e");

    private EventIds() {
    }

    public static String of(String aggregateId, String eventType, long version) {
        return uuidV5(NAMESPACE, aggregateId + "|" + eventType + "|" + version).toString();
    }

    static UUID uuidV5(UUID namespace, String name) {
        MessageDigest sha1;
        try {
            sha1 = MessageDigest.getInstance("SHA-1");
        } catch (NoSuchAlgorithmException e) {
            throw new IllegalStateException("SHA-1 not available", e);
        }
        ByteBuffer ns = ByteBuffer.allocate(16)
                .putLong(namespace.getMostSignificantBits())
                .putLong(namespace.getLeastSignificantBits());
        sha1.update(ns.array());
        sha1.update(name.getBytes(StandardCharsets.UTF_8));
        byte[] hash = sha1.digest();

        // Set version 5 and IETF variant bits (RFC 4122 §4.3) / Đặt bit version 5 và variant theo RFC 4122
        hash[6] = (byte) ((hash[6] & 0x0f) | 0x50);
        hash[8] = (byte) ((hash[8] & 0x3f) | 0x80);

        ByteBuffer bb = ByteBuffer.wrap(hash, 0, 16);
        return new UUID(bb.getLong(), bb.getLong());
    }
}
