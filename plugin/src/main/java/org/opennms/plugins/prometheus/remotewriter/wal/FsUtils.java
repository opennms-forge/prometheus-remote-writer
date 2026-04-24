/*
 * Copyright 2026 The OpenNMS Group, Inc.
 * SPDX-License-Identifier: Apache-2.0
 *
 * Created by Ronny Trommer <ronny@opennms.com>
 */
package org.opennms.plugins.prometheus.remotewriter.wal;

import java.io.IOException;
import java.nio.channels.FileChannel;
import java.nio.file.Path;
import java.nio.file.StandardOpenOption;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * Filesystem helpers that the WAL uses to achieve real crash-durability
 * beyond what {@code FileChannel.force()} alone provides.
 *
 * <p>On POSIX file systems (Linux, macOS, *BSD), a rename / create /
 * unlink is not durable until the containing directory's metadata has
 * been fsynced. {@link FileChannel#force(boolean)} on the file only
 * persists the file's data + inode — the directory entry can still be
 * lost on crash, producing a "file exists on disk but isn't in any
 * directory" orphan (or, more insidiously, "rename completed but the
 * old name is still listed").
 *
 * <p>Windows does not support opening a directory as a
 * {@link FileChannel}, and its filesystem (NTFS/ReFS) does not expose
 * a directory-level fsync. On Windows, {@link #fsyncDirectory} is a
 * best-effort no-op — durability of create/rename is stronger by
 * default there.
 */
final class FsUtils {

    private static final Logger LOG = LoggerFactory.getLogger(FsUtils.class);

    private FsUtils() { /* utility */ }

    /**
     * Fsync the given directory so that any renames / creates / unlinks
     * that have been made visible in it become durable. Silent no-op on
     * Windows (where directory fsync is not supported and the rename
     * semantics are stronger by default).
     */
    static void fsyncDirectory(Path dir) throws IOException {
        try (FileChannel ch = FileChannel.open(dir, StandardOpenOption.READ)) {
            ch.force(true);
        } catch (IOException e) {
            // On Windows, opening a directory as READ-only returns an
            // AccessDeniedException wrapped as an IOException. Treat as
            // no-op; fall back to the stronger default durability.
            String osName = System.getProperty("os.name", "").toLowerCase();
            if (osName.startsWith("windows")) {
                LOG.debug("directory fsync skipped on Windows ({}): {}", dir, e.getMessage());
                return;
            }
            throw e;
        }
    }
}
