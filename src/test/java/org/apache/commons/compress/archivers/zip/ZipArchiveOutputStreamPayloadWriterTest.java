/*
 * Licensed to the Apache Software Foundation (ASF) under one
 * or more contributor license agreements.  See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership.  The ASF licenses this file
 * to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License.  You may obtain a copy of the License at
 *
 *   https://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing,
 * software distributed under the License is distributed on an
 * "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY
 * KIND, either express or implied.  See the License for the
 * specific language governing permissions and limitations
 * under the License.
 */
package org.apache.commons.compress.archivers.zip;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertThrows;
import static org.junit.jupiter.api.Assertions.assertTrue;

import java.io.ByteArrayInputStream;
import java.io.ByteArrayOutputStream;
import java.io.IOException;
import java.io.InputStream;
import java.io.OutputStream;
import java.nio.channels.SeekableByteChannel;
import java.nio.charset.StandardCharsets;
import java.nio.file.Files;
import java.nio.file.Path;

import java.util.zip.CRC32;

import org.apache.commons.compress.archivers.ArchiveException;
import org.apache.commons.compress.compressors.bzip2.BZip2CompressorInputStream;
import org.apache.commons.compress.compressors.xz.XZCompressorInputStream;
import org.apache.commons.compress.compressors.xz.XZCompressorOutputStream;
import org.apache.commons.compress.compressors.zstandard.ZstdCompressorInputStream;
import org.apache.commons.compress.compressors.zstandard.ZstdCompressorOutputStream;
import org.apache.commons.compress.compressors.zstandard.ZstdUtils;
import org.apache.commons.compress.utils.SeekableInMemoryByteChannel;
import org.apache.commons.io.IOUtils;
import org.junit.jupiter.api.Assumptions;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.io.TempDir;
import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.EnumSource;
import org.junit.jupiter.params.provider.ValueSource;

import io.airlift.compress.zstd.ZstdOutputStream;

/**
 * Regression and feature tests for {@link ZipArchiveOutputStream#setCompressionPayloadWriterFactory}, data descriptors for Zstandard/XZ on non-seekable outputs,
 * and {@link ZipArchiveOutputStream#finish()}/{@link ZipArchiveOutputStream#close()} behavior.
 */
class ZipArchiveOutputStreamPayloadWriterTest {

    @TempDir
    Path tempDir;

    private static final int ZSTD_LEVEL = 3;
    private static final int XZ_LEVEL = 6;

    /**
     * Airlift {@link ZstdOutputStream} as entry payload compressor; frames must remain decodable by {@link ZstdCompressorInputStream}.
     */
    private static final ZipCompressionPayloadWriterFactory AIRLIFT_ZSTD_PAYLOAD_WRITER_FACTORY =
            (compressedPayloadSink, entry) -> {
                final ZstdOutputStream zOut = new ZstdOutputStream(nonClosingPayloadSink(compressedPayloadSink));
                return new ZipCompressionPayloadWriter() {
                    @Override
                    public void write(final byte[] b, final int off, final int len) throws IOException {
                        if (len > 0) {
                            zOut.write(b, off, len);
                        }
                    }

                    @Override
                    public void finish() throws IOException {
                        zOut.close();
                    }
                };
            };

    private static OutputStream nonClosingPayloadSink(final OutputStream delegate) {
        return new OutputStream() {
            @Override
            public void write(final int b) throws IOException {
                delegate.write(b);
            }

            @Override
            public void write(final byte[] b, final int off, final int len) throws IOException {
                delegate.write(b, off, len);
            }

            @Override
            public void flush() throws IOException {
                delegate.flush();
            }

            @Override
            public void close() {
                // Never close the ZIP stream: only airlift ZstdOutputStream.finish/close ends the frame.
            }
        };
    }

    private static void compressZstd(final InputStream input, final OutputStream output) throws IOException {
        @SuppressWarnings("resource")
        final ZstdCompressorOutputStream zOut = new ZstdCompressorOutputStream(output, ZSTD_LEVEL, true);
        IOUtils.copyLarge(input, zOut);
        zOut.flush();
    }

    private static void compressXz(final InputStream input, final OutputStream output) throws IOException {
        @SuppressWarnings("resource")
        final XZCompressorOutputStream xzOut = new XZCompressorOutputStream(output, XZ_LEVEL);
        IOUtils.copyLarge(input, xzOut);
        xzOut.flush();
        xzOut.finish();
    }

    /** DEFLATED without closing the entry: {@link ZipArchiveOutputStream#finish()} must still fail (unchanged contract). */
    @Test
    void testDeflatedFinishWithOpenEntryThrows() throws IOException {
        final ByteArrayOutputStream raw = new ByteArrayOutputStream();
        final ZipArchiveOutputStream zos = new ZipArchiveOutputStream(raw);
        try {
            final ZipArchiveEntry ze = new ZipArchiveEntry("open.txt");
            ze.setMethod(ZipArchiveOutputStream.DEFLATED);
            zos.putArchiveEntry(ze);
            zos.write('x');
            final ArchiveException ex = assertThrows(ArchiveException.class, zos::finish);
            assertTrue(ex.getMessage().contains("unclosed"));
        } finally {
            zos.destroy();
        }
    }

    /** DEFLATED: implicit {@link ZipArchiveOutputStream#close()} still requires an explicit {@link ZipArchiveOutputStream#closeArchiveEntry()}. */
    @Test
    void testDeflatedTryWithResourcesWithOpenEntryThrows() {
        final ByteArrayOutputStream raw = new ByteArrayOutputStream();
        assertThrows(ArchiveException.class, () -> {
            try (ZipArchiveOutputStream zos = new ZipArchiveOutputStream(raw)) {
                final ZipArchiveEntry ze = new ZipArchiveEntry("open.txt");
                ze.setMethod(ZipArchiveOutputStream.DEFLATED);
                zos.putArchiveEntry(ze);
                zos.write("data".getBytes(StandardCharsets.UTF_8));
            }
        });
    }

    /**
     * Legacy ZSTD path: no payload factory, pre-compressed bytes, {@link ZipMethod#ZSTD} on a non-seekable stream: archive must round-trip and local metadata
     * must use a data descriptor (CRC/sizes after payload).
     */
    @ParameterizedTest
    @EnumSource(names = { "ZSTD", "ZSTD_DEPRECATED" })
    void testZstdPassthroughOnByteArrayOutputStreamStillWorks(final ZipMethod zipMethod) throws IOException {
        Assumptions.assumeTrue(ZstdUtils.isZstdCompressionAvailable());
        final String name = "legacy.txt";
        final byte[] plain = "legacy passthrough zstd".getBytes(StandardCharsets.UTF_8);
        final ByteArrayOutputStream raw = new ByteArrayOutputStream();
        try (ZipArchiveOutputStream zos = new ZipArchiveOutputStream(raw)) {
            final ZipArchiveEntry ze = new ZipArchiveEntry(name);
            ze.setMethod(zipMethod.getCode());
            ze.setSize(plain.length);
            zos.putArchiveEntry(ze);
            assertTrue(ze.getGeneralPurposeBit().usesDataDescriptor(), "non-seekable ZSTD must defer CRC/sizes");
            compressZstd(new ByteArrayInputStream(plain), zos);
            zos.closeArchiveEntry();
        }
        try (SeekableByteChannel ch = new SeekableInMemoryByteChannel(raw.toByteArray());
                ZipFile zf = ZipFile.builder().setSeekableByteChannel(ch).get()) {
            final ZipArchiveEntry e = zf.getEntry(name);
            assertEquals(zipMethod.getCode(), e.getMethod());
            assertTrue(e.getGeneralPurposeBit().usesDataDescriptor());
            assertEquals(plain.length, e.getSize());
            try (InputStream in = zf.getInputStream(e)) {
                assertTrue(in instanceof ZstdCompressorInputStream);
                assertEquals(new String(plain, StandardCharsets.UTF_8), new String(IOUtils.toByteArray(in), StandardCharsets.UTF_8));
            }
        }
    }

    /**
     * Legacy XZ path on a non-{@link org.apache.commons.compress.archivers.zip.RandomAccessOutputStream} sink (plain {@link java.io.OutputStream} to a file): same
     * data-descriptor behavior as Zstandard, archive must round-trip.
     */
    @Test
    void testXzPassthroughOnNonSeekableStreamStillWorks() throws IOException {
        final String name = "xz-legacy.txt";
        final byte[] plain = "legacy passthrough xz".getBytes(StandardCharsets.UTF_8);
        final Path zipFile = tempDir.resolve("xz-nonseek.zip");
        try (ZipArchiveOutputStream zos = new ZipArchiveOutputStream(Files.newOutputStream(zipFile))) {
            assertFalse(zos.isSeekable());
            final ZipArchiveEntry ze = new ZipArchiveEntry(name);
            ze.setMethod(ZipMethod.XZ.getCode());
            ze.setSize(plain.length);
            zos.putArchiveEntry(ze);
            assertTrue(ze.getGeneralPurposeBit().usesDataDescriptor());
            compressXz(new ByteArrayInputStream(plain), zos);
            zos.closeArchiveEntry();
        }
        try (ZipFile zf = ZipFile.builder().setPath(zipFile).get()) {
            final ZipArchiveEntry e = zf.getEntry(name);
            assertEquals(ZipMethod.XZ.getCode(), e.getMethod());
            assertTrue(e.getGeneralPurposeBit().usesDataDescriptor());
            try (InputStream in = zf.getInputStream(e)) {
                assertTrue(in instanceof XZCompressorInputStream);
                assertEquals(new String(plain, StandardCharsets.UTF_8), new String(IOUtils.toByteArray(in), StandardCharsets.UTF_8));
            }
        }
    }

    /**
     * Two ZSTD payload entries without explicit {@link ZipArchiveOutputStream#closeArchiveEntry()} for the first: the second {@link
     * ZipArchiveOutputStream#putArchiveEntry} closes the previous entry (existing zip behaviour, independent of {@link ZipCompressionPayloadWriterFactory}). The
     * last entry is closed by stream {@link ZipArchiveOutputStream#close()}.
     */
    @ParameterizedTest
    @EnumSource(names = { "ZSTD", "ZSTD_DEPRECATED" })
    void testZstdPayloadWriterTwoEntries(final ZipMethod zipMethod) throws IOException {
        Assumptions.assumeTrue(ZstdUtils.isZstdCompressionAvailable());
        final ByteArrayOutputStream raw = new ByteArrayOutputStream();
        final byte[] a = "entry-a".getBytes(StandardCharsets.UTF_8);
        final byte[] b = "entry-b-longer".getBytes(StandardCharsets.UTF_8);
        try (ZipArchiveOutputStream zos = new ZipArchiveOutputStream(raw)) {
            zos.setCompressionPayloadWriterFactory(zipMethod.getCode(), ZipCompressionPayloadWriters.zstd(ZSTD_LEVEL));
            final ZipArchiveEntry e1 = new ZipArchiveEntry("a.txt");
            e1.setMethod(zipMethod.getCode());
            zos.putArchiveEntry(e1);
            zos.write(a);

            final ZipArchiveEntry e2 = new ZipArchiveEntry("b.txt");
            e2.setMethod(zipMethod.getCode());
            zos.putArchiveEntry(e2);
            zos.write(b);
        }
        try (SeekableByteChannel ch = new SeekableInMemoryByteChannel(raw.toByteArray());
                ZipFile zf = ZipFile.builder().setSeekableByteChannel(ch).get()) {
            final ZipArchiveEntry ra = zf.getEntry("a.txt");
            final ZipArchiveEntry rb = zf.getEntry("b.txt");
            assertEquals(a.length, ra.getSize());
            assertEquals(b.length, rb.getSize());
            try (InputStream in = zf.getInputStream(ra)) {
                assertEquals("entry-a", new String(IOUtils.toByteArray(in), StandardCharsets.UTF_8));
            }
            try (InputStream in = zf.getInputStream(rb)) {
                assertEquals("entry-b-longer", new String(IOUtils.toByteArray(in), StandardCharsets.UTF_8));
            }
        }
    }

    /** Removing the factory allows the next entry to use external ZSTD compression again. */
    @ParameterizedTest
    @ValueSource(ints = { 93 /* ZipMethod.ZSTD */, 20 /* ZipMethod.ZSTD_DEPRECATED */ })
    void testClearPayloadWriterFactoryRestoresExternalCompression(final int zipMethodCode) throws IOException {
        Assumptions.assumeTrue(ZstdUtils.isZstdCompressionAvailable());
        final String payloadName = "wrapped.txt";
        final byte[] plain = "external zstd after clear".getBytes(StandardCharsets.UTF_8);

        final ByteArrayOutputStream raw = new ByteArrayOutputStream();
        try (ZipArchiveOutputStream zos = new ZipArchiveOutputStream(raw)) {
            zos.setCompressionPayloadWriterFactory(zipMethodCode, ZipCompressionPayloadWriters.zstd(ZSTD_LEVEL));
            final ZipArchiveEntry e1 = new ZipArchiveEntry("inner.txt");
            e1.setMethod(zipMethodCode);
            zos.putArchiveEntry(e1);
            zos.write("inner".getBytes(StandardCharsets.UTF_8));
            zos.closeArchiveEntry();

            zos.setCompressionPayloadWriterFactory(zipMethodCode, null);

            final ZipArchiveEntry e2 = new ZipArchiveEntry(payloadName);
            e2.setMethod(zipMethodCode);
            e2.setSize(plain.length);
            zos.putArchiveEntry(e2);
            compressZstd(new ByteArrayInputStream(plain), zos);
            zos.closeArchiveEntry();
        }

        try (SeekableByteChannel ch = new SeekableInMemoryByteChannel(raw.toByteArray());
                ZipFile zf = ZipFile.builder().setSeekableByteChannel(ch).get()) {
            final ZipArchiveEntry e = zf.getEntry(payloadName);
            assertEquals(zipMethodCode, e.getMethod());
            assertEquals(plain.length, e.getSize());
            try (InputStream in = zf.getInputStream(e)) {
                assertEquals(new String(plain, StandardCharsets.UTF_8), new String(IOUtils.toByteArray(in), StandardCharsets.UTF_8));
            }
        }
    }

    /**
     * {@link ZipCompressionPayloadWriter} backed by Airlift {@link ZstdOutputStream}; default {@link ZipFile} decoding uses zstd-jni — verifies bitstream
     * compatibility for ZIP entries.
     */
    @ParameterizedTest
    @EnumSource(names = { "ZSTD", "ZSTD_DEPRECATED" })
    void testZstdPayloadWriterAirliftCompressorRoundtrip(final ZipMethod zipMethod) throws IOException {
        Assumptions.assumeTrue(ZstdUtils.isZstdCompressionAvailable());
        final String entryName = "airlift.txt";
        final byte[] plain = "Airlift aircompressor ZstdOutputStream in ZIP.".getBytes(StandardCharsets.UTF_8);

        final ByteArrayOutputStream rawOut = new ByteArrayOutputStream();
        try (ZipArchiveOutputStream zos = new ZipArchiveOutputStream(rawOut)) {
            zos.setCompressionPayloadWriterFactory(zipMethod.getCode(), AIRLIFT_ZSTD_PAYLOAD_WRITER_FACTORY);
            final ZipArchiveEntry ze = new ZipArchiveEntry(entryName);
            ze.setMethod(zipMethod.getCode());
            zos.putArchiveEntry(ze);
            zos.write(plain);
        }

        try (SeekableByteChannel ch = new SeekableInMemoryByteChannel(rawOut.toByteArray());
                ZipFile zf = ZipFile.builder().setSeekableByteChannel(ch).get()) {
            final ZipArchiveEntry e = zf.getEntry(entryName);
            assertEquals(zipMethod.getCode(), e.getMethod());
            assertEquals(plain.length, e.getSize());
            try (InputStream in = zf.getInputStream(e)) {
                assertTrue(in instanceof ZstdCompressorInputStream, "ZipFile decodes ZSTD with zstd-jni");
                assertEquals(new String(plain, StandardCharsets.UTF_8), new String(IOUtils.toByteArray(in), StandardCharsets.UTF_8));
            }
        }
    }

    /** Empty entry body with payload writer: {@link ZipArchiveOutputStream#close()} must still produce a valid archive. */
    @Test
    void testZstdPayloadWriterEmptyEntry() throws IOException {
        Assumptions.assumeTrue(ZstdUtils.isZstdCompressionAvailable());
        final ByteArrayOutputStream raw = new ByteArrayOutputStream();
        try (ZipArchiveOutputStream zos = new ZipArchiveOutputStream(raw)) {
            zos.setCompressionPayloadWriterFactory(ZipMethod.ZSTD.getCode(), ZipCompressionPayloadWriters.zstd(ZSTD_LEVEL));
            final ZipArchiveEntry ze = new ZipArchiveEntry("empty.txt");
            ze.setMethod(ZipMethod.ZSTD.getCode());
            zos.putArchiveEntry(ze);
        }
        try (SeekableByteChannel ch = new SeekableInMemoryByteChannel(raw.toByteArray());
                ZipFile zf = ZipFile.builder().setSeekableByteChannel(ch).get()) {
            final ZipArchiveEntry e = zf.getEntry("empty.txt");
            assertEquals(0L, e.getSize());
            try (InputStream in = zf.getInputStream(e)) {
                assertEquals(0, IOUtils.toByteArray(in).length);
            }
        }
    }

    /**
     * BZip2 via payload writer on non-seekable output: same streaming metadata path as ZSTD without hard-coding BZIP2 in the writer.
     */
    @Test
    void testBzip2PayloadWriterNonSeekableRoundtrip() throws IOException {
        final ByteArrayOutputStream raw = new ByteArrayOutputStream();
        final byte[] plain = "bzip2 payload writer unknown size".getBytes(StandardCharsets.UTF_8);
        try (ZipArchiveOutputStream zos = new ZipArchiveOutputStream(raw)) {
            zos.setCompressionPayloadWriterFactory(ZipMethod.BZIP2.getCode(), ZipCompressionPayloadWriters.bzip2());
            final ZipArchiveEntry ze = new ZipArchiveEntry("b2.txt");
            ze.setMethod(ZipMethod.BZIP2.getCode());
            zos.putArchiveEntry(ze);
            assertTrue(ze.getGeneralPurposeBit().usesDataDescriptor());
            zos.write(plain);
        }
        try (SeekableByteChannel ch = new SeekableInMemoryByteChannel(raw.toByteArray());
                ZipFile zf = ZipFile.builder().setSeekableByteChannel(ch).get()) {
            final ZipArchiveEntry e = zf.getEntry("b2.txt");
            assertEquals(ZipMethod.BZIP2.getCode(), e.getMethod());
            assertEquals(plain.length, e.getSize());
            try (InputStream in = zf.getInputStream(e)) {
                assertTrue(in instanceof BZip2CompressorInputStream);
                assertEquals(new String(plain, StandardCharsets.UTF_8), new String(IOUtils.toByteArray(in), StandardCharsets.UTF_8));
            }
        }
    }

    /**
     * Non-enum method code with a registered factory: data descriptor and CRC/size from plaintext; reading remains unsupported in {@link ZipFile}.
     */
    @Test
    void testCustomMethodCodePayloadWriterUsesDataDescriptorAndMetadata() throws IOException {
        final int customMethod = 254;
        final ZipCompressionPayloadWriterFactory identity = (sink, ze) -> new ZipCompressionPayloadWriter() {
            @Override
            public void write(final byte[] b, final int off, final int len) throws IOException {
                sink.write(b, off, len);
            }

            @Override
            public void finish() {
                // identity "compression" — no trailer
            }
        };
        final byte[] plain = "future-proof custom zip method code".getBytes(StandardCharsets.UTF_8);
        final CRC32 expectCrc = new CRC32();
        expectCrc.update(plain);
        final ByteArrayOutputStream raw = new ByteArrayOutputStream();
        try (ZipArchiveOutputStream zos = new ZipArchiveOutputStream(raw)) {
            zos.setCompressionPayloadWriterFactory(customMethod, identity);
            final ZipArchiveEntry ze = new ZipArchiveEntry("custom.txt");
            ze.setMethod(customMethod);
            zos.putArchiveEntry(ze);
            assertTrue(ze.getGeneralPurposeBit().usesDataDescriptor());
            zos.write(plain);
        }
        try (SeekableByteChannel ch = new SeekableInMemoryByteChannel(raw.toByteArray());
                ZipFile zf = ZipFile.builder().setSeekableByteChannel(ch).get()) {
            final ZipArchiveEntry e = zf.getEntry("custom.txt");
            assertEquals(customMethod, e.getMethod());
            assertEquals(plain.length, e.getSize());
            assertTrue(e.getGeneralPurposeBit().usesDataDescriptor());
            assertEquals(expectCrc.getValue(), e.getCrc());
            assertThrows(UnsupportedZipFeatureException.class, () -> zf.getInputStream(e));
        }
    }
}
