/*
 * Copyright 2026 Snowflake Inc.
 * SPDX-License-Identifier: Apache-2.0
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 * http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package net.snowflake.openflow.components.snowpipe.streaming.security;

import net.snowflake.openflow.components.snowpipe.streaming.pipe.PipeEncryptionInfo;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;

import java.nio.charset.StandardCharsets;
import java.security.GeneralSecurityException;
import java.security.SecureRandom;
import java.util.Base64;
import javax.crypto.Cipher;
import javax.crypto.SecretKey;
import javax.crypto.spec.IvParameterSpec;
import javax.crypto.spec.SecretKeySpec;

import static org.junit.jupiter.api.Assertions.assertArrayEquals;
import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertNotNull;

class StandardBufferEncryptorTest {

    private static final String CIPHER_ALGORITHM = "AES/CTR/NoPadding";

    private static final String KEY_ALGORITHM = "AES";

    private static final String INPUT = String.class.getName();

    private static final long PIPE_KEY_ID = 1;

    private static final int PIPE_KEY_LENGTH = 16;

    private static final String DIVERSIFIER = "0102030405060708";

    private static final Base64.Encoder encoder = Base64.getEncoder();

    private StandardBufferEncryptor encryptor;

    @BeforeEach
    void setEncryptor() {
        encryptor = new StandardBufferEncryptor();
    }

    @Test
    void testEncryptBuffer() throws GeneralSecurityException {
        final byte[] inputBuffer = INPUT.getBytes(StandardCharsets.UTF_8);

        final SecureRandom secureRandom = new SecureRandom();
        final byte[] randomKey = new byte[PIPE_KEY_LENGTH];
        secureRandom.nextBytes(randomKey);
        final String pipeKey = encoder.encodeToString(randomKey);

        final EncryptionVersion encryptionVersion = EncryptionVersion.AES_CTR_128_V1;
        final PipeEncryptionInfo pipeEncryptionInfo = new PipeEncryptionInfo(pipeKey, PIPE_KEY_ID, DIVERSIFIER, encryptionVersion.getVersion());

        final EncryptedBuffer encryptedBuffer = encryptor.encryptBuffer(inputBuffer, pipeEncryptionInfo);

        assertNotNull(encryptedBuffer);
        assertNotNull(encryptedBuffer.buffer());
        assertNotNull(encryptedBuffer.iv());
        assertEquals(DIVERSIFIER, encryptedBuffer.diversifier());
        assertEquals(Long.toString(PIPE_KEY_ID), encryptedBuffer.pipeKeyId());
        assertEquals(encryptionVersion.getVersion(), encryptedBuffer.encryptionVersion());

        final SecretKey secretKey = new SecretKeySpec(randomKey, KEY_ALGORITHM);
        assertDecryptedEquals(secretKey, inputBuffer, encryptedBuffer);
    }

    private void assertDecryptedEquals(final SecretKey secretKey, final byte[] inputBuffer, final EncryptedBuffer encryptedBuffer) throws GeneralSecurityException {
        final Cipher cipher = Cipher.getInstance(CIPHER_ALGORITHM);

        final byte[] iv = Base64.getDecoder().decode(encryptedBuffer.iv());
        final IvParameterSpec ivParameterSpec = new IvParameterSpec(iv);
        cipher.init(Cipher.DECRYPT_MODE, secretKey, ivParameterSpec);

        final byte[] decrypted = cipher.doFinal(encryptedBuffer.buffer());
        assertArrayEquals(inputBuffer, decrypted);
    }
}
