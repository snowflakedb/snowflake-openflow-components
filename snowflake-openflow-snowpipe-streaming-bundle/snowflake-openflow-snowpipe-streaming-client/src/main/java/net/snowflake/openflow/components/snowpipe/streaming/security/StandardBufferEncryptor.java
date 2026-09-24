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

import java.security.GeneralSecurityException;
import java.security.NoSuchAlgorithmException;
import java.security.SecureRandom;
import java.util.Base64;
import javax.crypto.Cipher;
import javax.crypto.NoSuchPaddingException;
import javax.crypto.SecretKey;
import javax.crypto.spec.IvParameterSpec;
import javax.crypto.spec.SecretKeySpec;

/**
 * Standard Buffer Encryptor for AES-CTR with a key size of 256 bits and an initialization vector size of 128 bits
 */
public class StandardBufferEncryptor implements BufferEncryptor {

    private static final String CIPHER_ALGORITHM = "AES/CTR/NoPadding";

    private static final String KEY_ALGORITHM = "AES";

    private static final int INITIALIZATION_VECTOR_LENGTH = 16;

    private static final SecureRandom secureRandom = new SecureRandom();

    private static final Base64.Decoder decoder = Base64.getDecoder();

    private static final Base64.Encoder encoder = Base64.getEncoder();

    @Override
    public EncryptedBuffer encryptBuffer(final byte[] inputBuffer, final PipeEncryptionInfo pipeEncryptionInfo) {
        // Pipe Key expected length is 32 bytes
        final byte[] pipeKey = decoder.decode(pipeEncryptionInfo.pipeKey());
        final SecretKey secretKey = new SecretKeySpec(pipeKey, KEY_ALGORITHM);
        final IvParameterSpec ivParameterSpec = getIvParameterSpec();

        try {
            final Cipher cipher = getCipher();
            cipher.init(Cipher.ENCRYPT_MODE, secretKey, ivParameterSpec);

            final byte[] buffer = cipher.doFinal(inputBuffer);
            final String diversifier = pipeEncryptionInfo.diversifier();
            final String iv = encoder.encodeToString(ivParameterSpec.getIV());
            final String pipeKeyId = Long.toString(pipeEncryptionInfo.pipeKeyId());
            return new EncryptedBuffer(buffer, iv, diversifier, pipeKeyId, EncryptionVersion.AES_CTR_128_V1.getVersion());
        } catch (final GeneralSecurityException e) {
            throw new IllegalStateException("Encryption failed with algorithm [%s]".formatted(CIPHER_ALGORITHM), e);
        }
    }

    private IvParameterSpec getIvParameterSpec() {
        final byte[] initializationVector = new byte[INITIALIZATION_VECTOR_LENGTH];
        secureRandom.nextBytes(initializationVector);
        return new IvParameterSpec(initializationVector);
    }

    private Cipher getCipher() throws NoSuchPaddingException, NoSuchAlgorithmException {
        return Cipher.getInstance(CIPHER_ALGORITHM);
    }
}
