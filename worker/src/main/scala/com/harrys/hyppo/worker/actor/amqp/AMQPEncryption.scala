package com.harrys.hyppo.worker.actor.amqp

import java.security.SecureRandom
import javax.crypto.spec.{GCMParameterSpec, PBEKeySpec, SecretKeySpec}
import javax.crypto.{Cipher, SecretKeyFactory}

import org.apache.commons.codec.digest.DigestUtils

/**
  * Created by jpetty on 11/12/15.
  */
object AMQPEncryption {

  private final val AESKeyBits    = 128
  private final val IVBytes       = 16
  private final val GCMTagBits    = 96
  private final val PBKDF2Rounds  = 2048

  def initializeEncryptCipher(key: SecretKeySpec) : Cipher = {
    val cipher  = Cipher.getInstance("AES/GCM/NoPadding")
    val random  = new SecureRandom()
    val ivBytes = new Array[Byte](IVBytes)
    random.nextBytes(ivBytes)
    cipher.init(Cipher.ENCRYPT_MODE, key, new GCMParameterSpec(GCMTagBits, ivBytes))
    cipher
  }

  def initializeDecryptCipher(key: SecretKeySpec, iv: Array[Byte]) : Cipher = {
    val cipher  = Cipher.getInstance("AES/GCM/NoPadding")
    if (iv.length != IVBytes){
      throw new IllegalArgumentException(s"IV provided is not the correct size. Required: ${ IVBytes } Found: ${ iv.length }")
    }
    cipher.init(Cipher.DECRYPT_MODE, key, new GCMParameterSpec(GCMTagBits, iv))
    cipher
  }

  def encryptWithSecret(key: SecretKeySpec, bytes: Array[Byte]) : Array[Byte] = {
    val cipher = initializeEncryptCipher(key)
    cipher.getIV ++ cipher.doFinal(bytes)
  }

  def decryptWithSecret(key: SecretKeySpec, bytes: Array[Byte]) : Array[Byte] = {
    if (bytes.length <= IVBytes){
      throw new IllegalArgumentException(s"Invalid encrypted payload size. Must minimally be more than the IV size of ${ IVBytes }. Found: ${ bytes.length }")
    }
    val (ivBytes, cipherBytes) = bytes.splitAt(IVBytes)
    val cipher  = initializeDecryptCipher(key, ivBytes)
    cipher.doFinal(cipherBytes)
  }


  def initializeKeyFromSecret(secret: String) : SecretKeySpec = {
    if (secret.isEmpty){
      throw new IllegalArgumentException("key secret must not be empty!")
    }
    val keyFactory  = SecretKeyFactory.getInstance("PBKDF2WithHmacSHA1")
    val saltValue   = DigestUtils.sha256(this.getClass.getCanonicalName + secret)
    val secretKey   = keyFactory.generateSecret(new PBEKeySpec(secret.toCharArray, saltValue, PBKDF2Rounds, AESKeyBits))
    new SecretKeySpec(secretKey.getEncoded, "AES")
  }
}
