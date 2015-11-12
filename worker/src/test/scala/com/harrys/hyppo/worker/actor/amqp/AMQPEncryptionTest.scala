package com.harrys.hyppo.worker.actor.amqp

import com.harrys.hyppo.worker.TestConfig
import org.apache.commons.io.Charsets
import org.scalatest.{Matchers, WordSpecLike}

/**
  * Created by jpetty on 11/12/15.
  */
class AMQPEncryptionTest extends WordSpecLike with Matchers {

  val config = TestConfig.workerWithRandomQueuePrefix()

  "The AMQP Encryptor" must {
    "encrypt and decrypt to the same message" in {
      val plaintext = "test".getBytes(Charsets.UTF_8)
      val encrypted = AMQPEncryption.encryptWithSecret(config.secretKey, plaintext)
      val decrypted = AMQPEncryption.decryptWithSecret(config.secretKey, encrypted)
      plaintext shouldEqual decrypted
    }
  }

}
