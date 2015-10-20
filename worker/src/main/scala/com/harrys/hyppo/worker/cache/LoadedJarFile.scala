package com.harrys.hyppo.worker.cache

import java.io.File

import com.harrys.hyppo.worker.api.code.IntegrationJarFile

/**
 * Created by jpetty on 7/30/15.
 */
final case class LoadedJarFile(key: IntegrationJarFile, @transient file: File) extends Serializable
