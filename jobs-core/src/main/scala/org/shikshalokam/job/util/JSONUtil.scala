package org.shikshalokam.job.util

import com.fasterxml.jackson.annotation.JsonInclude.Include
import com.fasterxml.jackson.core.JsonGenerator.Feature
import com.fasterxml.jackson.core.`type`.TypeReference
import com.fasterxml.jackson.core.json.JsonReadFeature
import com.fasterxml.jackson.databind.json.JsonMapper
import com.fasterxml.jackson.databind.{DeserializationFeature, SerializationFeature}
import com.fasterxml.jackson.module.scala.DefaultScalaModule

import java.lang.reflect.{ParameterizedType, Type}

object JSONUtil {

  @transient val mapper = JsonMapper.builder().enable(JsonReadFeature.ALLOW_BACKSLASH_ESCAPING_ANY_CHARACTER).build();
  mapper.registerModule(DefaultScalaModule)
  mapper.configure(DeserializationFeature.FAIL_ON_UNKNOWN_PROPERTIES, false)
  mapper.configure(SerializationFeature.FAIL_ON_EMPTY_BEANS, false)
  mapper.configure(Feature.WRITE_BIGDECIMAL_AS_PLAIN, true)
  mapper.setSerializationInclusion(Include.NON_NULL)

  @throws(classOf[Exception])
  def serialize(obj: AnyRef): String = {
    mapper.writeValueAsString(obj)
  }

  def deserialize[T: Manifest](json: String): T = {
    mapper.readValue(json, typeReference[T])
  }

  def deserialize[T: Manifest](json: Array[Byte]): T = {
    mapper.readValue(json, typeReference[T])
  }

  private[this] def typeReference[T: Manifest] = new TypeReference[T] {
    override def getType = typeFromManifest(manifest[T])
  }


  private[this] def typeFromManifest(m: Manifest[_]): Type = {
    if (m.typeArguments.isEmpty) { m.runtimeClass }
    else new ParameterizedType {
      def getRawType = m.runtimeClass
      def getActualTypeArguments = m.typeArguments.map(typeFromManifest).toArray
      def getOwnerType = null
    }
  }

}
