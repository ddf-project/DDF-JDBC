package io.ddf.aws

import io.ddf.DDFManager.EngineType
import io.ddf.datasource.DataSourceDescriptor
import io.ddf.datasource.JDBCDataSourceDescriptor
import io.ddf.exception.DDFException
import io.ddf.misc.Config
import io.ddf.postgres.PostgresDDFManager

import scala.collection.JavaConverters._

class AWSDDFManager(dataSourceDescriptor: JDBCDataSourceDescriptor, engineType: EngineType) extends PostgresDDFManager(dataSourceDescriptor, engineType) {
  override def getEngine = engineType.name()

  val configurationDataSourceDescriptor = dataSourceDescriptor.asInstanceOf[ExtJDBCDataSourceDescriptor]
  val credentials = configurationDataSourceDescriptor.getCredentials
  val passedConfig = configurationDataSourceDescriptor.getRuntimeConfiguration
  val staticConfig = Config.getConfigHandler.getSettings(getEngine)
  val runtimeConfig = getRuntimeConfig

  def getRuntimeConfig = {
    val map = new java.util.HashMap[String, String]
    map.putAll(staticConfig)
    map.putAll(passedConfig)
    map.asScala
  }

  def getRequiredValue(key: String = "dummy") = {
    runtimeConfig.get(key.toLowerCase) match {
      case Some(str) => str
      case None => throw new DDFException(s"Required property $key is missing")
    }
  }

}
