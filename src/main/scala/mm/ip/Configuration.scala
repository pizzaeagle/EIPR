package mm.ip

import com.typesafe.config.Config

case class Configuration(
  sqlUrl: String,
  sqlDBTable: String,
  sqlDriver: String,
  user: String,
  password: String,
  esNodes: String,
  esIndex: String,
  numberOfGeneratedRanges: Int
)

object Configuration {

  def apply(config: Config): Configuration = {
    new Configuration(
      sqlUrl = config.getString("sqlUrl"),
      sqlDBTable = config.getString("sqlDBTable"),
      sqlDriver = config.getString("sqlDriver"),
      user = config.getString("username"),
      password = config.getString("password"),
      esNodes = config.getString("esNodes"),
      esIndex = config.getString("esIndex"),
      numberOfGeneratedRanges = config.getInt("numberOfGeneratedRanges")
    )
  }
}
