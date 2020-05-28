import java.sql.{ Connection, DriverManager }

object GeoConnection {
  val oldDbConnection: Connection = {
    val dbFile = "/home/explorer/hdd/project/db/world/world.grid.db"
    DriverManager.getConnection(s"jdbc:sqlite:$dbFile")
  }

  val seaLayerConnection: Connection = {
    val dbFile = "/home/explorer/hdd/project/db/world/sea-layer.db"
    DriverManager.getConnection(s"jdbc:sqlite:$dbFile")
  }

  val insertBlobStatement = seaLayerConnection.prepareStatement(GeoQueries.insertBlob)

  def close() {
    insertBlobStatement.close()
    oldDbConnection.close()
    seaLayerConnection.close()
  }
}
