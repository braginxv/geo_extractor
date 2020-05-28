import java.nio.{ ByteBuffer, ByteOrder }
import java.sql.ResultSet
import scala.annotation.tailrec

case class Layer(id: Int, size: Int, models: List[Model])

case class Model(size: Short, raws: List[Raw])

case class Raw(geometryId: Long,
  provider: Byte,
  layerId: Long,
  geometryType: Byte,
  data: Array[Byte],
  gridId: Int = 0)

object Main {
  def main(args: Array[String]): Unit = {
    val statement = GeoConnection.seaLayerConnection.createStatement()

    if (statement.execute(GeoQueries.blobs)) {
      val resultSet = statement.getResultSet
      processQueryResult(resultSet)
      resultSet.close()
    } else {
      System.err.println("Cannot execute query")
      System.exit(1)
    }

    statement.close()
    GeoConnection.close()
  }

  private def processQueryResult(rs: ResultSet) {
    while (rs.next()) {
      val gridId = rs.getInt(1)
      val buffer = ByteBuffer.wrap(rs.getBytes(2)).order(ByteOrder.LITTLE_ENDIAN)
      println(s"gridId=${gridId}, blob size=${buffer.limit()}")

      @tailrec
      def getModels(layer: Int, restSize: Int, acc: List[Raw] = List.empty): List[Raw] =
        if (restSize > 0) {
          val zeroPosition = buffer.position()
          val id = buffer.getLong
          val provider = buffer.get
          val geometryType = buffer.get
          val blobSize = buffer.getShort
          val bytes = new Array[Byte](blobSize)
          buffer.get(bytes)
          getModels(layer, restSize - (buffer.position() - zeroPosition), Raw(id, provider, layer, geometryType, bytes, gridId) :: acc)
        } else {
          assert(restSize == 0)
          acc
        }

      @tailrec
      def getLayers(layer: Int, restSize: Int, acc: List[Model] = List.empty): List[Model] =
        if (restSize > 0) {
          val modelSize = buffer.getShort
          getLayers(layer, restSize - modelSize - 2, Model(modelSize, getModels(layer, modelSize)) :: acc)
        } else {
          assert(restSize == 0)
          acc
        }

      @tailrec
      def get(acc: List[Layer] = List.empty): List[Layer] =
        if (buffer.hasRemaining) {
          val layer = buffer.getInt
          val layerSize = buffer.getInt

          get(Layer(layer, layerSize, getLayers(layer, layerSize)) :: acc)
        } else {
          acc
        }

      get().foreach { layer =>
        println(s"layer {id: ${layer.id}, size: ${layer.size}}")
        layer.models.foreach { model =>
          println(s"model {size: ${model.size}}")
          model.raws.foreach { raw =>
            println(s"raw {gridId: ${raw.gridId}, geomId: ${raw.geometryId}, size: ${raw.data.length}}")
          }
        }
      }

//        insertBlob(gridId, get())
    }
  }

  private def insertBlob(gridId: Int, layers: List[Layer]): Unit = {
    val statement = GeoConnection.insertBlobStatement
    statement.setInt(1, gridId)
    val gridSize = layers.foldLeft(0)(_ + _.size) + layers.size * 8
    val buffer = ByteBuffer.allocate(gridSize).order(ByteOrder.LITTLE_ENDIAN)

    layers.foreach { layer =>
      buffer.putInt(layer.id)
      buffer.putInt(layer.size)

      layer.models.foreach { model =>
          buffer.putShort(model.size)

          model.raws.foreach { raw =>
            buffer.putLong(raw.geometryId)
            buffer.put(raw.provider)
            buffer.put(raw.geometryType)
            buffer.putShort(raw.data.length.toShort)
            buffer.put(raw.data)
          }
      }
    }

    assert(buffer.remaining() == 0)
    statement.setBytes(2, buffer.array())
    statement.executeUpdate()
  }

//  private def process(layers: List[Layer]): List[Layer] = {
//    layers
//      .filter(_.id == 47)
//      .map { layer =>
//        layer.copy(
//          id = 70708,
//          models = layer.models.copy(
//            map(_.raws.filter(_.geometryId == 1452497917))
//          )
//        )
//      }
//  }
}
