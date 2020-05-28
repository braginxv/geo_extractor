object GeoQueries {
  val countGeometries = "SELECT count(*) FROM grid_blobs;"

  val blobs = "SELECT grid_id, blob FROM grid_blobs WHERE grid_id BETWEEN -1732271230 AND -1732271223;"

  val insertBlob = "INSERT INTO grid_blobs(grid_id, blob) VALUES (?, ?);"
}
