import com.tiny.flink.connector.hive.scala.hcatalog.HiveHCatalogTableSource
import org.apache.flink.table.api.Types
import org.junit.Test

class HiveHCatalogTableSourceTest {

  @Test
  def test(): Unit = {
    val hiveSource = HiveHCatalogTableSource
      .builder()
      .database("analytics")
      .metastoreUris("thrift://ip:10000")
      .table("tmp_person")
      .field("id", Types.INT)
      .field("age", Types.INT)
      .field("name", Types.STRING)
      .field("cdate", Types.STRING)
      .build()
    hiveSource.getDataSet(null);
  }
}
