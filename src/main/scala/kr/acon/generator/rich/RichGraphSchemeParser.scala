package kr.acon.generator.rich

import org.w3c.dom.Element

import javax.xml.parsers.DocumentBuilderFactory

object RichGraphSchemeParser {
  val f = DocumentBuilderFactory.newInstance()
  val parser = f.newDocumentBuilder()
  val url = "use-cases//test.xml"
  val xmlDoc = parser.parse(url)

  val root = xmlDoc.getDocumentElement()

  implicit class XMLHelper(self: Element) {
    def elementOf(s: String, x: Int = 0) = self.getElementsByTagName(s).item(x).asInstanceOf[Element]
    def valueOf(s: String, x: Int = 0) = self.getElementsByTagName(s).item(x).getTextContent()
    def elementListOf(s: String, size: Int) = (0 until size).map(i => self.getElementsByTagName(s).item(i).asInstanceOf[Element])
  }

  val numberOfNode = root.elementOf("graph").getElementsByTagName("nodes").item(0).getTextContent().toLong

  val predicates = root.elementOf("predicates")
  val predicatesSize = predicates.valueOf("size").toInt
  val predicatesAliasList = new Array[String](predicatesSize)
  val predicatesProportionList = new Array[Double](predicatesSize)
  predicates.elementListOf("alias", predicatesSize).foreach { x =>
    predicatesAliasList(x.getAttribute("symbol").toInt) = x.getTextContent()
  }
  predicates.elementListOf("proportion", predicatesSize).foreach {
    x => predicatesProportionList(x.getAttribute("symbol").toInt) = x.getTextContent().toDouble
  }
  val predicatesList = predicatesAliasList.zip(predicatesProportionList)

  val types = root.elementOf("types")
  val typesSize = types.valueOf("size").toInt
  val typesAliasList = new Array[String](typesSize)
  val typesCountList = new Array[Long](typesSize)
  types.elementListOf("alias", typesSize).foreach { x =>
    typesAliasList(x.getAttribute("type").toInt) = x.getTextContent()
  }

  types.elementListOf("proportion", typesSize).filter(_ != null).foreach { x =>
    typesCountList(x.getAttribute("type").toInt) = (x.getTextContent().toDouble * numberOfNode).toLong
  }

  types.elementListOf("fixed", typesSize).filter(_ != null).foreach { x =>
    typesCountList(x.getAttribute("type").toInt) = x.getTextContent().toLong
  }

  val typesRangeList = Array.tabulate(typesSize, 3)((x, y) => 0l)
  for (i <- (0 until typesSize)) {
    typesRangeList(i)(0) = typesCountList(i)
    if (i != 0) typesRangeList(i)(1) = typesRangeList(i - 1)(2) + 1
    typesRangeList(i)(2) = (0 to i).map(y => typesRangeList(y)(0)).reduce(_ + _) - 1
  }

  val typesConfiguration = typesAliasList.zip(typesRangeList)

  //  println(typesList.deep)
  println("Types Configuration")
  typesConfiguration.zipWithIndex.foreach {
    case (x, i) =>
      println("(%d) %-10s\t|V_%-10s|=%d\t(startID,endID)=(%d,%d)".format(i, x._1, x._1, x._2(0), x._2(1), x._2(2)))
  }

  val schemas = root.elementOf("schema")
  val schemasSize = predicatesSize

  val schemasList = new Array[(Int, Int, Int)](schemasSize)

  sealed abstract class DegreeDistribution(param1: Double, param2: Double);
  sealed abstract class ZipfianDistribution(alpha: Double, constant: Double) extends DegreeDistribution(alpha, constant) {

  }

  sealed abstract class UnformDistribution(alpha: Double, constant: Double) extends DegreeDistribution(alpha, constant) {

  }

  //  val schemasList

  //  val schemasList = Array.tabulate(schemasSize, 5)((x, y) => )

  //  val schema

  //  val predicatesProportionList = new Array[Double](predicatesSize)
  //  predicates.elementListOf("alias", predicatesSize).foreach { x =>
  //    predicatesAliasList(x.getAttribute("symbol").toInt) = x.getTextContent()
  //  }
  //  

  //  println("Predicates schema")
  //  predicatesList.foreach{x=>
  //    
  //  }

  //  println(typesRangeList.deep)
}