package org.shikshalokam.job.project.functions

import org.apache.flink.api.common.typeinfo.TypeInformation
import org.apache.flink.configuration.Configuration
import org.apache.flink.streaming.api.functions.ProcessFunction
import org.shikshalokam.job.project.domain.Event
import org.shikshalokam.job.project.task.ProjectStreamConfig
import org.shikshalokam.job.{BaseProcessFunction, Metrics}
import org.slf4j.LoggerFactory

import scala.collection.immutable._


class ProjectStreamFunction(config: ProjectStreamConfig)(implicit val mapTypeInfo: TypeInformation[Event]/**, @transient var mongoUtil: MongoUtil = null*/)
  extends BaseProcessFunction[Event, Event](config) {

  private[this] val logger = LoggerFactory.getLogger(classOf[ProjectStreamFunction])

  override def metricsList(): List[String] = {
    List(config.projectsCleanupHit, config.skipCount, config.successCount, config.totalEventsCount)
  }

  override def open(parameters: Configuration): Unit = {
    super.open(parameters)
//    mongoUtil = new MongoUtil(config.dbHost, config.dbPort, config.dataBase)
  }

  override def close(): Unit = {
//    mongoUtil.close()
    super.close()
  }

  override def processElement(event: Event, context: ProcessFunction[Event, Event]#Context, metrics: Metrics): Unit = {



    val (projectEvidences, projectEvidencesCount) = extractEvidenceData(event.projectAttachments)
    println("projectEvidences = " + projectEvidences)
    println("projectEvidencesCount = " + projectEvidencesCount)

    val organisationsData = extractOrganisationsData(event.organisations)
    println(organisationsData)

    /** Required to feed orgIds into projects table */
    //val orgIdsString = organisationsData.map(_("orgId")).mkString(", ")
    //println(orgIdsString)

    val locationsDada = extractLocationsData(event.locations)
    println(locationsDada)







    }












  def extractEvidenceData(Attachments: List[Map[String, Any]]): (String, Int) = {
    val evidenceList = Attachments.map { attachment =>
      if (attachment.get("type").contains("link")) {
        attachment.get("name").map(_.toString).getOrElse("")
      } else {
        attachment.get("sourcePath").map(_.toString).getOrElse("")
      }
    }
    (evidenceList.mkString(", "), evidenceList.length)
  }

//  def extractOrganisationsData(Organisations: List[Map[String, Any]]): (String, String) = {
//    val orgId = Organisations.map(organisation => organisation.get("organisationId").map(id => if (id.toString.trim.isEmpty) "Null" else id.toString).getOrElse("Null"))
//    val orgName = Organisations.map(organisation => organisation.get("orgName").map(name => if (name.toString.trim.isEmpty) "Null" else name.toString).getOrElse("Null"))
//    (orgId.mkString(", "), orgName.mkString(", "))
//  }

  def extractOrganisationsData(organisations: List[Map[String, Any]]): List[Map[String, String]] = {
    organisations.map { organisation =>
      val id = organisation.get("organisationId").map(id => if (id.toString.trim.isEmpty) "Null" else id.toString).getOrElse("Null")
      val name = organisation.get("orgName").map(name => if (name.toString.trim.isEmpty) "Null" else name.toString).getOrElse("Null")
      Map("orgId" -> id, "orgName" -> name)
    }
  }

  def extractLocationsData(locations: List[Map[String, Any]]): List[Map[String, String]] = {
    locations.flatMap { location =>
      location.get("type").map(_.toString.trim).filter(_.nonEmpty).flatMap { locationType =>
        val code = location.get("code").map(id => if (id.toString.trim.isEmpty) "Null" else id.toString).getOrElse("Null")
        val externalId = location.get("id").map(id => if (id.toString.trim.isEmpty) "Null" else id.toString).getOrElse("Null")
        val name = location.get("name").map(id => if (id.toString.trim.isEmpty) "Null" else id.toString).getOrElse("Null")
        Some(Map(
          s"${locationType}Code" -> code,
          s"${locationType}ExternalId" -> externalId,
          s"${locationType}Name" -> name
        ))
      }
    }
  }


}