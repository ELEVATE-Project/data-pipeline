package org.shikshalokam.user.service.fixture

object ProgramEventsMock {

  val PROGRAM_CREATE_EVENT_WITH_USERNAME = """{"entity":"program","eventType":"create","username":"alex0000001","userId":101,"meta":{"programInformation":{"name":"DCPCR School Development Index 2018-19","externalId":"PROGRAM_EXT","id":"66386a1954afe7df153a54d3"}}}"""

  val PROGRAM_DELETE_EVENT_WITH_USERNAME = """{"entity":"program","eventType":"delete","username":"alex0000001","userId":101,"meta":{"programInformation":{"name":"DCPCR School Development Index 2018-19","externalId":"PROGRAM_EXT","id":"66386a1954afe7df153a54d3"}}}"""

}
