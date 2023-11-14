package com.morpheusdata.veeam.services

import com.morpheusdata.core.util.HttpApiClient
import com.morpheusdata.model.BackupProvider
import groovy.util.logging.Slf4j
import groovy.xml.StreamingMarkupBuilder

import java.util.regex.Matcher
import java.util.regex.Pattern

@Slf4j
class ApiService {

	static taskSleepInterval = 5l * 1000l //5 seconds
	static maxTaskAttempts = 36

	static CLOUD_TYPE_VMWARE = "VMWare"
	static CLOUD_TYPE_HYPERV = "HyperV"
	static CLOUD_TYPE_SCVMM = "Scvmm"
	static CLOUD_TYPE_VCD = "vCloud"

	static MANAGED_SERVER_TYPE_VCENTER = "VC"
	static MANAGED_SERVER_TYPE_HYPERV = "HvServer"
	static MANAGED_SERVER_TYPE_SCVMM = "Scvmm"
	static MANAGED_SERVER_TYPE_VCD = "VcdSystem"

	static buildHeaders(Map headers, String token, Map opts=[:]) {
		def rtn = [:]
		if(token) {
			rtn.'X-RestSvcSessionId' = token
		}
		if(opts.format == 'json') {
			rtn.Accept = 'application/json'
		} else {
			rtn.Accept = 'application/xml'
		}

		return rtn + headers
	}

	def getApiUrl(BackupProvider backupProvider) {
		def scheme = backupProvider.host.contains('http') ? '' : 'http://'
		def apiUrl = "${scheme}${backupProvider.host}:${backupProvider.port}"
		return apiUrl.toString()
	}

	Map getAuthConfig(BackupProvider backupProviderModel) {
		def rtn = [
			apiUrl: getApiUrl(backupProviderModel),
			basePath: '/api',
			username: backupProviderModel.credentialData?.username ?: backupProviderModel.username,
			password: backupProviderModel.credentialData?.password ?: backupProviderModel.password
		]
		log.debug("getAuthConfig: ${rtn}")
		return rtn
	}

	def loginSession(BackupProvider backupProviderModel) {
		def authConfig = getAuthConfig(backupProviderModel)
		return loginSession(authConfig)
	}

	def loginSession(Map authConfig) {
		log.debug("loginSession: {}, {}", authConfig.apiUrl, authConfig.username)
		def rtn = [success:false]
		def response = getToken(authConfig)
		if(response.success) {
			rtn.success = true
			rtn.token = response.token
			rtn.sessionId = response.sessionId
		} else {
			rtn.errorCode = response.errorCode
			rtn.msg = response.msg
			rtn.content = response.content
			rtn.error = true
		}
		return rtn
	}

	def logoutSession(apiUrl, token, sessionId) {
		if(token && sessionId) {
			logout(apiUrl, token, sessionId)
		}
	}

	static getToken(Map authConfig) {
		def rtn = [success:false]
		def requestToken = true
		if(authConfig.token) {
			rtn.success = true
			rtn.token = authConfig.token
			rtn.sessionId = authConfig.sessionId
			requestToken = false
		}
		//if need a new one
		if(requestToken == true) {
			def apiPath = authConfig.basePath + '/sessionMngr/'
			def headers = buildHeaders([:], null)
			HttpApiClient.RequestOptions requestOpts = new HttpApiClient.RequestOptions(headers:headers)
			HttpApiClient httpApiClient = new HttpApiClient()
			def results = httpApiClient.callXmlApi(authConfig.apiUrl, apiPath, authConfig.username, authConfig.password, requestOpts, 'POST')
			rtn.success = results?.success && results?.error != true
			if(rtn.success == true) {
				rtn.token = results.headers['X-RestSvcSessionId']
				rtn.sessionId = results.data.SessionId.toString()
				authConfig.token = rtn.token
				authConfig.sessionId = rtn.sessionId
			} else {
				rtn.content = results.content
				rtn.data = results.data
				rtn.errorCode = results.errorCode
				rtn.headers = results.headers
			}
		}
		return rtn
	}

	static logout(url, token, sessionId){
		def rtn = [success:false]
		def headers = buildHeaders([:], token)
		HttpApiClient httpApiClient = new HttpApiClient()
		HttpApiClient.RequestOptions requestOpts = new HttpApiClient.RequestOptions(headers:headers)
		def results = httpApiClient.callXmlApi(url.toString(), "/api/logonSessions/${sessionId}".toString(), null, null, requestOpts, 'DELETE')
		log.debug("got: ${results}")
		rtn.success = results?.success
		return rtn
	}

	static listSupportedApiVersions(Map authConfig, Map opts) {
		def rtn = [success:false, data:[]]
		def tokenResults = getToken(authConfig)
		if(tokenResults.success == true) {
			def apiPath = authConfig.basePath + '/'
			def headers = buildHeaders([:], tokenResults.token)
			HttpApiClient httpApiClient = new HttpApiClient()
			HttpApiClient.RequestOptions requestOpts = new HttpApiClient.RequestOptions(headers:headers)
			def results = httpApiClient.callXmlApi(authConfig.apiUrl, apiPath, null, null, requestOpts, 'GET')
			if(results.success == true) {
				results.data.SupportedVersions.SupportedVersion.each { supportedVersion ->
					def row = xmlToMap(supportedVersion, true)
					row.version = row.name?.replace('v', '')?.replace('_', '.')?.toFloat()
					rtn.data << row
				}
			}
			rtn.success = true
		} else {
			//return token errors?
		}
		return rtn
	}

	static getLatestApiVersion(Map authConfig, Map opts=[:]) {
		def rtn = [success:false, apiVersion: null]
		try {
			def supportedVersions = listSupportedApiVersions(authConfig, opts)
			if(supportedVersions.success) {
				rtn.apiVersion = supportedVersions.data.collect{ it.version }.sort { a, b -> b<=>a }.getAt(0)
				rtn.success = true
			} else {
				rtn.msg = supportedVersions.msg
			}
		} catch (e) {
			log.error("Error getting latest API version: ${e}", e)
		}

		return rtn
	}

	static listBackupJobs(Map authConfig) {
		log.debug "listBackupJobs: ${authConfig}"
		def rtn = [success:false, jobs:[]]
		def tokenResults = getToken(authConfig)
		if(tokenResults.success == true) {
			def apiPath = authConfig.basePath + '/jobs'
			def headers = buildHeaders([:], tokenResults.token)
			def page = '1'
			def perPage = '50'
			def query = [format:'Entity', pageSize:perPage, page:page]
//			if(opts.backupType)
//				query.filter = 'Platform==' + opts.backupType
			def keepGoing = true
			while(keepGoing) {
				HttpApiClient.RequestOptions requestOpts = new HttpApiClient.RequestOptions(headers:headers, queryParams:query)
				HttpApiClient httpApiClient = new HttpApiClient()
				def results = httpApiClient.callXmlApi(authConfig.apiUrl, apiPath, null, null, requestOpts, 'GET')
				// log.debug("List Backup Jobs result: ${results}")
				if(results.success == true) {
					//iterate results
					results.data.Job?.each { job ->
						def row = xmlToMap(job, true)
						row.externalId = row.uid
						row.scheduleCron = decodeScheduling(job)

						rtn.jobs << row
					}
					//paging
					if(results.data.PagingInfo?.size() > 0 && results.data.PagingInfo['@PageNum']?.toInteger() < results.data.PagingInfo['@PagesCount']?.toInteger()) {
						query.page = (results.data.PagingInfo['@PageNum']?.toInteger() + 1).toString()
						keepGoing = true
					} else {
						keepGoing = false
					}
				} else {
					keepGoing = false
				}
			}
			//no errors - good
			rtn.success = true
		} else {
			//return token errors?

		}
		return rtn
	}

	// BEGIN PORT FROM MORPHEUS CODE BASE - INCLUDE AS NEEDED
//	//new list
//
//	static listHierarchyRoots(Map authConfig, Map opts) {
//		def rtn = [success:false, hierarchyRoots:[]]
//		def tokenResults = getToken(authConfig)
//		if(tokenResults.success == true) {
//			def apiPath = authConfig.basePath + '/hierarchyRoots'
//			def headers = buildHeaders([:], tokenResults.token)
//			def page = opts.page ?: 1
//			def perPage = opts.perPage ?: 50
//			def query = [format:'Entity', pageSize:perPage, page:page]
//			def keepGoing = true
//			while(keepGoing) {
//				HttpApiClient.RequestOptions requestOpts = new HttpApiClient.RequestOptions(headers:headers, queryParams: query)
//				HttpApiClient httpApiClient = new HttpApiClient()
//				def results = httpApiClient.callXmlApi(authConfig.apiUrl, apiPath, null, null, requestOpts, 'GET')
//				if(results.success == true) {
//					results.data.HierarchyRoot?.each { managedServer ->
//						def row = xmlToMap(managedServer, true)
//						row.externalId = row.uid
//						rtn.hierarchyRoots << row
//					}
//					//paging
//					if(results.data.PagingInfo?.size() > 0 && results.data.PagingInfo['@PageNum']?.toInteger() < results.data.PagingInfo['@PagesCount']?.toInteger()) {
//						query.page = (results.data.PagingInfo['@PageNum']?.toInteger() + 1)
//						keepGoing = true
//					} else {
//						keepGoing = false
//					}
//				} else {
//					keepGoing = false
//				}
//			}
//			rtn.success = true
//		} else {
//			//return token errors?
//		}
//		return rtn
//	}
//
//	static listManagedServers(Map authConfig, Map opts) {
//		def rtn = [success:false, managedServers:[]]
//		def tokenResults = getToken(authConfig)
//		if(tokenResults.success == true) {
//			def apiPath = authConfig.basePath + '/managedServers'
//			def headers = buildHeaders([:], tokenResults.token)
//			def page = opts.page ?: 1
//			def perPage = opts.perPage ?: 50
//			def query = [format:'Entity', pageSize:perPage, page:page]
//			if(opts.managedServerType)
//				query.filter = 'managedServerType==' + opts.managedServerType
//			def keepGoing = true
//			while(keepGoing) {
//				HttpApiClient.RequestOptions requestOpts = new HttpApiClient.RequestOptions(headers:headers, queryParams: query)
//				HttpApiClient httpApiClient = new HttpApiClient()
//				def results = httpApiClient.callXmlApi(authConfig.apiUrl, apiPath, null, null, requestOpts, 'GET')
//				if(results.success == true) {
//					results.data.ManagedServer?.each { managedServer ->
//						def row = xmlToMap(managedServer, true)
//						row.externalId = row.uid
//						rtn.managedServers << row
//					}
//					//paging
//					if(results.data.PagingInfo?.size() > 0 && results.data.PagingInfo['@PageNum']?.toInteger() < results.data.PagingInfo['@PagesCount']?.toInteger()) {
//						query.page = (results.data.PagingInfo['@PageNum']?.toInteger() + 1)
//						keepGoing = true
//					} else {
//						keepGoing = false
//					}
//				} else {
//					keepGoing = false
//				}
//			}
//			//no errors - good
//			rtn.success = true
//		} else {
//			//return token errors?
//		}
//		return rtn
//	}
//
//	static getManagedServerRoot(Map authConfig, String name, Map opts) {
//		def rtn = [success:false]
//		try {
//			def tokenResults = getToken(authConfig)
//			def headers = buildHeaders([:], tokenResults.token)
//			def query = [type: 'HierarchyRoot', filter: "Name==\"${name}\"", format:"Entities", pageSize:1]
//			HttpApiClient httpApiClient = new HttpApiClient()
//			HttpApiClient.RequestOptions requestOpts = new HttpApiClient.RequestOptions(headers:headers, queryParams: query)
//			def results = httpApiClient.callXmlApi(authConfig.apiUrl, "/api/query", null, null, requestOpts, 'GET')
//			rtn.success = results.success
//			if(rtn.success) {
//				def hRoot = results.data.Entities.HierarchyRoots.HierarchyRoot.getAt(0)
//				if(hRoot) {
//					rtn.data = [
//							id: hRoot.HierarchyRootId.toString(),
//							uid: hRoot["@UID"].toString(),
//							uniqueId: hRoot.UniqueId.toString(),
//							name: hRoot["@Name"].toString(),
//							hostType: hRoot.HostType.toString(),
//							links: []
//					]
//					hRoot.Links.Link.each {
//						rtn.data.links << [
//								name:it["@Name"].toString(),
//								type:it["@Type"].toString(),
//								rel:it["@Rel"].toString(),
//								href: it["@Href"].toString()
//						]
//					}
//				}
//			}
//			log.debug("managed server root query results: ${rtn}")
//		} catch(Exception e) {
//			log.error("getManagedServerRoot error: ${e}", e)
//		}
//
//		return rtn
//	}
//
//	static listBackupServers(Map authConfig, Map opts) {
//		def rtn = [success:false, backupServers:[]]
//		def tokenResults = getToken(authConfig)
//		if(tokenResults.success == true) {
//			def apiPath = authConfig.basePath + '/backupServers'
//			def headers = buildHeaders([:], tokenResults.token)
//			def page = opts.page ?: 1
//			def perPage = opts.perPage ?: 50
//			def query = [format:'Entity', pageSize:perPage, page:page]
//			if(opts.managedServerType)
//				query.filter = 'managedServerType==' + opts.managedServerType
//			def keepGoing = true
//			while(keepGoing) {
//				HttpApiClient.RequestOptions requestOpts = new HttpApiClient.RequestOptions(headers:headers, queryParams: query)
//				HttpApiClient httpApiClient = new HttpApiClient()
//				def results = httpApiClient.callXmlApi(authConfig.apiUrl, apiPath, null, null, requestOpts, 'GET')
//				if(results.success == true) {
//					//iterate results
//					results.data.BackupServer?.each { backupServer ->
//						def row = xmlToMap(backupServer, true)
//						row.externalId = row.uid
//						rtn.backupServers << row
//					}
//					//paging
//					if(results.data.PagingInfo?.size() > 0 && results.data.PagingInfo['@PageNum']?.toInteger() < results.data.PagingInfo['@PagesCount']?.toInteger()) {
//						query.page = (results.data.PagingInfo['@PageNum']?.toInteger() + 1)
//						keepGoing = true
//					} else {
//						keepGoing = false
//					}
//				} else {
//					keepGoing = false
//				}
//			}
//			//no errors - good
//			rtn.success = true
//		} else {
//			//return token errors?
//		}
//
//		return rtn
//	}
//
//	static listBackups(Map authConfig, jobId, Map opts) {
//		def rtn = [success:false, backups:[]]
//		def tokenResults = getToken(authConfig)
//		if(tokenResults.success == true) {
//			def apiPath = "${authConfig.basePath}/jobs/${jobId}/includes"
//			def headers = buildHeaders([:], tokenResults.token)
//			def page = opts.page ?: 1
//			def perPage = opts.perPage ?: 50
//			def query = [format:'Entity', pageSize:perPage, page:page]
//			def keepGoing = true
//			while(keepGoing) {
//				HttpApiClient.RequestOptions requestOpts = new HttpApiClient.RequestOptions(headers:headers, queryParams: query)
//				HttpApiClient httpApiClient = new HttpApiClient()
//				def results = httpApiClient.callXmlApi(authConfig.apiUrl, apiPath, null, null, requestOpts, 'GET')
//				results.data.ObjectInJob.each { objectInJob ->
//					def row = xmlToMap(objectInJob, true)
//					row.externalId = row.objectInJobId
//					rtn.backups << row
//				}
//				//paging
//				if(results.data.PagingInfo?.size() > 0 && results.data.PagingInfo['@PageNum']?.toInteger() < results.data.PagingInfo['@PagesCount']?.toInteger()) {
//					query.page = (results.data.PagingInfo['@PageNum']?.toInteger() + 1)
//					keepGoing = true
//				} else {
//					keepGoing = false
//				}
//			}
//			//no errors - good
//			rtn.success = true
//		} else {
//			//return token errors?
//		}
//		return rtn
//	}
//
//	static loadBackupJob(Map authConfig, String jobId, Map opts) {
//		def rtn = [success:false, job:null, data:null]
//		def tokenResults = getToken(authConfig)
//		if(tokenResults.success == true) {
//			def apiPath = authConfig.basePath + '/jobs/' + jobId
//			def headers = buildHeaders([:], tokenResults.token)
//			def query = [format:'Entity']
//			HttpApiClient.RequestOptions requestOpts = new HttpApiClient.RequestOptions(headers:headers, queryParams: query)
//			HttpApiClient httpApiClient = new HttpApiClient()
//			def results = httpApiClient.callXmlApi(authConfig.apiUrl, apiPath, null, null, requestOpts, 'GET)
//			if(results.success == true) {
//				rtn.data = results.data
//				rtn.job = xmlToMap(results.data, true)
//			}
//			rtn.success = true
//		} else {
//			//return token errors?
//		}
//		return rtn
//	}
//
//	//old lists
//	static getManagedServers(url, token, managedServerType) {
//		def rtn = [success:false]
//		def managedServers = []
//		def filter = ""
//
//		def headers = buildHeaders([:], token)
//		def query = [type: 'managedServer', format:'entities']
//		if(managedServerType) {
//			query.filter = "managedServerType==${managedServerType}"
//		}
//		HttpApiClient httpApiClient = new HttpApiClient()
//		HttpApiClient.RequestOptions requestOpts = new HttpApiClient.RequestOptions(headers:headers, queryParams: query)
//		def results = httpApiClient.callXmlApi(url, "/api/query", null, null, requestOpts, 'GET')
//		log.debug("got: ${results}")
//		rtn.success = results?.success
//		if(rtn.success == true) {
//			def response = new groovy.util.XmlSlurper().parseText(results.content)
//			response.Entities.ManagedServers.ManagedServer.each { managedServer ->
//				def managedServerName = managedServer['@Name'].toString()
//				def managedServerUid = managedServer['@UID'].toString()
//				def managedServerId = managedServerUid.substring(managedServerUid.lastIndexOf(":") + 1)
//				managedServerType = managedServer.ManagedServerType.toString()
//				def backupServerId
//				def backupServerName
//				managedServer.Links.Link.each{ link ->
//					if(link['@Type'] == "BackupServer") {
//						def backupServerLink = link['@Href'].toString()
//						backupServerId = backupServerLink.substring(backupServerLink.lastIndexOf("/") + 1, backupServerLink.indexOf("?"))
//						backupServerName = link['@Name'].toString()
//						//if backup server name is localhost, use the IP/DNS of the Enterprise Manager so it is more clear to the user
//						if(backupServerName == 'localhost') {
//							backupServerName = url.substring(url.indexOf("://")+3, url.lastIndexOf(":"))
//						}
//					}
//				}
//				def managedServerInfo = [managedServerName:managedServerName, managedServerId:managedServerId, managedServerType:managedServerType,
//				                         backupServerName:backupServerName, backupServerId:backupServerId]
//				managedServers << managedServerInfo
//			}
//		}
//		rtn.managedServers = managedServers
//		return rtn
//	}
//
//	static getBackupServers(url, token){
//		def rtn = [success:false]
//		def backupServers = []
//		def headers = buildHeaders([:], token)
//		HttpApiClient httpApiClient = new HttpApiClient()
//		HttpApiClient.RequestOptions requestOpts = new HttpApiClient.RequestOptions(headers:headers)
//		def results = httpApiClient.callXmlApi(url, "/api/backupServers", null, null, requestOpts, 'GET')
//		log.debug("got: ${results}")
//		rtn.success = results?.success
//		if(rtn.success == true) {
//			def response = new groovy.util.XmlSlurper().parseText(results.content)
//			response.Ref.each { backupServer ->
//				def backupServerName = backupServer['@Name'].toString()
//				def backupServerUid = backupServer['@UID'].toString()
//				def backupServerId = backupServerUid.substring(backupServerUid.lastIndexOf(":")+1)
//				if(backupServerName == 'localhost'){
//					backupServerName = url.substring(url.indexOf("://")+3, url.lastIndexOf(":"))
//				}
//				def backupServerInfo = [backupServerName:backupServerName, backupServerUid:backupServerUid, backupServerId:backupServerId]
//				backupServers << backupServerInfo
//			}
//		}
//		rtn.backupServers = backupServers
//		return rtn
//	}
//
	static getBackupRepositories(Map authConfig) {
		def rtn = [success:false, repositories:[]]
		def tokenResults = getToken(authConfig)
		if(tokenResults.success == true) {
			def apiPath = authConfig.basePath + '/repositories'
			def headers = buildHeaders([:], tokenResults.token)
			def page = '1'
			def perPage = '50'
			def query = [format:'Entity', pageSize:perPage, page:page]
//			if(opts.backupType)
//				query.filter = 'Platform==' + opts.backupType
			def keepGoing = true
			while(keepGoing) {
				HttpApiClient.RequestOptions requestOpts = new HttpApiClient.RequestOptions(headers:headers, queryParams: query)
				HttpApiClient httpApiClient = new HttpApiClient()
				def results = httpApiClient.callXmlApi(authConfig.apiUrl, apiPath, null, null, requestOpts, 'GET')
				if(results.success == true) {
					//iterate results
					results.data.Repository?.each { repository ->
						def row = xmlToMap(repository, true)
						row.externalId = row.uid
						rtn.repositories << row
					}
					//paging
					if(results.data.PagingInfo?.size() > 0 && results.data.PagingInfo['@PageNum']?.toInteger() < results.data.PagingInfo['@PagesCount']?.toInteger()) {
						query.page = (results.data.PagingInfo['@PageNum']?.toInteger() + 1).toString()
						keepGoing = true
					} else {
						keepGoing = false
					}
				} else {
					keepGoing = false
				}
			}
			//no errors - good
			rtn.success = true
		} else {
			//return token errors?

		}
		return rtn
	}
//
//	static getBackupRepositories(url, token){
//		log.debug "getBackupRepositories"
//		def rtn = [success:false]
//		def repositories = []
//		def headers = buildHeaders([:], token)
//		HttpApiClient httpApiClient = new HttpApiClient()
//		HttpApiClient.RequestOptions requestOpts = new HttpApiClient.RequestOptions(headers:headers, token: token)
//		def results = httpApiClient.callXmlApi(url, '/api/repositories', null, null, requestOpts, 'GET')
//		log.debug("got: ${results}")
//		rtn.success = results?.success
//		if(rtn.success == true) {
//			def response = new groovy.util.XmlSlurper().parseText(results.content)
//			response.Ref.each { ref ->
//				def repo = [:]
//				def name = ref['@Name'].toString()
//				repo.name = name
//				repo.id = ref['@UID'].toString()
//				def backupServerReference = ref.Links.Link.find { it['@Type'].toString() == 'BackupServerReference' }
//				def backupServerHref = backupServerReference['@Href'].toString()
//				def backupServerId = backupServerHref.substring(backupServerHref.lastIndexOf('/') + 1)
//				repo.backupServerId = backupServerId
//				repositories << repo
//			}
//		}
//		rtn.repositories = repositories
//		return rtn
//	}
//
//	static getBackupJobId(url, token, backupJobName){
//		def rtn = [success:false]
//		def headers = buildHeaders([:], token)
//		def query = [type: 'job', filter:"name==\"${backupJobName}\"", format:'entities', pageSize:1]
//		HttpApiClient httpApiClient = new HttpApiClient()
//		HttpApiClient.RequestOptions requestOpts = new HttpApiClient.RequestOptions(headers:headers, queryParams: query)
//		def results = httpApiClient.callXmlApi(url, "/api/query", null, null, requestOpts, 'GET')
//		log.debug("got: ${results}")
//		rtn.success = results?.success
//		if(rtn.success) {
//			def response = new groovy.util.XmlSlurper().parseText(results.content)
//			def job = response.Entities.Jobs.Job
//			rtn.backupJobId = job['@UID'].toString()
//		}
//		return rtn
//	}
//
//	static getRepositoryId(url, token, repositoryName){
//		repositoryName = repositoryName ?: "Default Backup Repository"
//		def rtn = [success:false]
//		def headers = buildHeaders([:], token)
//		def query = [type: 'repository', filter: "name==\"${repositoryName}\"", format:"entities", pageSize:1]
//		HttpApiClient httpApiClient = new HttpApiClient()
//		HttpApiClient.RequestOptions requestOpts = new HttpApiClient.RequestOptions(headers:headers, queryParams: query)
//		def results = httpApiClient.callXmlApi(url, "/api/query", null, null, requestOpts, 'GET')
//		log.debug("got: ${results}")
//		rtn.success = results?.success
//		if(rtn.success == true) {
//			def response = new groovy.util.XmlSlurper().parseText(results.content)
//			def repository = response.Entities.Repositories.Repository
//			rtn.repositoryId = repository['@UID'].toString()
//		}
//		return rtn
//	}
//
//	static getBackupJob(url, token, backupJobId){
//		def rtn = [success:false]
//		def headers = buildHeaders([:], token)
//		def query = [format: "Entity"]
//		HttpApiClient httpApiClient = new HttpApiClient()
//		HttpApiClient.RequestOptions requestOpts = new HttpApiClient.RequestOptions(headers:headers, queryParams: query)
//		def results = httpApiClient.callXmlApi(url, "/api/jobs/${backupJobId}", null, null, requestOpts, 'GET')
//		rtn.results = results
//		log.debug("got: ${results}")
//		rtn.success = results?.success
//		def job = new groovy.util.XmlSlurper().parseText(results.content)
//		def name = job['@Name'].toString()
//		rtn.jobId = backupJobId
//		rtn.jobName = name
//		rtn.scheduleEnabled = job.ScheduleEnabled.toString()
//		rtn.scheduleCron = decodeScheduling(job)
//		return rtn
//	}
//
//	static getBackupJobs(url, token){
//		log.debug "getBackupJobs: ${url}"
//		def rtn = [success:false]
//		def headers = buildHeaders([:], token)
//		def query = [format: "Entity"]
//		HttpApiClient httpApiClient = new HttpApiClient()
//		HttpApiClient.RequestOptions requestOpts = new HttpApiClient.RequestOptions(headers:headers, queryParams: query)
//		def results = httpApiClient.callXmlApi(url, "/api/jobs", null, null, requestOpts, 'GET')
//		rtn.success = results?.success
//		rtn.content = new groovy.util.XmlSlurper().parseText(results.content)
//		return rtn
//	}
//
//	static getBackupJobBackups(url, token, backupJobId){
//		def rtn = [success:false]
//		def headers = buildHeaders([:], token)
//		def query = [format:'Entity']
//		HttpApiClient.RequestOptions requestOpts = new HttpApiClient.RequestOptions(headers:headers, queryParams: query)
//		HttpApiClient httpApiClient = new HttpApiClient()
//		def results = httpApiClient.callXmlApi(url, "/api/jobs/${backupJobId}/includes", null, null, requestOpts, 'GET')
//		rtn.taskId = results.data.taskId
//		rtn.data = []
//		results.data.ObjectInJob.each { vmInJob ->
//			rtn.data << [objectId: vmInJob.ObjectInJobId, objectRef: vmInJob.HierarchyObjRef, name: vmInJob.Name]
//		}
//		rtn.success = results?.success
//		return rtn
//	}
//
//	//
//	static createBackupJob(Map authConfig, Map opts) {
//		// use powershell commands
//	}
//
//	static cloneBackupJob(Map authConfig, String cloneId, Map opts) {
//		log.info("cloneBackupJob: ${opts}")
//		def rtn = [success:false, jobId:null, data:null]
//		def tokenResults = getToken(authConfig)
//		if(tokenResults.success == true) {
//			def sourceJob = getBackupJob(authConfig.apiUrl, tokenResults.token, cloneId)
//			def apiPath = authConfig.basePath + '/jobs/' + cloneId
//			def headers = buildHeaders([:], tokenResults.token)
//			def query = [action:'clone']
//			def jobName = opts.jobName
//			def repositoryId = opts.repositoryId
//			def requestXml = new StreamingMarkupBuilder().bind() {
//				JobCloneSpec("xmlns":"http://www.veeam.com/ent/v1.0", "xmlns:xsd":"http://www.w3.org/2001/XMLSchema", "xmlns:xsi":"http://www.w3.org/2001/XMLSchema-instance") {
//					"BackupJobCloneInfo"() {
//						"JobName"(jobName)
//						"RepositoryUid"(repositoryId)
//					}
//				}
//			}
//			HttpApiClient.RequestOptions requestOpts = new HttpApiClient.RequestOptions(headers:headers, queryParams: query, body:requestXml.toString())
//			HttpApiClient httpApiClient = new HttpApiClient()
//			log.debug("requestOpts: ${requestOpts}")
//			def results = httpApiClient.callXmlApi(authConfig.apiUrl, apiPath, null, null, requestOpts, 'POST')
//			log.debug("clone results: ${results}")
//			if(results.success == true) {
//				rtn.data = results.data
//				def taskId = results.data?.TaskId.toString()
//				if(taskId) {
//					def taskResults = waitForTask(authConfig, taskId)
//					log.debug("taskResults: ${taskResults}")
//					if(taskResults.success == true) {
//						//find the job link
//						def jobLink = taskResults.links?.find{ it.type == 'Job' }
//						if(jobLink) {
//							rtn.jobId = parseEntityId(jobLink.href)
//							def jobInfo = getBackupJob(authConfig.apiUrl, tokenResults.token, rtn.jobId)
//							rtn.scheduleCron = jobInfo.scheduleCron
//							rtn.success = true
//						} else {
//							rtn.msg = taskResults.msg
//							log.error("Error cloning job: ${taskResults.msg}")
//						}
//						// ensure the job is enabled if the source job was enabled
//						if(sourceJob.scheduleEnabled == "true" && rtn.jobId) {
//							Promises.task {
//								// The only way I found to ensure a job is enabled after cloning is to first
//								// disable it and then enable it.
//								def disableScheduleResults = disableBackupJobSchedule(authConfig.apiUrl, tokenResults.token, rtn.jobId)
//								if(disableScheduleResults.taskId) {
//									waitForTask(authConfig, disableScheduleResults.taskId)
//								}
//
//								// for now just disable the schedule on the cloned job, morpheus will run the jobs
//								// def enableScheduleResults = enableBackupJobSchedule(authConfig.apiUrl, tokenResults.token, rtn.jobId)
//								// if(enableScheduleResults.success == true) {
//								// 	log.debug("Enable job schedule results: ${enableScheduleResults}")
//								// 	if(enableScheduleResults.taskId) {
//								// 		waitForTask(authConfig, enableScheduleResults.taskId)
//								// 	}
//								// }
//
//								return true
//							}.onError { Exception ex ->
//								log.error("Error disabling cloned job schedule: {}", ex, ex)
//							}
//						}
//					} else {
//						rtn.msg = taskResults.msg
//						log.error("Error cloning job: ${taskResults.msg}")
//					}
//				}
//			}
//		}
//		return rtn
//	}
//
//	//vm utils
//	static findVm(Map authConfig, String externalId, List managedServers, Map opts) {
//		log.debug("findVm: ${externalId}, ManagedServers: ${managedServers}, opts: ${opts}")
//		def rtn = [success:false, vmId:null]
//		def tokenResults = getToken(authConfig)
//		if(tokenResults.success == true) {
//			//search all servers
//			managedServers?.each { managedServer ->
//				if(rtn.success != true) {
//					def hostRoot = managedServer.contains("HierarchyRoot") ? managedServer : "urn:veeam:HierarchyRoot:${managedServer}"
//					def results = lookupVm(authConfig.apiUrl, tokenResults.token, opts.cloudType, hostRoot, externalId)
//					//check results
//					log.debug("findVm results: ${results}")
//					if(results.success == true) {
//						rtn.vmId = results.vmId
//						rtn.vmName = results.vmName
//						rtn.hierarchyRoot = hostRoot
//						rtn.success = true
//					}
//				}
//			}
//		}
//
//		return rtn
//	}
//
//	static findHierarchyRootByManagedServerName(Map authConfig, String name, Map opts=[:]) {
//		log.debug("findHierarchyRootByManagedServerName: ${name}, opts: ${opts}")
//		def rtn = [success:false, data:[:]]
//		def tokenResults = getToken(authConfig)
//		if(tokenResults.success == true) {
//			def apiPath = authConfig.basePath + '/query'
//			def headers = buildHeaders([:], tokenResults.token)
//
//			def query = [type:'hierarchyRoot', filter:"name==\"${name}\""]
//			HttpApiClient.RequestOptions requestOpts = new HttpApiClient.RequestOptions(headers:headers, queryParams: query)
//			HttpApiClient httpApiClient = new HttpApiClient()
//			def results = httpApiClient.callXmlApi(authConfig.apiUrl, apiPath, null, null, requestOpts, 'GET')
//			//check results
//			if(results.success) {
//				def contentMap = xmlToMap(results.content, true)
//				def hierarchyRootRef = contentMap.refs?.ref?.uid
//				if(hierarchyRootRef) {
//					rtn.data = [objRef: hierarchyRootRef]
//				}
//				if(rtn.data?.objRef) {
//					rtn.success = true
//				}
//			}
//		}
//
//		return rtn
//	}
//
//	static queryBackupJobs(url, token, backupType = 'VMware') {
//		def rtn = [success:false]
//		def headers = buildHeaders([:], token)
//		def query = [type:"job", format:'entities', pageSize:100, page:1]
//		if(backupType) {
//			query.filter = "Platform==${backupType}"
//		}
//		HttpApiClient httpApiClient = new HttpApiClient()
//		HttpApiClient.RequestOptions requestOpts = new HttpApiClient.RequestOptions(headers:headers, queryParams: query)
//		def results = httpApiClient.callXmlApi(url, "/api/query", null, null, requestOpts, 'GET')
//		rtn.success = results?.success && !results?.errorCode
//		def jobs = []
//		if(rtn.success == true) {
//			def response = new groovy.util.XmlSlurper().parseText(results.content)
//			response.Entities?.Jobs?.Job.each { job ->
//				def name = job['@Name'].toString()
//				def uid = job['@UID'].toString()
//				def platform = job.Platform.toString()
//				def backupServerReference = job.Links.Link.find { it['@Type'].toString() == 'BackupServerReference' }
//				def backupServerHref = backupServerReference['@Href'].toString()
//				def backupServerId = backupServerHref.substring(backupServerHref.lastIndexOf('/') + 1)
//				jobs << [uid: uid, name: name, platform: platform, backupServerId: backupServerId]
//			}
//			while(response.PagingInfo?.size() > 0 && response.PagingInfo['@PageNum']?.toInteger() < response.PagingInfo['@PagesCount']?.toInteger()) {
//				def nextPage = response.PagingInfo['@PageNum']?.toInteger() + 1
//				query.page = nextPage
//				requestOpts = new HttpApiClient.RequestOptions(headers:headers, queryParams: query)
//				results = httpApiClient.callXmlApi(url, "/api/query", null, null, requestOpts, 'GET')
//				if(results?.success) {
//					response = new groovy.util.XmlSlurper().parseText(results.content)
//					response.Entities?.Jobs?.Job.each { job ->
//						def name = job['@Name'].toString()
//						def uid = job['@UID'].toString()
//						def platform = job.Platform.toString()
//						def backupServerReference = job.Links.Link.find { it['@Type'].toString() == 'BackupServerReference' }
//						def backupServerHref = backupServerReference['@Href'].toString()
//						def backupServerId = backupServerHref.substring(backupServerHref.lastIndexOf('/') + 1)
//						jobs << [uid:uid, name:name, platform:platform, backupServerId:backupServerId]
//					}
//				}
//			}
//		}
//		log.debug("found ${jobs.size()} jobs")
//		rtn.jobs = jobs
//		return rtn
//	}
//
//	static waitForVm(Map authConfig, String vmExternalId, List managedServers, Map opts) {
//		def rtn = [success:false, error:false, data:null, vmId:null]
//		def attempt = 0
//		def keepGoing = true
//		while(keepGoing == true && attempt < 2) {
//			//load the vm
//			def results = findVm(authConfig, vmExternalId, managedServers, opts)
//			if(results.success == true) {
//				rtn.success = true
//				rtn.data = results.data
//				rtn.vmId = results.vmId
//				rtn.vmName = results.vmName
//				rtn.hierarchyRoot = results.hierarchyRoot
//				keepGoing = false
//			} else {
//				attempt++
//				sleep(taskSleepInterval)
//			}
//		}
//		return rtn
//	}
//
//	//backup
//	static createBackupJobBackup(Map authConfig, String jobId, List managedServers, Map opts) {
//		log.debug("createBackupJobBackup: ${opts}")
//		def rtn = [success:false, backupId:null, data:null]
//		def tokenResults = getToken(authConfig)
//		if(tokenResults.success == true) {
//			def apiPath = authConfig.basePath + '/jobs/' + jobId + '/includes'
//			def headers = buildHeaders([:], tokenResults.token)
//			def query = [format:'Entity']
//			HttpApiClient.RequestOptions requestOpts = new HttpApiClient.RequestOptions(headers:headers, queryParams: query)
//			HttpApiClient httpApiClient = new HttpApiClient()
//			//need managed servers
//			def findResults = waitForVm(authConfig, opts.externalId, managedServers, opts)
//			log.info("wait results: ${findResults}")
//			if(findResults.success == true) {
//				def vmId = findResults.vmId
//				def vmName = findResults.vmName
//				rtn.hierarchyObjecRef = findResults.vmId
//				rtn.hierarchyRoot = findResults.hierarchyRoot
//				//load up the existing job
//				def results = httpApiClient.callXmlApi(authConfig.apiUrl, apiPath, null, null, requestOpts, 'GET')
//				log.info("job results: ${results}")
//				if(results.success == true) {
//					//save off the existing
//					def existingItems = []
//					def processingOpts
//					results.data.ObjectInJob.each { vmInJob ->
//						if(vmInJob.ObjectInJobId) {
//							existingItems << vmInJob.ObjectInJobId.toString()
//							if(processingOpts == null)
//								processingOpts = vmInJob.GuestProcessingOptions
//						}
//					}
//					//add new
//					def requestXml = new StreamingMarkupBuilder().bind {
//						CreateObjectInJobSpec('xmlns':'http://www.veeam.com/ent/v1.0', 'xmlns:xsd':'http://www.w3.org/2001/XMLSchema', 'xmlns:xsi':'http://www.w3.org/2001/XMLSchema-instance') {
//							'HierarchyObjRef'(vmId)
//							'HierarchyObjName'(vmName)
//						}
//					}
//					//add it
//					log.debug("requestOpts: ${requestOpts}")
//					requestOpts = new HttpApiClient.RequestOptions(headers:headers, queryParams: query, body: requestXml.toString())
//					def addResults = httpApiClient.callXmlApi(authConfig.apiUrl, apiPath, null, null, requestOpts, 'POST')
//					log.debug("addResults: ${addResults}")
//					if(addResults.success == true) {
//						//get task id and wait
//						def taskId = addResults.data?.TaskId.toString()
//						log.debug("taskId: ${taskId}")
//						if(taskId) {
//							def taskResults = waitForTask(authConfig, taskId)
//							if(taskResults.success == true) {
//								rtn.success = true
//								log.debug("taskResults: ${taskResults}")
//								//get the backup id?
//								//remove existing?
//								if(opts.removeJobs == true) {
//									requestOpts = new HttpApiClient.RequestOptions(headers:headers)
//									existingItems.each { existingId ->
//										def itemPath = apiPath + '/' + existingId
//										def deleteResults = httpApiClient.callXmlApi(authConfig.apiUrl, itemPath, null, null, requestOpts, 'DELETE')
//										def deleteTaskId = deleteResults.data?.TaskId?.toString()
//										if(deleteTaskId) {
//											def deleteTaskResults = waitForTask(authConfig, deleteTaskId)
//											log.debug("deleteResults: ${deleteResults}")
//										}
//									}
//								}
//								//get the job and id
//								def jobObject
//								def jobDetailAttempts = 0
//								def maxJobDetailAttempts = 10
//								while(!jobObject && jobDetailAttempts < maxJobDetailAttempts) {
//									def jobResults = loadBackupJob(authConfig, jobId, opts)
//									if(jobResults.success == true) {
//										if(jobResults.job.jobInfo?.backupJobInfo?.includes?.objectInJob instanceof Map) {
//											log.debug("ONLY FOUND ONE OBJECT IN JOB")
//											def tmpJobObj = jobResults.job.jobInfo?.backupJobInfo?.includes?.objectInJob
//											if(opts.externalId) {
//												def jobObjMor = extractMOR(tmpJobObj.hierarchyObjRef)
//												def jobObjUid = extractVeeamUuid(tmpJobObj.hierarchyObjRef)
//												if(opts.externalId == jobObjMor || opts.externalId.contains(jobObjUid)) {
//													jobObject = tmpJobObj
//												}
//											}
//										} else {
//											log.debug("FOUND MULTIPLE JOB OBJECTS, FIND THE RIGHT ONE")
//											jobObject = jobResults.job.jobInfo?.backupJobInfo?.includes?.objectInJob?.find {
//												if(opts.externalId) {
//													def itMor = extractMOR(it.hierarchyObjRef)
//													return opts.externalId == itMor
//												} else {
//													return vmName == it.name
//												}
//											}
//										}
//										if(rtn.backupId == null && jobObject) {
//											log.debug("JOB OBJECT WAS FOUND")
//											rtn.backupId = jobObject.objectInJobId
//											rtn.objectId = jobResults.job.uid
//											rtn.success = true
//										}
//
//									} else {
//										//couldn't find the job
//									}
//									if(!jobObject) {
//										log.debug("DIDN'T FIND THE JOB OBJECT WE WERE LOOKING FOR")
//										jobDetailAttempts++
//										sleep(3000)
//									}
//								}
//							} else {
//								//error waiting for task to finish
//								rtn.msg = taskResults.msg ?: "Failed to find backup creation task in Veeam."
//							}
//						} else {
//							// failed to create task, no task ID returned
//							rtn.msg = "Failed to create request for backup creation in Veeam."
//							log.error("Failed to create back up include in backup job, task ID not found in API response: ${addResults}")
//						}
//					} else {
//						//error adding to job
//						rtn.msg = "Failed to create backup job."
//						log.error("Failed to create backup job: ${addResults}")
//					}
//				} else {
//					rtn.msg = "Unable to load details for job ${jobId}."
//					log.error("Failed to load job details: ${results}")
//				}
//			} else {
//				//count not find the vm
//				rtn.msg = "Failed to find VM in Veeam. Ensure the VM is accessible to the Veeam backup server."
//				log.error("Unable to locate VM in Veeam: ${findResults}")
//			}
//		}
//		return rtn
//	}
//
//	static createBackupJobFromTemplate(url, token, backupJobIdToClone, newBackupName, vmNameToBackup, repositoryUid){
//		log.debug "createBackupJobFromTemplate: backupJobIdToClone=${backupJobIdToClone}, newBackupName: ${newBackupName}, repositoryUid: ${repositoryUid}"
//		def rtn = [success:false]
//		def taskId
//		def jobId
//		def headers = buildHeaders([:], token)
//		def xml = new StreamingMarkupBuilder().bind() {
//			JobCloneSpec("xmlns":"http://www.veeam.com/ent/v1.0", "xmlns:xsd":"http://www.w3.org/2001/XMLSchema", "xmlns:xsi":"http://www.w3.org/2001/XMLSchema-instance") {
//				"BackupJobCloneInfo"(){
//					"JobName"(newBackupName)
//					"RepositoryUid"(repositoryUid)
//				}
//			}
//		}
//		def body = xml.toString()
//		log.debug("body: ${body}")
//		//clone
//		def query = [action:'clone']
//		HttpApiClient httpApiClient = new HttpApiClient()
//		HttpApiClient.RequestOptions requestOpts = new HttpApiClient.RequestOptions(headers:headers, queryParams: query, body: body)
//		def results = httpApiClient.callXmlApi(url, "/api/jobs/${backupJobIdToClone}", null, null, requestOpts, 'POST')
//		log.debug("got: ${results}")
//		rtn.success = results?.success
//		if(results?.success == true) {
//			def response = new groovy.util.XmlSlurper().parseText(results.content)
//			taskId = response.TaskId.toString()
//		}
//		if(taskId) {
//			def i=0
//			while(!rtn.state && i < 5){
//				sleep(10000l) //wait for job to clone
//				log.debug "waiting for clone to complete..."
//				def taskQuery = [format: "Entity"]
//
//				requestOpts = new HttpApiClient.RequestOptions(headers:headers, query:taskQuery)
//				results = httpApiClient.callXmlApi(url, "/api/tasks/${taskId}", null, null, requestOpts, 'GET')
//				log.debug("got: ${results}")
//				rtn.success = results?.success
//				if(rtn.success == true) {
//					def response = new groovy.util.XmlSlurper().parseText(results.content)
//					def state = response.State.toString()
//					if(state == "Finished") {
//						rtn.state = state
//						def success = response.Result['@Success']
//						if(success == "true"){
//							response.Links.Link.each{ link ->
//								if(link['@Type'] == "Job"){
//									def jobLink = link['@Href'].toString()
//									jobId = jobLink.substring(jobLink.lastIndexOf("/")+1, jobLink.indexOf("?"))
//									rtn.jobId = jobId
//								}
//							}
//						}
//						else if(success == "false") {
//							rtn.success = false
//							rtn.error = response.Result.Message.toString()
//						}
//					}
//				}
//				i++
//			}
//		}
//		//edit the job and change the VM
//		//if(jobId){
//		//	updateBackupJobVM(url, token, jobId, managedServerName, vmNameToBackup)
//		//}
//		return rtn
//	}
//
//	static startBackupJob(Map authConfig, jobId, opts=[:]){
//		log.debug "startBackupJob: ${jobId}"
//		def rtn = [success:false]
//		def tokenResults = getToken(authConfig)
//		if(tokenResults.success == true) {
//			def taskId
//			def apiPath = authConfig.basePath + '/jobs/' + jobId
//			def headers = buildHeaders([:], tokenResults.token)
//			def query = [action:'start']
//			HttpApiClient.RequestOptions requestOpts = new HttpApiClient.RequestOptions(headers:headers, queryParams: query)
//			HttpApiClient httpApiClient = new HttpApiClient()
//			def results = httpApiClient.callXmlApi(authConfig.apiUrl, apiPath, null, null, requestOpts, 'POST')
//			log.debug("veeam backup start request got: ${results}")
//			def jobStartDate = results.headers.Date
//			rtn.success = results?.success
//			if (results?.success == true) {
//				def response = xmlToMap(results.data, true)
//				taskId = response.taskId
//			}
//			if (taskId) {
//				def taskResults = waitForTask(authConfig, taskId, ['Finished'])
//				rtn.success = taskResults?.success
//				if(taskResults.success && !taskResults.error) {
//					log.debug("backup job task results: ${taskResults}")
//					def jobSessionLink = taskResults.links.find { it.type == "BackupJobSession"}?.href
//					if(jobSessionLink) {
//						rtn.backupSessionId = extractVeeamUuid(jobSessionLink)
//					} else {
//						if(jobStartDate instanceof String) {
//							def tmpJobStartDate = MorpheusUtils.parseDate(jobStartDate)
//							jobStartDate = MorpheusUtils.formatDate(tmpJobStartDate)
//						}
//						def backupResult = getLastBackupResult(authConfig, jobId, opts + [startRefDateStr: jobStartDate])
//						log.info("got backup result - " + backupResult)
//						rtn.backupSessionId = backupResult.backupResult?.backupSessionId
//					}
//				} else{
//					rtn.success = false
//					def resultData = xmlToMap(taskResults.data, true)
//					rtn.errorMsg = resultData.result.message
//				}
//			}
//		}
//		return rtn
//	}
//
//	static stopBackupJob(url, token, backupJobId){
//		def rtn = [success:false]
//		def taskId = ""
//		def headers = buildHeaders([:], token)
//		def query = [action:'stop']
//		HttpApiClient httpApiClient = new HttpApiClient()
//		HttpApiClient.RequestOptions requestOpts = new HttpApiClient.RequestOptions(headers:headers, queryParams: query)
//		def results = httpApiClient.callXmlApi(url, "/api/jobs/${backupJobId}", null, null, requestOpts, 'POST')
//		log.debug("got: ${results}")
//		rtn.success = results?.success
//		if(results?.success == true) {
//			def response = new groovy.util.XmlSlurper().parseText(results.content)
//			rtn.taskId = response.TaskId.toString()
//		}
//		return rtn
//	}
//
//	static startQuickBackup(Map authConfig, String backupServerId, String vmId, Map opts = [:]){
//		log.debug "startQuickBackup - backupServerId: ${backupServerId}, vmId: ${vmId}"
//		def rtn = [success:false]
//		def tokenResults = getToken(authConfig)
//		if(tokenResults.success == true) {
//			def taskId
//			def apiPath = authConfig.basePath + "/backupServers/${backupServerId}"
//			def headers = buildHeaders([:], tokenResults.token)
//			def query = [action:'quickbackup']
//			def bodyXml = new StreamingMarkupBuilder().bind() {
//				QuickBackupStartupSpec("xmlns":"http://www.veeam.com/ent/v1.0", "xmlns:xsd":"http://www.w3.org/2001/XMLSchema", "xmlns:xsi":"http://www.w3.org/2001/XMLSchema-instance") {
//					VmRef(vmId)
//				}
//			}
//			def body = bodyXml.toString()
//			HttpApiClient.RequestOptions requestOpts = new HttpApiClient.RequestOptions(headers:headers, queryParams: query, body: body)
//			HttpApiClient httpApiClient = new HttpApiClient()
//			def results = httpApiClient.callXmlApi(authConfig.apiUrl, apiPath, null, null, requestOpts, 'POST')
//			log.debug("veeam quick backup start request got: ${results}")
//			def backupStartDate = results.headers.Date
//			rtn.success = results?.success
//			if (results?.success == true) {
//				def response = xmlToMap(results.data, true)
//				taskId = response.taskId
//			}
//			if (taskId) {
//				def taskResults = waitForTask(authConfig, taskId, ['Finished'])
//				rtn.success = taskResults?.success
//				if(taskResults.success && !taskResults.error) {
//					log.debug("quick backup task results: ${taskResults}")
//					def jobSessionLink = taskResults.links.find { it.type == "BackupJobSession"}?.href
//					if(jobSessionLink) {
//						rtn.backupSessionId = extractVeeamUuid(jobSessionLink)
//						rtn.startDate = backupStartDate
//					} else {
//						rtn.success = false
//						rtn.errorMsg = "Job session ID not found in Veeam task results."
//					}
//				} else{
//					rtn.success = false
//					rtn.status = "FAILED"
//					def resultData = xmlToMap(taskResults.data, true)
//					rtn.errorMsg = resultData.result.message
//				}
//			}
//		}
//		return rtn
//	}
//
//	static startVeeamZip(Map authConfig, String backupServerId, String repositoryId, String vmId, Map opts = [:]){
//		log.debug "startVeeamZip - backupServerId: ${backupServerId}, RepositoryId: ${repositoryId}, vmId: ${vmId}"
//		def rtn = [success:false]
//		def tokenResults = getToken(authConfig)
//		if(tokenResults.success == true) {
//			def taskId
//			def apiPath = authConfig.basePath + "/backupServers/${backupServerId}"
//			def headers = buildHeaders([:], tokenResults.token)
//			def query = [action:'veeamzip']
//			def bodyXml = new StreamingMarkupBuilder().bind() {
//				VeeamZipStartupSpec("xmlns":"http://www.veeam.com/ent/v1.0", "xmlns:xsd":"http://www.w3.org/2001/XMLSchema", "xmlns:xsi":"http://www.w3.org/2001/XMLSchema-instance") {
//					VmRef(vmId)
//					RepositoryUid(repositoryId)
//					BackupRetention("Never")
//					Compressionlevel(3)
//					if(opts.vmwToolsInstalled) {
//						// doesn't work well with vmware tools quiescensce
//						DisableGuestQuiescence(true)
//					}
//				}
//			}
//			def body = bodyXml.toString()
//			HttpApiClient.RequestOptions requestOpts = new HttpApiClient.RequestOptions(headers:headers, queryParams: query, body: body)
//			HttpApiClient httpApiClient = new HttpApiClient()
//			def results = httpApiClient.callXmlApi(authConfig.apiUrl, apiPath, null, null, requestOpts, 'POST')
//			log.debug("veeamzip backup start request got: ${results}")
//			def backupStartDate = results.headers.Date
//			rtn.success = results?.success
//			if (results?.success == true) {
//				def response = xmlToMap(results.data, true)
//				taskId = response.taskId
//			}
//			if (taskId) {
//				def taskResults = waitForTask(authConfig, taskId, ['Finished'])
//				rtn.success = taskResults?.success
//				if(taskResults.success && !taskResults.error) {
//					log.debug("veeamzip task results: ${taskResults}")
//					def jobSessionLink = taskResults.links.find { it.type == "BackupJobSession"}?.href
//					if(jobSessionLink) {
//						rtn.backupSessionId = extractVeeamUuid(jobSessionLink)
//						rtn.startDate = backupStartDate
//					} else {
//						rtn.success = false
//						rtn.errorMsg = "Job session ID not found in Veeam task results."
//					}
//				} else{
//					rtn.success = false
//					rtn.status = "FAILED"
//					def resultData = xmlToMap(taskResults.data, true)
//					rtn.errorMsg = resultData.result.message
//				}
//			}
//		}
//		return rtn
//	}
//
//
//	//not supported by API
//	static deleteBackupJob(url, token, backupJobId) {
//		def rtn = [success:false, data:[:]]
//		try {
//			def headers = buildHeaders([:], token)
//			HttpApiClient httpApiClient = new HttpApiClient()
//			HttpApiClient.RequestOptions requestOpts = new HttpApiClient.RequestOptions(headers:headers)
//			def results = httpApiClient.callXmlApi(url, "/api/jobs/${backupJobId}", null, null, requestOpts, 'DELETE')
//			log.debug("got: ${results}")
//			rtn.success = results?.success
//			if(results?.success == true) {
//				def response = new groovy.util.XmlSlurper().parseText(results.content)
//				rtn.data.taskId = response.TaskId.toString()
//			} else if(results.errorCode == 400) {
//				rtn.success = true
//			}
//		} catch(Exception e) {
//			log.error("deleteBackupJob error: {}", e, e)
//		}
//
//		return rtn
//	}
//
//	//turn off backup schedule
//	static disableBackupJobSchedule(url, token, backupJobId){
//		def rtn = [success:false]
//		def taskId = ""
//		def backupJob = getBackupJob(url, token, backupJobId)
//		if(backupJob.scheduleEnabled == "false" && backupJob.ScheduleConfigured == "false"){
//			//schedule is already off
//			rtn.success = true
//			return rtn
//		}
//
//		def bodyXmlBuilder = backupJob.results.data
//		bodyXmlBuilder.ScheduleConfigured = "false"
//		bodyXmlBuilder.ScheduleEnabled = "false"
//		bodyXmlBuilder.jobScheduleOptions = ""
//
//		def headers = buildHeaders([:], token)
//		def query = [format: "Entity"]
//		def body = new StreamingMarkupBuilder().bindNode(bodyXmlBuilder).toString()
//		HttpApiClient httpApiClient = new HttpApiClient()
//		HttpApiClient.RequestOptions requestOpts = new HttpApiClient.RequestOptions(headers:headers, queryParams: query, body: body)
//		def results = httpApiClient.callXmlApi(url, "/api/jobs/${backupJobId}", null, null, requestOpts, 'PUT')
//		log.debug("disableBackupJobSchedule got: ${results}")
//		rtn.success = results?.success
//		if(results?.success == true) {
//			def response = new groovy.util.XmlSlurper().parseText(results.content)
//			rtn.taskId = response.TaskId.toString()
//		}
//		return rtn
//	}
//
//	//turn off backup schedule
//	static enableBackupJobSchedule(url, token, backupJobId){
//		def rtn = [success:false]
//		def taskId = ""
//		def backupJob = getBackupJob(url, token, backupJobId)
//		if(backupJob.scheduleEnabled){
//			//schedule is already on
//			rtn.success = true
//			return rtn
//		}
//		def headers = buildHeaders([:], token)
//		def query = [action: "toggleScheduleEnabled"]
//		HttpApiClient httpApiClient = new HttpApiClient()
//		HttpApiClient.RequestOptions requestOpts = new HttpApiClient.RequestOptions(headers:headers, queryParams: query)
//		def results = httpApiClient.callXmlApi(url, "/api/jobs/${backupJobId}", null, null, requestOpts, 'POST')
//		log.debug("got: ${results}")
//		rtn.success = results?.success
//		if(results?.success == true) {
//			def response = new groovy.util.XmlSlurper().parseText(results.content)
//			rtn.taskId = response.TaskId.toString()
//		}
//		return rtn
//	}
//
//	static removeVmFromBackupJob(url, token, backupJobId, vmId) {
//		def rtn = [success:false]
//		def headers = buildHeaders([:], token)
//		def query = [format:'Entity']
//		HttpApiClient.RequestOptions requestOpts = new HttpApiClient.RequestOptions(headers:headers, queryParams: query)
//		HttpApiClient httpApiClient = new HttpApiClient()
//		def apiPath = '/api/jobs/' + backupJobId + '/includes/' + vmId
//		def results = httpApiClient.callXmlApi(url, apiPath, null, null, requestOpts, 'DELETE')
//		rtn.taskId = results.data?.TaskId.toString()
//		rtn.success = results?.success
//		rtn.data = results.data
//		log.debug "remove vm results ${results}"
//		return rtn
//	}
//
//	static removeVmsFromBackupJob(url, token, backupJobId) {
//		def rtn = [success:false]
//		def existingVmsInJob = []
//		def headers = buildHeaders([:], token)
//		def query = [format: "Entity"]
//		//get current VMs in job
//		HttpApiClient httpApiClient = new HttpApiClient()
//		HttpApiClient.RequestOptions requestOpts = new HttpApiClient.RequestOptions(headers:headers, queryParams: query)
//		def results = httpApiClient.callXmlApi(url, "/api/jobs/${backupJobId}/includes", null, null, requestOpts, 'GET')
//		log.debug("got: ${results}")
//		rtn.success = results?.success
//		if(rtn.success == true) {
//			def response = new groovy.util.XmlSlurper(false, false).parseText(results.content)
//			response.ObjectInJob.each { vmInJob ->
//				existingVmsInJob << vmInJob.ObjectInJobId.toString()
//			}
//		}
//		existingVmsInJob.each { vmInJob ->
//			requestOpts = new HttpApiClient.RequestOptions(headers:headers)
//			results = httpApiClient.callXmlApi(url, "/api/jobs/${backupJobId}/includes/${vmInJob}", null, null, requestOpts, 'DELETE')
//			log.debug "got ${results}"
//		}
//		rtn.success = results?.success
//		return rtn
//	}
//
//	static getBackupResult(url, token, backupSessionId) {
//		def rtn = [success:false]
//		def backupResult = [:]
//		def headers = buildHeaders([:], token)
//		def query = [format: "Entity"]
//		HttpApiClient httpApiClient = new HttpApiClient()
//		HttpApiClient.RequestOptions requestOpts = new HttpApiClient.RequestOptions(headers:headers, queryParams: query)
//		def results = httpApiClient.callXmlApi(url, "/api/backupSessions/${backupSessionId}", null, null, requestOpts, 'GET')
//		log.debug("got: ${results}")
//		rtn.success = results?.success
//		if(results?.success == true) {
//			def response = new groovy.util.XmlSlurper().parseText(results.content)
//			def jobUid = response.JobUid.toString()
//			def jobName = response.JobName.toString()
//			def startTime = response.CreationTimeUTC.toString()
//			def endTime = response.EndTimeUTC.toString()
//			def state = response.State.toString()
//			def result = response.Result.toString()
//			def progress = response.Progress.toString()
//			backupResult = [backupSessionId: backupSessionId, backupJobName: jobName, startTime: startTime, endTime: endTime, state: state, result: result, progress: progress, links: []]
//			response.Links.Link.each { link ->
//				backupResult.links << [href: link['@Href'], type: link['@Type']]
//			}
//			if(result == "Success" || result == "Warning"){
//				def stats = getBackupResultStats(url, token, backupSessionId)
//				backupResult.totalSize = stats?.totalSize ?: 0
//			}
//		}
//		rtn.result = backupResult
//		return rtn
//	}
//
//	static getBackupResults(url, token, backupJobId) {
//		def rtn = [success:false]
//		def backupJobUid = "urn:veeam:Job:${backupJobId}".toString()
//		def backupResults = []
//		def headers = buildHeaders([:], token)
//		def query = [type:'backupJobSession', filter:"jobUid==\"${backupJobUid}\"", format:'entities']
//		HttpApiClient httpApiClient = new HttpApiClient()
//		HttpApiClient.RequestOptions requestOpts = new HttpApiClient.RequestOptions(headers:headers, queryParams: query)
//		def results = httpApiClient.callXmlApi(url, "/api/query", null, null, requestOpts, 'GET')
//		log.debug("got: ${results}")
//		rtn.success = results?.success
//		if(results?.success == true) {
//			def response = new groovy.util.XmlSlurper().parseText(results.content)
//			response.Entities.BackupJobSessions.BackupJobSession.each { backupJobSession ->
//				def uid = backupJobSession.JobUid.toString()
//
//				def backupJobSessionUid = backupJobSession['@UID'].toString()
//				def backupJobSessionId = backupJobSessionUid.substring(backupJobSessionUid.lastIndexOf(":")+1)
//				def jobName = backupJobSession.JobName.toString()
//				def startTime = backupJobSession.CreationTimeUTC.toString()
//				def endTime = backupJobSession.EndTimeUTC.toString()
//				def state = backupJobSession.State.toString()
//				def result = backupJobSession.Result.toString()
//				def progress = backupJobSession.Progress.toString()
//				def backupResult = [backupSessionId: backupJobSessionId, backupJobName: jobName, startTime: startTime, endTime: endTime, state: state, result: result, progress: progress]
//				if(result == "Success" || result == "Warning"){
//					def stats = getBackupResultStats(url, token, backupJobSessionId)
//					backupResult.totalSize = stats?.totalSize ?: 0
//				}
//				backupResults << backupResult
//			}
//		}
//		rtn.results = backupResults
//		return rtn
//	}
//
//	static getLastBackupResult(Map authConfig, backupJobId, opts=[:]) {
//		log.debug "getLastBackupResult: ${backupJobId}"
//		def rtn = [success:false]
//		def backupResult
//		def backupJobUid = "urn:veeam:Job:${extractVeeamUuid(backupJobId)}"
//		//get hiearchy	 root for the VM cloud
//		def tokenResults = getToken(authConfig)
//		if(tokenResults.success == true) {
//			def apiPath = authConfig.basePath + '/query'
//			def headers = buildHeaders([:], tokenResults.token)
//			def queryFilter = "jobUid==\"${backupJobUid}\""
//			if(opts.startRefDateStr) {
//				queryFilter += ";CreationTime>=\"${opts.startRefDateStr}\""
//			}
//			def query = [type: 'backupJobSession', filter: queryFilter, format: 'entities', sortDesc: 'CreationTime', pageSize: 1 ]
//			HttpApiClient.RequestOptions requestOpts = new HttpApiClient.RequestOptions(headers: headers, queryParams: query)
//			HttpApiClient httpApiClient = new HttpApiClient()
//			log.info("getLastBackupResult query: ${query}")
//			def attempt = 0
//			def keepGoing = true
//			def response
//			while(keepGoing == true && attempt < maxTaskAttempts) {
//				def results = httpApiClient.callXmlApi(authConfig.apiUrl, apiPath, null, null, requestOpts, 'GET')
//				rtn.success = results?.success
//				if(rtn.success == true) {
//					response = xmlToMap(results.data, true)
//					if(response.entities.backupJobSessions?.size() > 0) {
//						def tmpJobSession = response.entities.backupJobSessions.backupJobSession
//						def tmpJobSessionUid = tmpJobSession.uid
//						def tmpBackupSessionId = tmpJobSessionUid.substring(tmpJobSessionUid.lastIndexOf(":")+1)
//						if(!opts.lastBackupSessionId || tmpBackupSessionId != opts.lastBackupSessionId) {
//							keepGoing = false
//						}
//					}
//
//				} else {
//					keepGoing = false
//					return rtn
//				}
//				sleep(taskSleepInterval)
//				attempt++
//			}
//
//			if(response.entities?.backupJobSessions?.size() > 0) {
//				def backupJobSession = response.entities.backupJobSessions.backupJobSession
//				def backupJobSessionUid = backupJobSession.uid
//				def backupJobSessionId = backupJobSessionUid.substring(backupJobSessionUid.lastIndexOf(":") + 1)
//				def jobName = backupJobSession.jobName.toString()
//				def startTime = backupJobSession.creationTimeUTC.toString()
//				def endTime = backupJobSession.endTimeUTC.toString()
//				def state = backupJobSession.state.toString()
//				def result = backupJobSession.result.toString()
//				def progress = backupJobSession.progress.toString()
//				backupResult = [backupSessionId: backupJobSessionId, backupJobName: jobName, startTime: startTime, endTime: endTime, state: state, result: result, progress: progress]
//			}
//			rtn.backupResult = backupResult
//		}
//
//		return rtn
//	}
//
//	static getRestorePoint(Map authConfig, String objectRef, Map opts=[:]) {
//		log.debug "getLatestRestorePoint: ${objectRef}"
//		def rtn = [success:false, data: [:]]
//		def tokenResults = getToken(authConfig)
//		if(tokenResults.success == true) {
//			def headers = buildHeaders([:], tokenResults.token)
//			def queryFilter = "HierarchyObjRef==\"${objectRef}\""
//			if(opts.startRefDateStr) {
//				queryFilter += ";CreationTime>=\"${opts.startRefDateStr}\""
//			}
//			def query = [type: 'VmRestorePoint', filter: queryFilter, format: 'Entities', sortDesc: 'CreationTime', pageSize: 1 ]
//
//			def attempt = 0
//			def keepGoing = true
//			while(keepGoing == true && attempt < maxTaskAttempts) {
//				HttpApiClient httpApiClient = new HttpApiClient()
//				HttpApiClient.RequestOptions requestOpts = new HttpApiClient.RequestOptions(headers:headers, queryParams: query)
//				def restorePointsResults = httpApiClient.callXmlApi(authConfig.apiUrl, "/api/query", null, null, requestOpts, 'GET')
//				if(restorePointsResults.success) {
//					def restorePointsResponse = new groovy.util.XmlSlurper().parseText(restorePointsResults.content)
//					def restoreRef = restorePointsResponse.Entities.VmRestorePoints.VmRestorePoint.getAt(0)
//					if(restoreRef) {
//						rtn.data.externalId = restoreRef["@UID"].toString()
//						rtn.success = true
//						keepGoing = false
//					}
//				} else {
//					keepGoing = false
//					return rtn
//				}
//				sleep(taskSleepInterval)
//				attempt++
//			}
//		}
//
//		return rtn
//	}
//
//	static getRestoreResult(url, token, restoreSessionId) {
//		def rtn = [success:false]
//		def restoreResult = [:]
//		def headers = buildHeaders([:], token)
//		def query = [format: "Entity"]
//		HttpApiClient httpApiClient = new HttpApiClient()
//		HttpApiClient.RequestOptions requestOpts = new HttpApiClient.RequestOptions(headers:headers, queryParams: query)
//		def results = httpApiClient.callXmlApi(url, "/api/restoreSessions/${restoreSessionId}", null, null, requestOpts, 'GET')
//		log.debug("got: ${results}")
//		rtn.success = results?.success
//		if(results?.success == true) {
//			def response = new groovy.util.XmlSlurper().parseText(results.content)
//			def startTime = response.CreationTimeUTC.toString()
//			def endTime = response.EndTimeUTC?.toString()
//			def state = response.State.toString()
//			def result = response.Result.toString()
//			def progress = response.Progress.toString()
//			def vmId
//			def vmRef = response.RestoredObjRef.toString()
//			if(vmRef && vmRef != ""){
//				vmId = vmRef?.substring(vmRef?.lastIndexOf(".")+1)
//			}
//			restoreResult = [restoreSessionId: restoreSessionId, vmId:vmId, startTime: startTime, endTime: endTime, state: state, result: result, progress: progress]
//		}
//		rtn.result = restoreResult
//		return rtn
//	}
//
//	//lookup backup task sessions for backup size
//	static getBackupResultStats(url, token, backupJobSessionId){
//		def rtn = [success:false]
//		rtn.totalSize = 0
//		def headers = buildHeaders([:], token)
//		def query = [format: "Entity"]
//		HttpApiClient httpApiClient = new HttpApiClient()
//		HttpApiClient.RequestOptions requestOpts = new HttpApiClient.RequestOptions(headers:headers, queryParams: query)
//		def results = httpApiClient.callXmlApi(url, "/api/backupSessions/${backupJobSessionId}/taskSessions", null, null, requestOpts, 'GET')
//		log.debug("got: ${results}")
//		rtn.success = results?.success
//		if(rtn.success == true) {
//			def totalSize = 0
//			def response = new groovy.util.XmlSlurper().parseText(results.content)
//			response.BackupTaskSession.each { backupTaskSession ->
//				rtn.totalSize += backupTaskSession.TotalSize.toLong()
//			}
//		}
//		return rtn
//	}
//
//	static getBackupStats(url, token){
//		def rtn = [success:false]
//		def headers = buildHeaders([:], token)
//		HttpApiClient httpApiClient = new HttpApiClient()
//		HttpApiClient.RequestOptions requestOpts = new HttpApiClient.RequestOptions(headers:headers)
//		def results = httpApiClient.callXmlApi(url, "/api/reports/summary/job_statistics", null, null, requestOpts, 'GET')
//		log.debug("got: ${results}")
//		rtn.success = results?.success
//		if(rtn.success == true) {
//			def response = new groovy.util.XmlSlurper().parseText(results.content)
//			rtn.runningJobs = response.RunningJobs.toString()
//			rtn.scheduledBackupJobs = response.ScheduledBackupJobs.toString()
//			rtn.totalJobRuns = response.TotalJobRuns.toString()
//			rtn.successfulJobRuns = response.SuccessfulJobRuns.toString()
//			rtn.failedJobRuns = response.FailedJobRuns.toString()
//			rtn.maxBackupJobDuration = response.MaxBackupJobDuration.toString()
//			rtn.maxDurationBackupJobName = response.MaxDurationBackupJobName.toString()
//		}
//		return rtn
//	}
//
//	static restoreVM(url, token, objectRef, backupSessionId, opts=[:]) {
//		def rtn = [success:false]
//		def headers = buildHeaders([:], token)
//		def query = [format: "Entity"]
//		def restoreLink
//
//		if(opts.restoreHref) {
//			def restoreType = opts.restoreType
//			def uri = new URI(opts.restoreHref)
//			HttpApiClient httpApiClient = new HttpApiClient()
//			HttpApiClient.RequestOptions requestOpts = new HttpApiClient.RequestOptions(headers:headers, queryParams: query)
//			def results = httpApiClient.callXmlApi(url, uri.path, null, null, requestOpts, 'GET')
//			log.debug("got: ${results}")
//			rtn.success = results?.success
//			if(rtn.success == true) {
//				def response = new groovy.util.XmlSlurper().parseText(results.content)
//				response[restoreType].Links.Link.each { link ->
//					if(link['@Type'] == "Restore" || link['@Rel'] == "Restore") {
//						def restoreUrl = new URI(link['@Href']?.toString())
//						restoreLink = restoreUrl.path
//					}
//				}
//			}
//		}
//
//		// for everything not vcd
//		// the restore point is set in the backup result config
//		if(!restoreLink && (opts.restorePointId || opts.restoreRef)) {
//			def restorePointId = opts.restorePointId ?: opts.restoreRef
//			HttpApiClient httpApiClient = new HttpApiClient()
//			HttpApiClient.RequestOptions requestOpts = new HttpApiClient.RequestOptions(headers:headers, queryParams: query)
//			def results = httpApiClient.callXmlApi(url, "/api/restorePoints/${restorePointId}/vmRestorePoints", null, null, requestOpts, 'GET')
//			log.debug("got: ${results}")
//			rtn.success = results?.success
//			if(rtn.success == true) {
//				def response = new groovy.util.XmlSlurper().parseText(results.content)
//				response.VmRestorePoint.Links.Link.each { link ->
//					if(link['@Type'] == "Restore" || link['@Rel'] == "Restore") {
//						def restoreUrl = new URI(link['@Href']?.toString())
//						restoreLink = restoreUrl.path
//					}
//				}
//			}
//		}
//		if(opts.vmRestorePointId || (opts.restorePointId && !restoreLink)) {
//			HttpApiClient httpApiClient = new HttpApiClient()
//			HttpApiClient.RequestOptions requestOpts = new HttpApiClient.RequestOptions(headers:headers, queryParams: query)
//			def results = httpApiClient.callXmlApi(url, "/api/vmRestorePoints/${opts.vmRestorePointId ?: opts.restorePointId}", null, null, requestOpts, 'GET')
//			log.debug("got: ${results}")
//			rtn.success = results?.success
//			if(rtn.success == true) {
//				def response = new groovy.util.XmlSlurper().parseText(results.content)
//				response.Links.Link.each { link ->
//					if(link['@Rel'] == "Restore") {
//						def restoreUrl = new URI(link['@Href']?.toString())
//						restoreLink = restoreUrl.path
//					}
//				}
//			}
//		}
//
//		// we only have the backup session, find the restore resources
//		if(!restoreLink) {
//			HttpApiClient httpApiClient = new HttpApiClient()
//			HttpApiClient.RequestOptions requestOpts = new HttpApiClient.RequestOptions(headers:headers, queryParams: query)
//			def results = httpApiClient.callXmlApi(url, "/api/backupSessions/${backupSessionId}", null, null, requestOpts, 'GET')
//			//find restore points
//			def restorePointsLink
//			rtn.success = results?.success
//			if(rtn.success == true) {
//				def response = new groovy.util.XmlSlurper().parseText(results.content)
//				response.Links.Link.each { link ->
//					if(link['@Type'] == "RestorePointReference") {
//						def restorePointsUrl = new URI(link['@Href'].toString())
//						restorePointsLink = "${restorePointsUrl.path}/vmRestorePoints"
//					}
//					if(link['@Type'] == "VmRestorePoint") {
//						def restoreUrl = new URI(link['@Href']?.toString())
//						restorePointsLink = restoreUrl.path
//					}
//				}
//			}
//
//
//			// probably a veeamzip, need to go find the restore point for the backup session
//			if(!restorePointsLink) {
//				def response = xmlToMap(results.content, true)
//				def backupName = response.jobName // the backup session and the backup(result) should have the same name
//				def backupResults = fetchQuery(opts.authConfig, "Backup", [Name: backupName])
//				def restoreRefList = backupResults.data.refs?.ref?.links?.link?.find { it.type == "RestorePointReferenceList" }
//				// get a list of restore points from the backup
//				def refListLink = new URI(restoreRefList.href)
//				requestOpts = new HttpApiClient.RequestOptions(headers:headers, queryParams: query)
//				def refListResults = httpApiClient.callXmlApi(url, null, null, refListLink.path,requestOpts, 'GET')
//				rtn.success = refListResults?.success
//				if(rtn.success == true) {
//					// we need the vm restore point to execute the restore
//					def refListResponse = xmlToMap(refListResults.content)
//					refListResponse.RestorePoint.Links.Link.each { link ->
//						if(link['Type'] == "VmRestorePointReferenceList") {
//							def restoreUrl = new URI(link['Href']?.toString())
//							restorePointsLink = restoreUrl.path
//						}
//					}
//				}
//			}
//
//
//			if(restorePointsLink) {
//				requestOpts = new HttpApiClient.RequestOptions(headers:headers, queryParams: query)
//				def restoreLinkResults = httpApiClient.callXmlApi(url, "${restorePointsLink}", null, null, requestOpts, 'GET')
//				log.debug("got: ${results}")
//				rtn.success = results?.success
//				if(rtn.success == true) {
//					def response = new groovy.util.XmlSlurper().parseText(restoreLinkResults.content)
//					def restorePoint
//					if(response.name() == "VmRestorePoints") {
//						restorePoint = response.VmRestorePoint.find { it.HierarchyObjRef.text().toString().toLowerCase() == objectRef?.toLowerCase() || it.VmName.text().toString() == opts.vmName }
//						if(!restorePoint && opts.vCenterVmId) {
//							restorePoint = response.VmRestorePoint.find { it.HierarchyObjRef.text().toString().endsWith(opts.vCenterVmId) }
//						}
//					} else {
//						restorePoint = response
//					}
//
//					if(restorePoint) {
//						restorePoint.Links.Link.each { link ->
//							if(link['@Rel']?.toString() == "Restore") {
//								def restoreUrl = new URI(link['@Href']?.toString())
//								restoreLink = restoreUrl.path
//							}
//						}
//					} else {
//						rtn.msg = "Veeam restore point not found for VM"
//						rtn.success = false
//						log.error(rtn.msg)
//					}
//				}
//			}
//		}
//
//		//execute the restore
//		def taskId
//		if(restoreLink) {
//			log.debug("Performing restore with endpoint: ${restoreLink}")
//			def xml
//			if(opts.zoneType == 'vcd' && opts.backupType != "veeamzip") {
//				def restorePointUid = extractVeeamUuid(restoreLink)
//				def hierarchyRootUid = extractVeeamUuid(opts.hierarchyRoot)
//				xml = new StreamingMarkupBuilder().bind() {
//					RestoreSpec("xmlns": "http://www.veeam.com/ent/v1.0", "xmlns:xsd": "http://www.w3.org/2001/XMLSchema", "xmlns:xsi": "http://www.w3.org/2001/XMLSchema-instance") {
//						vCloudVmRestoreSpec() {
//							"PowerOnAfterRestore"(true)
//							"HierarchyRootUid"(hierarchyRootUid)
//							vAppRef("urn:${CLOUD_TYPE_VCD}:Vapp:${hierarchyRootUid}.urn:vcloud:vapp:${opts.vAppId}")
//							VmRestoreParameters() {
//								VmRestorePointUid("urn:veeam:VmRestorePoint:${restorePointUid}")
//							}
//						}
//					}
//				}
//			} else {
//				xml = new StreamingMarkupBuilder().bind() {
//					RestoreSpec("xmlns": "http://www.veeam.com/ent/v1.0", "xmlns:xsd": "http://www.w3.org/2001/XMLSchema", "xmlns:xsi": "http://www.w3.org/2001/XMLSchema-instance") {
//						VmRestoreSpec() {
//							"PowerOnAfterRestore"(true)
//							"QuickRollback"(false)
//						}
//					}
//				}
//			}
//			def body = xml.toString()
//			log.debug "body: ${body}"
//			def restoreQuery = query + [action: 'restore']
//			HttpApiClient httpApiClient = new HttpApiClient()
//			HttpApiClient.RequestOptions requestOpts = new HttpApiClient.RequestOptions(headers:headers, query:restoreQuery, body: body)
//			def results = httpApiClient.callXmlApi(url, restoreLink, null, null, requestOpts, 'POST')
//			rtn.success = results?.success
//			if(rtn.success == true) {
//				def response = new groovy.util.XmlSlurper().parseText(results.content)
//				//get the restore session id
//				taskId = response.TaskId
//			}
//		} else if(!rtn.msg) {
//			log.debug("Unable to perform restore, no restore link found.")
//			rtn.msg = "Veeam restore link not found"
//			rtn.success = false
//			log.error(rtn.msg)
//		}
//		if(taskId) {
//			def restoreTaskResults = waitForTask([token: token, apiUrl: url, basePath:'/api'], taskId.toString())
//			rtn.success = restoreTaskResults?.success
//			if(rtn.success == true) {
//				restoreTaskResults.links.each{ link ->
//					if(link.type == "RestoreSession"){
//						def restoreSessionUrl =  new URL(link.href?.toString())
//						def restoreSessionId = restoreSessionUrl.path.substring(restoreSessionUrl.path.lastIndexOf("/")+1)
//						rtn.restoreSessionId = restoreSessionId
//					}
//				}
//			} else {
//				rtn.success = false
//				rtn.msg = restoreTaskResults.msg
//			}
//		}
//		return rtn
//	}
//
//	//lookup the veeam VM ID by searching a single hierarchy root for the VM name
//	static lookupVmByName(url, token, managedServerId, vmName) {
//		def rtn = [success:false]
//		rtn = getVmIdByName(url, token, managedServerId, vmName)
//		if(!rtn.vmId) {
//			log.error("Failed to find VM object in Veeam: ${vmName}")
//		}
//		return rtn
//	}
//
//	//get the veeam VM ID given the veeam managed server ID (hiearchy root) and VM name
//	static getVmIdByName(url, token, managedServerId, vmName) {
//		def rtn = [success:false]
//		//find the VM under the VM cloud
//		def vmId
//		if(managedServerId) {
//			def hierarchyRoot = managedServerId.contains("HierarchyRoot") ? managedServerId : "urn:veeam:HierarchyRoot:${managedServerId}"
//			def headers = buildHeaders([:], token)
//			def query = [host: hierarchyRoot, name: vmName, type: 'Vm']
//			HttpApiClient httpApiClient = new HttpApiClient()
//			HttpApiClient.RequestOptions requestOpts = new HttpApiClient.RequestOptions(headers:headers, queryParams: query)
//			def results = httpApiClient.callXmlApi(url, "/api/lookup", null, null, requestOpts, 'GET')
//			log.debug("got vmbyid results: ${results}")
//			rtn.success = results?.success
//			if(rtn.success == true) {
//				def response = new groovy.util.XmlSlurper().parseText(results.content)
//				vmId = response.HierarchyItem.ObjectRef.toString()
//				rtn.vmId = vmId
//			}
//		}
//		return rtn
//	}
//
//	//lookup the veeam VM ID given the veeam managed server and the vmware VM ref ID
//	static lookupVm(url, token, cloudType, hierarchyRoot, vmRefId) {
//		def rtn = [success:false]
//		rtn = getVmId(url, token, cloudType, hierarchyRoot, vmRefId)
//		if(!rtn.vmId){
//			log.error("Failed to find VM object in Veeam: ${vmRefId}")
//		}
//		return rtn
//	}
//
//	//get the veeam VM ID given the veeam managed server ID (hiearchy root) and vmware VM ref ID
//	static getVmId(url, token, cloudType, hierarchyRoot, vmRefId) {
//		def rtn = [success:false]
//		//construct the object ref
//		def vmId = getVmHierarchyObjRef(vmRefId, hierarchyRoot, cloudType)
//		def headers = buildHeaders([:], token)
//		def query = [hierarchyRef: vmId]
//		HttpApiClient httpApiClient = new HttpApiClient()
//		HttpApiClient.RequestOptions requestOpts = new HttpApiClient.RequestOptions(headers:headers, queryParams: query)
//		def results = httpApiClient.callXmlApi(url, "/api/lookup", null, null, requestOpts, 'GET')
//		log.debug("got: ${results}")
//		rtn.success = results?.success
//		if(rtn.success == true) {
//			def response = new groovy.util.XmlSlurper().parseText(results.content)
//			rtn.vmId = results.data.HierarchyItem.ObjectRef.toString()
//			rtn.vmName = results.data.HierarchyItem.ObjectName.toString()
//		}
//		return rtn
//	}
//
//	static getVmHierarchyObjRef(backup) {
//		def objRef = backup.getConfigProperty('hierarchyObjRef')
//		if(!objRef) {
//			def hierarchyRootUid = backup.getConfigProperty("veeamHierarchyRootUid")
//			def managedServerId = hierarchyRootUid ? hierarchyRootUid : backup.getConfigProperty("veeamManagedServerId")?.split(":")?.getAt(0)
//			def container = Container.get(backup.containerId)
//			def server = container?.server
//			if(server) {
//				def cloudType = getCloudTypeFromZoneType(server.zone.zoneType)
//				objRef = getVmHierarchyObjRef(server.externalId, managedServerId, cloudType)
//			}
//		}
//
//
//		return objRef
//	}
//
//	static getVmHierarchyObjRef(vmRefId, managedServerId, cloudType) {
//		def parentServerId = managedServerId
//		if(managedServerId.contains("urn:veeam")) {
//			parentServerId = extractVeeamUuid(managedServerId)
//		}
//		def rtn = "urn:${cloudType}:"
//		rtn += cloudType == CLOUD_TYPE_VCD ? "Vapp" : "Vm"
//		rtn += ":${parentServerId}.${vmRefId}"
//
//		return rtn
//	}
//
//	static getCloudTypeFromZoneType(ComputeZoneType zoneType) {
//		def rtn
//
//		if (zoneType?.code == 'hyperv' || zoneType?.code == 'scvmm') {
//			rtn = CLOUD_TYPE_HYPERV
//		} else if (zoneType?.code == 'vmware') {
//			rtn = CLOUD_TYPE_VMWARE
//		} else if(zoneType?.code == 'vcd') {
//			rtn = CLOUD_TYPE_VCD
//		}
//
//		return rtn
//	}
//
//	static getManagedServerTypeFromZoneType(ComputeZoneType zoneType) {
//		def rtn = zoneType.code == 'vmware' ? 'VC' : 'HV'
//		//todo find out real code for hyperv
//		if(zoneType.code == 'hyperv') {
//			rtn = MANAGED_SERVER_TYPE_HYPERV
//		} else if(zoneType.code == 'vmware') {
//			rtn = MANAGED_SERVER_TYPE_VCENTER
//		} else if(zoneType.code == 'scvmm') {
//			rtn = MANAGED_SERVER_TYPE_SCVMM
//		} else if(zoneType.code == 'vcd') {
//			rtn = MANAGED_SERVER_TYPE_VCD
//		}
//
//		return rtn
//	}
//
//	static updateBackupJob(url, token, backupJobId, schedule) {
//		def rtn = [success:false]
//		def body
//		def headers = buildHeaders([:], token)
//		def query = [format: "Entity"]
//		HttpApiClient httpApiClient = new HttpApiClient()
//		HttpApiClient.RequestOptions requestOpts = new HttpApiClient.RequestOptions(headers:headers, queryParams: query)
//		def results = httpApiClient.callXmlApi(url, "/api/jobs/${backupJobId}", null, null, requestOpts, 'GET')
//		log.debug("got: ${results}")
//		rtn.success = results?.success
//		if(rtn.success == true) {
//			def response = new groovy.util.XmlSlurper(false, false).parseText(results.content)
//			def optionsDaily = response.JobScheduleOptions.OptionsDaily[0]
//			optionsDaily.replaceNode {
//				OptionsDaily {
//					Kind("SelectedDays")
//					schedule.days.each{ day ->
//						Days(day)
//					}
//					Time(schedule.time)
//				}
//			}
//			body = new StreamingMarkupBuilder().bindNode(response).toString()
//		}
//		if(body) {
//			requestOpts = new HttpApiClient.RequestOptions(headers:headers, queryParams: query, body: body)
//			results = httpApiClient.callXmlApi(url, "/api/jobs/${backupJobId}", null, null, requestOpts, 'PUT')
//			log.debug("got: ${results}")
//			rtn.success = results?.success
//		}
//		return rtn
//	}
//
//	static updateBackupJobVM(url, token, backupJobId, vmUid, vmName) {
//		def rtn = [success:false]
//		def existingVmsInJob = []
//		def processingOptions
//		def headers = buildHeaders([:], token)
//		def query = [format: "Entity"]
//		//get current VMs in job
//		HttpApiClient httpApiClient = new HttpApiClient()
//		HttpApiClient.RequestOptions requestOpts = new HttpApiClient.RequestOptions(headers:headers, queryParams: query)
//		def results =  httpApiClient.callXmlApi(url, "/api/jobs/${backupJobId}/includes", null, null, requestOpts, 'GET')
//		log.debug("got: ${results}")
//		rtn.success = results?.success
//		if(rtn.success == true) {
//			def response = new groovy.util.XmlSlurper(false, false).parseText(results.content)
//			response.ObjectInJob.each { vmInJob ->
//				existingVmsInJob << vmInJob.ObjectInJobId.toString()
//				processingOptions = vmInJob.GuestProcessingOptions
//			}
//		}
//		//add new VM to the job
//		if(processingOptions) {
//			def body = new StreamingMarkupBuilder().bind {
//				CreateObjectInJobSpec("xmlns":"http://www.veeam.com/ent/v1.0", "xmlns:xsd":"http://www.w3.org/2001/XMLSchema", "xmlns:xsi":"http://www.w3.org/2001/XMLSchema-instance") {
//					"HierarchyObjRef"(vmUid)
//					"HierarchyObjName"(vmName)
//					"Order"(0)
//					mkp.yield processingOptions
//				}
//			}.toString()
//			log.debug("body: " + body)
//			requestOpts = new HttpApiClient.RequestOptions(headers:headers, body: body)
//			results = httpApiClient.callXmlApi(url, "/api/jobs/${backupJobId}/includes", null, null, requestOpts, 'POST')
//			log.debug("got: ${results}")
//			rtn.success = results?.success
//			if(results?.success == true) {
//				def response = new groovy.util.XmlSlurper().parseText(results.content)
//				def taskId = response.TaskId.toString()
//				if(taskId) {
//					sleep(10000l) //wait for update to finish
//					requestOpts = new HttpApiClient.RequestOptions(headers:headers, queryParams: query)
//					results = httpApiClient.callXmlApi(url, "/api/tasks/${taskId}", null, null, requestOpts, 'GET')
//					log.debug("got: ${results}")
//					rtn.success = results?.success
//					if(rtn.success == true) {
//						response = new groovy.util.XmlSlurper().parseText(results.content)
//						def success = response.Result['@Success']
//						if(success != "true"){
//							rtn.success = false
//						}
//					}
//				}
//			}
//		}
//		//remove other VMs from the job
//		if(rtn.success == true) {
//			existingVmsInJob.each { vmInJob ->
//				requestOpts = new HttpApiClient.RequestOptions(headers:headers)
//				results = httpApiClient.callXmlApi(url, "/api/jobs/${backupJobId}/includes/${vmInJob}", null, null, requestOpts, 'DELETE')
//			}
//		} else {
//			log.error("Failed to update backupJob: ${backupJobId} with vmUid: ${vmUid}")
//		}
//		return rtn
//	}
//
//	static fetchQuery(Map authConfig, String objType, Map filters, Boolean entityFormat=false, Map opts=[:]) {
//		def rtn = [success:false]
//		def apiPath = authConfig.basePath + '/query'
//		def apiUrl = authConfig.apiUrl
//		def headers = buildHeaders([:], authConfig.token)
//		def query = [type: objType, filter:""]
//		if(entityFormat) {
//			query.format = 'entities'
//		}
//		for(filter in filters) {
//			if(query.filter.size() > 0) {
//				query.filter += "&"
//			}
//			query.filter += "${filter.key}==\"${filter.value}\""
//		}
//
//		HttpApiClient httpApiClient = new HttpApiClient()
//		HttpApiClient.RequestOptions requestOpts = new HttpApiClient.RequestOptions(headers:headers, queryParams: query)
//		rtn = httpApiClient.callXmlApi(apiUrl, apiPath, null, null, requestOpts, 'GET')
//		log.debug("fetchQuery results: ${rtn}")
//		if(rtn.success) {
//			rtn.data = xmlToMap(rtn.content, true)
//		}
//		return rtn
//	}
//
//	//tasks
//	static waitForTask(Map authConfig, String taskId, waitForState=['Finished']) {
//		def rtn = [success:false, error:false, data:null, state:null, links:[]]
//		def apiPath = authConfig.basePath + '/tasks/' + taskId
//		def headers = buildHeaders([:], authConfig.token)
//		def query = [format:'Entity']
//		HttpApiClient.RequestOptions requestOpts = new HttpApiClient.RequestOptions(headers:headers, queryParams: query)
//		HttpApiClient httpApiClient = new HttpApiClient()
//		def attempt = 0
//		def keepGoing = true
//		while(keepGoing == true && attempt < maxTaskAttempts) {
//			//load the task
//			def results = httpApiClient.callXmlApi(authConfig.apiUrl, apiPath, null, null, requestOpts, 'GET')
//			//check results
//			if(results?.success == true) {
//				def taskState = results.data.State.text()
//				if(waitForState.contains(taskState)) {
//					rtn.success = true
//					rtn.data = results.data
//					rtn.state = taskState
//					keepGoing = false
//					//parse results
//					def taskSuccess = results.data.Result['@Success']
//					if(taskSuccess == 'true') {
//						results.data.Links?.Link?.each { link ->
//							def linkType = link['@Type']?.toString()
//							def linkHref = link['@Href']?.toString()
//							if(linkType && linkHref)
//								rtn.links << [type: linkType, href: linkHref]
//						}
//					} else if(taskSuccess == 'false') {
//						rtn.success = false
//						def msg = results.data?.Result?.Message?.text()
//						if(msg?.indexOf('not found') > -1 && attempt < 3) {
//							//try again
//							sleep(taskSleepInterval)
//							keepGoing = true
//						} else {
//							rtn.msg = msg
//							rtn.error = true
//							keepGoing = false
//						}
//					}
//				} else {
//					sleep(taskSleepInterval)
//				}
//			} else if(results.errorCode == 500) {
//				def errorMessage
//				try {
//					def response = new groovy.util.XmlSlurper(false,true).parseText(results.content)
//					errorMessage = response["@Message"]
//				} catch (Exception ex1) {
//					try {
//						// we might encounter json here?
//						def response = new groovy.json.JsonSlurper().parseText(results.content)
//						errorMessage = response.Message
//					} catch (Exception ex2) {
//						// if all else fails, just treat it as a string
//						errorMessage = results.data?.toString()
//					}
//				}
//				if(errorMessage =~ /^.*?no\s.*?\stask\swith\sid/) {
//					// "There is no backup task with id [task-297] in current rest session"
//					// the task has completed and cleaned up???
//					rtn.success = true
//				} else {
//					rtn.msg = errorMessage
//				}
//				keepGoing = false
//			} else {
//				sleep(taskSleepInterval)
//			}
//			attempt++
//		}
//		return rtn
//	}
//
	static dayOfWeekList = [
			[index:1, name:'Sunday'],
			[index:2, name:'Monday'],
			[index:3, name:'Tuesday'],
			[index:4, name:'Wednesday'],
			[index:5, name:'Thursday'],
			[index:6, name:'Friday'],
			[index:7, name:'Saturday']
	]

	static monthList = [
			[index:1, name:'January'],
			[index:2, name:'February'],
			[index:3, name:'March'],
			[index:4, name:'April'],
			[index:5, name:'May'],
			[index:6, name:'June'],
			[index:7, name:'July'],
			[index:8, name:'August'],
			[index:9, name:'September'],
			[index:10, name:'October'],
			[index:11, name:'November'],
			[index:12, name:'December']
	]

	//scheduling
	static decodeScheduling(job) {
		def rtn
		//build a cron representation
		def scheduleSet = job.ScheduleConfigured
		def scheduleOn = job.ScheduleEnabled
		def optionsDaily = job.JobScheduleOptions?.OptionsDaily
		def optionsMonthly = job.JobScheduleOptions?.OptionsMonthly
		def optionsPeriodically = job.JobScheduleOptions?.OptionsPeriodically
		//build cron off the type
		if(optionsDaily['@Enabled'] == 'true') {
			//get the hour offset
			def timeOffset = optionsDaily.TimeOffsetUtc?.toLong()
			def hour = ((int)(timeOffset.div(3600l)))
			def minute = ((int)((timeOffset - (hour * 3600l)).div(60)))
			//build the string
			rtn = '0 ' + minute + ' ' + hour
			//get the days of the week
			if(optionsDaily.Kind == 'Everyday') {
				rtn = rtn + ' 	* * ?'
			} else {
				def dayList = []
				dayOfWeekList?.each { day ->
					if(optionsDaily.Days.find{ it.toString() == day.name }) {
						dayList << day.index
					}
				}
				rtn = rtn + ' ? * ' + dayList.join(',')
			}
		} else if(optionsMonthly['@Enabled'] == 'true') {
			def timeOffset = optionsMonthly.TimeOffsetUtc?.toLong()
			def hour = ((int)(timeOffset.div(3600l)))
			def minute = ((int)((timeOffset - (hour * 3600l)).div(60)))
			def day = optionsMonthly.DayOfMonth
			//cron can't handle the other style - fourth saturday of month
			//build the string
			rtn = '0 ' + minute + ' ' + hour + ' ' + day
			//get the days of the month
			def months = []
			monthList?.each { month ->
				if(optionsMonthly.Months.find { it.toString() == month.name })
					months << month.index
			}
			if(months?.size() == 12) {
				rtn = rtn + ' ' + '*'
			} else {
				rtn = rtn + ' ' + months.join(',')
			}
			rtn + ' ?'
		} else if(optionsPeriodically['@Enabled']== 'true') {
			//add continuously support
			def hour = optionsPeriodically.FullPeriod
			//build the string
			rtn = '0 0 ' + hour + ' * * ?'
		}
		return rtn
	}
//
//	//api access
//	static parseEntityId(String href) {
//		def rtn
//		def firstSlash = href.lastIndexOf('/')
//		if(firstSlash > -1) {
//			def firstQuestion = href.indexOf('?')
//			if(firstQuestion > -1)
//				rtn = href.substring(firstSlash + 1, firstQuestion)
//			else
//				rtn = href.substring(firstSlash + 1)
//		}
//		return rtn
//	}
//
	static extractVeeamUuid(String url) {
		def rtn = url
		def lastSlash = rtn?.lastIndexOf('/')
		if(lastSlash > -1)
			rtn = rtn.substring(lastSlash + 1)
		def lastQuestion = rtn?.lastIndexOf('?')
		if(lastQuestion > -1)
			rtn = rtn.substring(0, lastQuestion)
		def lastColon = rtn?.lastIndexOf(':')
		if(lastColon > -1)
			rtn = rtn.substring(lastColon + 1)
		return rtn
	}

//	static extractMOR(String uuid) {
//		def rtn = uuid
//		def lastPeriod = rtn?.lastIndexOf('.')
//		if(lastPeriod > -1)
//			rtn = rtn.substring(lastPeriod + 1)
//		def lastQuestion = rtn?.lastIndexOf('?')
//		if(lastQuestion > -1)
//			rtn = rtn.substring(0, lastQuestion)
//
//		return rtn
//	}

	static xmlToMap(String xml, Boolean camelCase = false) {
		def rtn = xml ? xmlToMap(new groovy.util.XmlSlurper().parseText(xml), camelCase) : [:]
	}

	static xmlToMap(groovy.util.slurpersupport.NodeChild node, Boolean camelCase = false) {
		def rtn = [:]
		def children = node?.children()
		def attributeMap = node?.attributes()
		if(children) {
			children.each { child ->
				//node name
				def childName = child.name()
				if(camelCase == true)
					childName = getCamelKeyName(childName)
				//get value
				def childAttributeMap = child.attributes()
				if(child.childNodes()) {
					def childResult = xmlToMap(child, camelCase)
					setMapXmlValue(rtn, childName, childResult, null)
					//has sub stuff
				} else if(childAttributeMap?.size() > 0) {
					if(camelCase == true) {
						def cloneMap = [:]
						childAttributeMap.each { key, value ->
							def keyName = getCamelKeyName(key)
							cloneMap[keyName] = value
						}
						setMapXmlValue(rtn, childName, cloneMap, child.text())
					} else {
						setMapXmlValue(rtn, childName, childAttributeMap, child.text())
					}
				} else {
					//just plain old value
					setMapXmlValue(rtn, childName, child.text(), null)
				}
			}
		}
		//attributes
		if(attributeMap?.size() > 0) {
			if(camelCase == true) {
				def cloneMap = [:]
				attributeMap.each { key, value ->
					def keyName = getCamelKeyName(key)
					cloneMap[keyName] = value
					rtn += cloneMap
				}
			} else {
				rtn += attributeMap
			}
		}
		return rtn
	}

	static getCamelKeyName(String key) {
		def rtn
		if(key == 'UID')
			rtn = 'uid'
		else if(key == 'ID')
			rtn = 'id'
		else
			rtn = lowerCamelCase(key)
		//return
		return rtn
	}

	static setMapXmlValue(Map target, String name, Object value, Object extraValue) {
		def current = target[name]
		if(current == null) {
			target[name] = value
			if(extraValue)
				value.value = extraValue
		} else {
			if(!(current instanceof List)) {
				target[name] = []
				target[name] << current
			}
			target[name] << value
			if(extraValue)
				value.value = extraValue
		}
	}

	private static lowerCamelCase( String lowerCaseAndUnderscoredWord) {
		return camelCase(lowerCaseAndUnderscoredWord,false);
	}

	private static camelCase( String lowerCaseAndUnderscoredWord, boolean uppercaseFirstLetter) {
		if (lowerCaseAndUnderscoredWord == null) return null;
		lowerCaseAndUnderscoredWord = lowerCaseAndUnderscoredWord.trim();
		if (lowerCaseAndUnderscoredWord.length() == 0) return "";
		if (uppercaseFirstLetter) {
			String result = lowerCaseAndUnderscoredWord;
			// Change the case at the beginning at after each underscore ...
			return replaceAllWithUppercase(result, "(^|_)(.)", 2);
		}
		if (lowerCaseAndUnderscoredWord.length() < 2) return lowerCaseAndUnderscoredWord;
		return "" + Character.toLowerCase(lowerCaseAndUnderscoredWord.charAt(0)) + camelCase(lowerCaseAndUnderscoredWord, true).substring(1);
	}

	private static String replaceAllWithUppercase( String input, String regex, int groupNumberToUppercase ) {
		Pattern underscoreAndDotPattern = Pattern.compile(regex);
		Matcher matcher = underscoreAndDotPattern.matcher(input);
		StringBuffer sb = new StringBuffer();
		while (matcher.find()) {
			matcher.appendReplacement(sb, matcher.group(groupNumberToUppercase).toUpperCase());
		}
		matcher.appendTail(sb);
		return sb.toString();
	}

}
