package com.morpheusdata.veeam.services

import com.morpheusdata.core.Plugin
import com.morpheusdata.core.util.DateUtility
import com.morpheusdata.core.util.HttpApiClient
import com.morpheusdata.model.BackupProvider
import com.morpheusdata.veeam.utils.VeeamUtils
import com.morpheusdata.veeam.utils.VeeamScheduleUtils
import com.morpheusdata.veeam.utils.XmlUtils
import groovy.util.logging.Slf4j
import groovy.xml.StreamingMarkupBuilder

@Slf4j
class ApiService {

	Plugin plugin

	static taskSleepInterval = 5l * 1000l //5 seconds
	static maxTaskAttempts = 36

	ApiService(Plugin plugin) {
		this.plugin = plugin
	}

	def getApiUrl(BackupProvider backupProvider) {
		backupProvider.serviceUrl
		def scheme = backupProvider.host.contains('http') ? '' : 'http://'
		def apiUrl = "${scheme}${backupProvider.host}:${backupProvider.port}"
		return apiUrl.toString()
	}

	Map getAuthConfig(BackupProvider backupProviderModel) {
		if(!backupProviderModel.account) {
			backupProviderModel = this.plugin.morpheusContext.services.backupProvider.get(backupProviderModel.id)
		}
		def newBackupProvider = this.plugin.loadCredentials(backupProviderModel)
		log.debug("newBackupProvider, credsLoaded: ${newBackupProvider.credentialLoaded}, credentialData: ${newBackupProvider.credentialData}")

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
		log.debug("authConfig: ${authConfig}")
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
			def apiPath = authConfig.basePath + '/sessionMngr'
			def headers = buildHeaders([:], null)
			log.debug("tokenHeaders: ${headers}, authConfig: ${authConfig}")
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
			def results = httpApiClient.callXmlApi(authConfig.apiUrl, apiPath, requestOpts, 'GET')
			log.debug("Supported API Versions results: ${results}")
			if(results.success == true) {
				results.data.SupportedVersions.SupportedVersion.each { supportedVersion ->
					log.debug("Veeam support API versions: ${supportedVersion}")
					def row = XmlUtils.xmlToMap(supportedVersion, true)
					row.version = row.name?.replace('v', '')?.replace('_', '.')?.toFloat()
					rtn.data << row
				}
				rtn.success = true
			}
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

	static listBackupJobs(Map authConfig, Map opts=[:]) {
		log.debug "listBackupJobs: ${authConfig}"
		def rtn = [success:false, jobs:[]]
		def tokenResults = getToken(authConfig)
		if(tokenResults.success == true) {
			def apiPath = authConfig.basePath + '/jobs'
			def headers = buildHeaders([:], tokenResults.token)
			def page = '1'
			def perPage = '50'
			def query = [format:'Entity', pageSize:perPage, page:page]
			if(opts.backupType)
				query.filter = 'Platform==' + opts.backupType
			def keepGoing = true
			while(keepGoing) {
				HttpApiClient.RequestOptions requestOpts = new HttpApiClient.RequestOptions(headers:headers, queryParams:query)
				HttpApiClient httpApiClient = new HttpApiClient()
				def results = httpApiClient.callXmlApi(authConfig.apiUrl, apiPath, null, null, requestOpts, 'GET')
				// log.debug("List Backup Jobs result: ${results}")
				if(results.success == true) {
					//iterate results
					results.data.Job?.each { job ->
						def row = XmlUtils.xmlToMap(job, true)
						row.externalId = row.uid
						row.scheduleCron = VeeamScheduleUtils.decodeScheduling(job)

						rtn.jobs << row
					}
					//paging
					if(results.data.PagingInfo?.size() > 0 && results.data.PagingInfo['@PageNum']?.toInteger() < results.data.PagingInfo['@PagesCount']?.toInteger()) {
						query.page = (results.data.PagingInfo['@PageNum']?.toInteger() + 1).toString()
						keepGoing = true
					} else {
						keepGoing = false
						// we've iterated all pages successfully
						rtn.success = true
					}
				} else {
					keepGoing = false
				}
			}
		} else {
			//return token errors?

		}
		return rtn
	}

	static listManagedServers(Map authConfig, Map opts=[:]) {
		def rtn = [success:false, managedServers:[]]
		def tokenResults = getToken(authConfig)
		if(tokenResults.success == true) {
			def apiPath = authConfig.basePath + '/managedServers'
			def headers = buildHeaders([:], tokenResults.token)
			def page = '1'
			def perPage = '50'
			def query = [format:'Entity', pageSize:perPage, page:page]
			if(opts.managedServerType)
				query.filter = 'managedServerType==' + opts.managedServerType
			def keepGoing = true
			while(keepGoing) {
				HttpApiClient.RequestOptions requestOpts = new HttpApiClient.RequestOptions(headers:headers, queryParams: query)
				HttpApiClient httpApiClient = new HttpApiClient()
				def results = httpApiClient.callXmlApi(authConfig.apiUrl, apiPath, null, null, requestOpts, 'GET')
				if(results.success == true) {
					results.data.ManagedServer?.each { managedServer ->
						def row = XmlUtils.xmlToMap(managedServer, true)
						row.externalId = row.uid
						rtn.managedServers << row
					}
					//paging
					if(results.data.PagingInfo?.size() > 0 && results.data.PagingInfo['@PageNum']?.toInteger() < results.data.PagingInfo['@PagesCount']?.toInteger()) {
						query.page = (results.data.PagingInfo['@PageNum']?.toInteger() + 1).toString()
						keepGoing = true
					} else {
						keepGoing = false
						// we've iterated all pages successfully
						rtn.success = true
					}
				} else {
					keepGoing = false
				}
			}
		} else {
			//return token errors?
		}
		return rtn
	}
//
	static getManagedServerRoot(Map authConfig, String name) {
		def rtn = [success:false]
		try {
			def tokenResults = getToken(authConfig)
			def headers = buildHeaders([:], tokenResults.token)
			def query = [type: 'HierarchyRoot', filter: "Name==\"${name}\"", format:"Entities", pageSize:'1']
			HttpApiClient httpApiClient = new HttpApiClient()
			HttpApiClient.RequestOptions requestOpts = new HttpApiClient.RequestOptions(headers:headers, queryParams: query)
			def results = httpApiClient.callXmlApi(authConfig.apiUrl, "/api/query", null, null, requestOpts, 'GET')
			rtn.success = results.success
			if(rtn.success) {
				def hRoot = results.data.Entities.HierarchyRoots.HierarchyRoot.getAt(0)
				if(hRoot) {
					rtn.data = [
							id: hRoot.HierarchyRootId.toString(),
							uid: hRoot["@UID"].toString(),
							uniqueId: hRoot.UniqueId.toString(),
							name: hRoot["@Name"].toString(),
							hostType: hRoot.HostType.toString(),
							links: []
					]
					hRoot.Links.Link.each {
						rtn.data.links << [
								name:it["@Name"].toString(),
								type:it["@Type"].toString(),
								rel:it["@Rel"].toString(),
								href: it["@Href"].toString()
						]
					}
				}
			}
			log.debug("managed server root query results: ${rtn}")
		} catch(Exception e) {
			log.error("getManagedServerRoot error: ${e}", e)
		}

		return rtn
	}

	static listBackupServers(Map authConfig, Map opts=[:] ) {
		def rtn = [success:false, backupServers:[]]
		def tokenResults = getToken(authConfig)
		if(tokenResults.success == true) {
			def apiPath = authConfig.basePath + '/backupServers'
			def headers = buildHeaders([:], tokenResults.token)
			def page = '1'
			def perPage = '50'
			def query = [format:'Entity', pageSize:perPage, page:page]
			if(opts.managedServerType)
				query.filter = 'managedServerType==' + opts.managedServerType
			def keepGoing = true
			while(keepGoing) {
				HttpApiClient.RequestOptions requestOpts = new HttpApiClient.RequestOptions(headers:headers, queryParams: query)
				HttpApiClient httpApiClient = new HttpApiClient()
				def results = httpApiClient.callXmlApi(authConfig.apiUrl, apiPath, null, null, requestOpts, 'GET')
				if(results.success == true) {
					//iterate results
					results.data.BackupServer?.each { backupServer ->
						def row = XmlUtils.xmlToMap(backupServer, true)
						row.externalId = row.uid
						rtn.backupServers << row
					}
					//paging
					if(results.data.PagingInfo?.size() > 0 && results.data.PagingInfo['@PageNum']?.toInteger() < results.data.PagingInfo['@PagesCount']?.toInteger()) {
						query.page = (results.data.PagingInfo['@PageNum']?.toInteger() + 1).toString()
						keepGoing = true
					} else {
						keepGoing = false
						// we've iterated all pages successfully
						rtn.success = true
					}
				} else {
					keepGoing = false
				}
			}
		} else {
			//return token errors?
		}

		return rtn
	}

	static loadBackupJob(Map authConfig, String jobId, Map opts) {
		def rtn = [success:false, job:null, data:null]
		def tokenResults = getToken(authConfig)
		if(tokenResults.success == true) {
			def apiPath = authConfig.basePath + '/jobs/' + jobId
			def headers = buildHeaders([:], tokenResults.token)
			def query = [format:'Entity']
			HttpApiClient.RequestOptions requestOpts = new HttpApiClient.RequestOptions(headers:headers, queryParams: query)
			HttpApiClient httpApiClient = new HttpApiClient()
			def results = httpApiClient.callXmlApi(authConfig.apiUrl, apiPath, null, null, requestOpts, 'GET')
			if(results.success == true) {
				rtn.data = results.data
				rtn.job = XmlUtils.xmlToMap(results.data, true)
				rtn.success = true
			}
		} else {
			//return token errors?
		}
		return rtn
	}

	static getBackupRepositories(Map authConfig, Map opts=[:]) {
		def rtn = [success:false, repositories:[]]
		def tokenResults = getToken(authConfig)
		if(tokenResults.success == true) {
			def apiPath = authConfig.basePath + '/repositories'
			def headers = buildHeaders([:], tokenResults.token)
			def page = '1'
			def perPage = '50'
			def query = [format:'Entity', pageSize:perPage, page:page]
			if(opts.backupType)
				query.filter = 'Platform==' + opts.backupType
			def keepGoing = true
			while(keepGoing) {
				HttpApiClient.RequestOptions requestOpts = new HttpApiClient.RequestOptions(headers:headers, queryParams: query)
				HttpApiClient httpApiClient = new HttpApiClient()
				def results = httpApiClient.callXmlApi(authConfig.apiUrl, apiPath, null, null, requestOpts, 'GET')
				if(results.success == true) {
					//iterate results
					results.data.Repository?.each { repository ->
						def row = XmlUtils.xmlToMap(repository, true)
						row.externalId = row.uid
						rtn.repositories << row
					}
					//paging
					if(results.data.PagingInfo?.size() > 0 && results.data.PagingInfo['@PageNum']?.toInteger() < results.data.PagingInfo['@PagesCount']?.toInteger()) {
						query.page = (results.data.PagingInfo['@PageNum']?.toInteger() + 1).toString()
						keepGoing = true
					} else {
						keepGoing = false
						// we've iterated all pages successfully
						rtn.success = true
					}
				} else {
					keepGoing = false
				}
			}
		} else {
			//return token errors?

		}
		return rtn
	}

	static getBackupJob(url, token, backupJobId){
		log.debug("getBackupJob: ${backupJobId}")
		def rtn = [success:false]
		def headers = buildHeaders([:], token)
		def query = [format: "Entity"]
		HttpApiClient httpApiClient = new HttpApiClient()
		HttpApiClient.RequestOptions requestOpts = new HttpApiClient.RequestOptions(headers:headers, queryParams: query)
		def results = httpApiClient.callXmlApi(url, "/api/jobs/${backupJobId}", null, null, requestOpts, 'GET')
		rtn.results = results
		log.debug("got: ${results}")
		rtn.success = results?.success
		if(results.success) {
			def job = new groovy.util.XmlSlurper().parseText(results.content)
			def name = job['@Name'].toString()
			rtn.jobId = backupJobId
			rtn.jobName = name
			rtn.scheduleEnabled = job.ScheduleEnabled.toString()
			rtn.scheduleCron = VeeamScheduleUtils.decodeScheduling(job)
		} else {
			rtn.content = results.content
			rtn.data = results.data
			rtn.errorCode = results.errorCode
			rtn.headers = results.headers
		}

		return rtn
	}

	static getBackupJobJson(url, token, backupJobId){
		def rtn = [success:false]
		def headers = buildHeaders([:], token)
		def query = [format: "Entity"]
		HttpApiClient httpApiClient = new HttpApiClient()
		HttpApiClient.RequestOptions requestOpts = new HttpApiClient.RequestOptions(headers:headers, queryParams: query)
		def results = httpApiClient.callJsonApi(url, "/api/jobs/${backupJobId}", requestOpts, 'GET')
		log.debug("getBackupJobJson got: ${results}")
		rtn.success = results?.success
		if(rtn.success) {
			rtn.data = results.data
		} else {
			rtn.content = results.content
			rtn.data = results.data
			rtn.errorCode = results.errorCode
			rtn.headers = results.headers
		}
		return rtn
	}

	static updateBackupJob(String url, String token, String backupJobId, Map config) {
		def headers = buildHeaders([:], token)
		def query = [format: "Entity"]
		def body = config
		HttpApiClient httpApiClient = new HttpApiClient()
		HttpApiClient.RequestOptions requestOpts = new HttpApiClient.RequestOptions(headers:headers, queryParams: query, body:body)
		def results = httpApiClient.callJsonApi(url, "/api/jobs/${backupJobId}", requestOpts, 'PUT')
		log.debug("updateBackupJob results: ${results}")

		return results
	}

	static getBackupJobBackups(url, token, backupJobId){
		def rtn = [success:false]
		def headers = buildHeaders([:], token)
		def query = [format:'Entity']
		HttpApiClient.RequestOptions requestOpts = new HttpApiClient.RequestOptions(headers:headers, queryParams: query)
		HttpApiClient httpApiClient = new HttpApiClient()
		def results = httpApiClient.callXmlApi(url, "/api/jobs/${backupJobId}/includes", null, null, requestOpts, 'GET')
		rtn.taskId = results.data.taskId
		rtn.data = []
		results.data.ObjectInJob.each { vmInJob ->
			rtn.data << [objectId: vmInJob.ObjectInJobId, objectRef: vmInJob.HierarchyObjRef, name: vmInJob.Name]
		}
		rtn.success = results?.success
		return rtn
	}

	static cloneBackupJob(Map authConfig, String cloneId, Map opts) {
		log.info("cloneBackupJob: ${opts}")
		def rtn = [success:false, jobId:null, data:null]
		def tokenResults = getToken(authConfig)
		log.debug("cloneBackupJob tokenResults: ${tokenResults}")
		if(tokenResults.success == true) {
			def sourceJob = getBackupJobJson(authConfig.apiUrl, tokenResults.token, cloneId)?.data
			def apiPath = authConfig.basePath + '/jobs/' + cloneId
			def headers = buildHeaders([:], tokenResults.token)
			def query = [action:'clone']
			def jobName = opts.jobName
			def repositoryId = opts.repositoryId
			def requestXml = new StreamingMarkupBuilder().bind() {
				JobCloneSpec("xmlns":"http://www.veeam.com/ent/v1.0", "xmlns:xsd":"http://www.w3.org/2001/XMLSchema", "xmlns:xsi":"http://www.w3.org/2001/XMLSchema-instance") {
					"BackupJobCloneInfo"() {
						"JobName"(jobName)
						"RepositoryUid"(repositoryId)
					}
				}
			}
			HttpApiClient.RequestOptions requestOpts = new HttpApiClient.RequestOptions(headers:headers, queryParams: query, body:requestXml.toString())
			HttpApiClient httpApiClient = new HttpApiClient()
			log.debug("requestOpts: ${requestOpts}")
			def results = httpApiClient.callXmlApi(authConfig.apiUrl, apiPath, null, null, requestOpts, 'POST')
			log.debug("clone results: ${results}")
			if(results.success == true) {
				rtn.data = results.data
				def taskId = results.data?.TaskId.toString()
				if(taskId) {
					def taskResults = waitForTask(authConfig, taskId)
					log.debug("taskResults: ${taskResults}")
					if(taskResults.success == true) {
						//find the job link
						def clonedJobInfo = null
						def jobLink = taskResults.links?.find{ it.type == 'Job' }
						if(jobLink) {
							rtn.jobId = VeeamUtils.parseEntityId(jobLink.href)
							clonedJobInfo = getBackupJobJson(authConfig.apiUrl, tokenResults.token, rtn.jobId)?.data
							rtn.scheduleCron = clonedJobInfo.scheduleCron
							rtn.success = true
						} else {
							rtn.msg = taskResults.msg
							log.error("Error cloning job: ${taskResults.msg}")
						}

						// copy retention info to the new job (not done by the job clone API)
						if(sourceJob.jobInfo && clonedJobInfo) {
							def updateJobConfig = clonedJobInfo
							updateJobConfig.jobInfo.SimpleRetentionPolicy = sourceJob.jobInfo.SimpleRetentionPolicy
							updateBackupJob(authConfig.apiUrl, tokenResults.token, updateJobConfig)
						}
						
						// ensure the job is enabled if the source job was enabled
						if(sourceJob.scheduleEnabled == "true" && rtn.jobId) {
							// The only way I found to ensure a job is enabled after cloning is to first
							// disable it and then enable it.
							def disableScheduleResults = disableBackupJobSchedule(authConfig.apiUrl, tokenResults.token, rtn.jobId)
							if(disableScheduleResults.taskId) {
								waitForTask(authConfig, disableScheduleResults.taskId)
							}
						}
					} else {
						rtn.msg = taskResults.msg
						log.error("Error cloning job: ${taskResults.msg}")
					}
				}
			}
		}
		return rtn
	}

	//vm utils
	static findVm(Map authConfig, List vmObjectRefs, Map opts) {
		log.debug("findVm, vmObjectRefs: ${vmObjectRefs}, opts: ${opts}")
		def rtn = [success:false, vmId:null]
		def tokenResults = getToken(authConfig)
		if(tokenResults.success == true) {
			//search all servers
			vmObjectRefs?.each { vmObjectRef ->
				if(rtn.success != true) {
					def results = lookupVm(authConfig.apiUrl, tokenResults.token, vmObjectRef)
					//check results
					log.debug("findVm results: ${results}")
					if(results.success == true) {
						rtn.vmId = results.vmId
						rtn.vmName = results.vmName
						rtn.success = true
					}
				}
			}
		}

		return rtn
	}

	/**
	 * This method waits for a VM to be available in the Veeam backup server.
	 * It continuously checks for the VM's availability until a maximum number of attempts is reached.
	 *
	 * @param authConfig A map containing the authentication configuration for the Veeam server.
	 *                   This includes the API URL, base path, and the token.
	 * @param vmExternalId The external ID of the VM to wait for.
	 * @param managedServers A list of managed servers in the Veeam backup server that the VM could be associated with.
	 * @param maxWaitTime The maximum time (in seconds) to wait for the VM to be available.
	 *                    The method will stop checking after this time has passed.
	 * @param opts Additional options for the method. This can include various configuration parameters.
	 * @return A map containing the result of the operation.
	 *         This includes a success flag and the VM ID if the VM was found.
	 */
	static waitForVm(Map authConfig, List vmObjectRefs, Long maxWaitTime = 300, Map opts) {
		def rtn = [success:false, error:false, data:null, vmId:null]
		def attempt = 0
		def keepGoing = true
		def maxAttempts = (maxWaitTime * 1000l) / taskSleepInterval
		while(keepGoing == true && attempt < maxAttempts) {
			//load the vm
			def results = findVm(authConfig, vmObjectRefs, opts)
			if(results.success == true) {
				rtn.success = true
				rtn.data = results.data
				rtn.vmId = results.vmId
				rtn.vmName = results.vmName
				rtn.hierarchyRoot = results.hierarchyRoot
				keepGoing = false
			} else {
				attempt++
				sleep(taskSleepInterval)
			}


		}
		return rtn
	}

	//backup
	static createBackupJobBackup(Map authConfig, String jobId, vmId, vmName, Map opts) {
		log.debug("createBackupJobBackup: ${opts}")
		def rtn = [success:false, backupId:null, data:null]
		def tokenResults = getToken(authConfig)
		if(tokenResults.success == true) {
			def apiPath = authConfig.basePath + '/jobs/' + jobId + '/includes'
			def headers = buildHeaders([:], tokenResults.token)
			def query = [format:'Entity']
			HttpApiClient.RequestOptions requestOpts = new HttpApiClient.RequestOptions(headers:headers, queryParams: query)
			HttpApiClient httpApiClient = new HttpApiClient()

			//load up the existing job
			def results = httpApiClient.callXmlApi(authConfig.apiUrl, apiPath, null, null, requestOpts, 'GET')
			log.info("job results: ${results}")
			if(results.success == true) {
				//save off the existing
				def existingItems = []
				def processingOpts
				results.data.ObjectInJob.each { vmInJob ->
					if(vmInJob.ObjectInJobId) {
						existingItems << vmInJob.ObjectInJobId.toString()
						if(processingOpts == null)
							processingOpts = vmInJob.GuestProcessingOptions
					}
				}
				//add new
				def requestXml = new StreamingMarkupBuilder().bind {
					CreateObjectInJobSpec('xmlns':'http://www.veeam.com/ent/v1.0', 'xmlns:xsd':'http://www.w3.org/2001/XMLSchema', 'xmlns:xsi':'http://www.w3.org/2001/XMLSchema-instance') {
						'HierarchyObjRef'(vmId)
						'HierarchyObjName'(vmName)
					}
				}
				//add it
				log.debug("requestOpts: ${requestOpts}")
				requestOpts = new HttpApiClient.RequestOptions(headers:headers, queryParams: query, body: requestXml.toString())
				def addResults = httpApiClient.callXmlApi(authConfig.apiUrl, apiPath, null, null, requestOpts, 'POST')
				log.debug("addResults: ${addResults}")
				if(addResults.success == true) {
					//get task id and wait
					def taskId = addResults.data?.TaskId.toString()
					log.debug("taskId: ${taskId}")
					if(taskId) {
						def taskResults = waitForTask(authConfig, taskId)
						if(taskResults.success == true) {
							rtn.success = true
							log.debug("taskResults: ${taskResults}")
							//get the backup id?
							//remove existing?
							if(opts.removeJobs == true) {
								requestOpts = new HttpApiClient.RequestOptions(headers:headers)
								existingItems.each { existingId ->
									def itemPath = apiPath + '/' + existingId
									def deleteResults = httpApiClient.callXmlApi(authConfig.apiUrl, itemPath, null, null, requestOpts, 'DELETE')
									def deleteTaskId = deleteResults.data?.TaskId?.toString()
									if(deleteTaskId) {
										def deleteTaskResults = waitForTask(authConfig, deleteTaskId)
										log.debug("deleteResults: ${deleteResults}")
									}
								}
							}
							//get the job and id
							def jobObject
							def jobDetailAttempts = 0
							def maxJobDetailAttempts = 10
							while(!jobObject && jobDetailAttempts < maxJobDetailAttempts) {
								def jobResults = loadBackupJob(authConfig, jobId, opts)
								if(jobResults.success == true) {
									if(jobResults.job.jobInfo?.backupJobInfo?.includes?.objectInJob instanceof Map) {
										log.debug("ONLY FOUND ONE OBJECT IN JOB")
										def tmpJobObj = jobResults.job.jobInfo?.backupJobInfo?.includes?.objectInJob
										if(opts.externalId) {
											def jobObjMor = VeeamUtils.extractVmIdFromObjectRef(tmpJobObj.hierarchyObjRef)
											def jobObjUid = VeeamUtils.extractVeeamUuid(tmpJobObj.hierarchyObjRef)
											if(opts.externalId == jobObjMor || opts.externalId.contains(jobObjUid)) {
												jobObject = tmpJobObj
											}
										}
									} else {
										log.debug("FOUND MULTIPLE JOB OBJECTS, FIND THE RIGHT ONE")
										jobObject = jobResults.job.jobInfo?.backupJobInfo?.includes?.objectInJob?.find {
											if(opts.externalId) {
												def itMor = VeeamUtils.extractVmIdFromObjectRef(it.hierarchyObjRef)
												return opts.externalId == itMor
											} else {
												return vmName == it.name
											}
										}
									}
									if(rtn.backupId == null && jobObject) {
										log.debug("JOB OBJECT WAS FOUND")
										rtn.backupId = jobObject.objectInJobId
										rtn.objectId = jobResults.job.uid
										rtn.success = true
									}

								} else {
									//couldn't find the job
								}
								if(!jobObject) {
									log.debug("DIDN'T FIND THE JOB OBJECT WE WERE LOOKING FOR")
									jobDetailAttempts++
									sleep(3000)
								}
							}
						} else {
							//error waiting for task to finish
							rtn.msg = taskResults.msg ?: "Failed to find backup creation task in Veeam."
						}
					} else {
						// failed to create task, no task ID returned
						rtn.msg = "Failed to create request for backup creation in Veeam."
						log.error("Failed to create back up include in backup job, task ID not found in API response: ${addResults}")
					}
				} else {
					//error adding to job
					rtn.msg = "Failed to create backup job."
					log.error("Failed to create backup job: ${addResults}")
				}
			} else {
				rtn.msg = "Unable to load details for job ${jobId}."
				log.error("Failed to load job details: ${results}")
			}

		}
		return rtn
	}

	static startBackupJob(Map authConfig, jobId, opts=[:]){
		log.debug "startBackupJob: ${jobId}"
		def rtn = [success:false]
		def tokenResults = getToken(authConfig)
		if(tokenResults.success == true) {
			def taskId
			def apiPath = authConfig.basePath + '/jobs/' + jobId
			def headers = buildHeaders([:], tokenResults.token)
			def query = [action:'start']
			HttpApiClient.RequestOptions requestOpts = new HttpApiClient.RequestOptions(headers:headers, queryParams: query)
			HttpApiClient httpApiClient = new HttpApiClient()
			def results = httpApiClient.callXmlApi(authConfig.apiUrl, apiPath, null, null, requestOpts, 'POST')
			log.debug("veeam backup start request got: ${results}")
			def jobStartDate = results.headers?.Date
			rtn.success = results?.success
			if (results?.success == true) {
				def response = XmlUtils.xmlToMap(results.data, true)
				taskId = response.taskId
			}
			if (taskId) {
				def taskResults = waitForTask(authConfig, taskId, ['Finished'])
				rtn.success = taskResults?.success
				if(taskResults.success && !taskResults.error) {
					log.debug("backup job task results: ${taskResults}")
					def jobSessionLink = taskResults.links.find { it.type == "BackupJobSession"}?.href
					if(jobSessionLink) {
						rtn.backupSessionId = VeeamUtils.extractVeeamUuid(jobSessionLink)
					} else {
						if(jobStartDate instanceof String) {
							def tmpJobStartDate = DateUtility.parseDate(jobStartDate)
							jobStartDate = DateUtility.formatDate(tmpJobStartDate)
						}
						def backupResult = getLastBackupResult(authConfig, jobId, opts + [startRefDateStr: jobStartDate])
						log.info("got backup result - " + backupResult)
						rtn.backupSessionId = backupResult.backupResult?.backupSessionId
					}
				} else{
					rtn.success = false
					def resultData = XmlUtils.xmlToMap(taskResults.data, true)
					rtn.errorMsg = resultData.result.message
				}
			}
		}
		return rtn
	}

	static stopBackupJob(url, token, backupJobId){
		def rtn = [success:false]
		def taskId = ""
		def headers = buildHeaders([:], token)
		def query = [action:'stop']
		HttpApiClient httpApiClient = new HttpApiClient()
		HttpApiClient.RequestOptions requestOpts = new HttpApiClient.RequestOptions(headers:headers, queryParams: query)
		def results = httpApiClient.callXmlApi(url, "/api/jobs/${backupJobId}", null, null, requestOpts, 'POST')
		log.debug("got: ${results}")
		rtn.success = results?.success
		if(results?.success == true) {
			def response = new groovy.util.XmlSlurper().parseText(results.content)
			rtn.taskId = response.TaskId.toString()
		} else if(results?.errorCode?.toString() == "404") {
			rtn.success = true
		}
		return rtn
	}

	static startQuickBackup(Map authConfig, String backupServerId, String vmId, Map opts = [:]){
		log.debug "startQuickBackup - backupServerId: ${backupServerId}, vmId: ${vmId}"
		def rtn = [success:false]
		def tokenResults = getToken(authConfig)
		if(tokenResults.success == true) {
			def taskId
			def apiPath = authConfig.basePath + "/backupServers/${backupServerId}"
			def headers = buildHeaders([:], tokenResults.token)
			def query = [action:'quickbackup']
			def bodyXml = new StreamingMarkupBuilder().bind() {
				QuickBackupStartupSpec("xmlns":"http://www.veeam.com/ent/v1.0", "xmlns:xsd":"http://www.w3.org/2001/XMLSchema", "xmlns:xsi":"http://www.w3.org/2001/XMLSchema-instance") {
					VmRef(vmId)
				}
			}
			def body = bodyXml.toString()
			HttpApiClient.RequestOptions requestOpts = new HttpApiClient.RequestOptions(headers:headers, queryParams: query, body: body)
			HttpApiClient httpApiClient = new HttpApiClient()
			def results = httpApiClient.callXmlApi(authConfig.apiUrl, apiPath, null, null, requestOpts, 'POST')
			log.debug("veeam quick backup start request got: ${results}")
			def backupStartDate = results.headers.Date
			rtn.success = results?.success
			if (results?.success == true) {
				def response = XmlUtils.xmlToMap(results.data, true)
				taskId = response.taskId
			}
			if (taskId) {
				def taskResults = waitForTask(authConfig, taskId, ['Finished'])
				rtn.success = taskResults?.success
				if(taskResults.success && !taskResults.error) {
					log.debug("quick backup task results: ${taskResults}")
					def jobSessionLink = taskResults.links.find { it.type == "BackupJobSession"}?.href
					if(jobSessionLink) {
						rtn.data = [
							backupSessionId: VeeamUtils.extractVeeamUuid(jobSessionLink),
							startDate: backupStartDate
						]
					} else {
						rtn.success = false
						rtn.errorMsg = "Job session ID not found in Veeam task results."
					}
				} else{
					rtn.success = false
					rtn.status = "FAILED"
					def resultData = XmlUtils.xmlToMap(taskResults.data, true)
					rtn.errorMsg = resultData.result.message
				}
			}
		}
		return rtn
	}

	static startVeeamZip(Map authConfig, String backupServerId, String repositoryId, String vmId, Map opts = [:]){
		log.debug "startVeeamZip - backupServerId: ${backupServerId}, RepositoryId: ${repositoryId}, vmId: ${vmId}"
		def rtn = [success:false]
		def tokenResults = getToken(authConfig)
		if(tokenResults.success == true) {
			def taskId
			def apiPath = authConfig.basePath + "/backupServers/${backupServerId}"
			def headers = buildHeaders([:], tokenResults.token)
			def query = [action:'veeamzip']
			def bodyXml = new StreamingMarkupBuilder().bind() {
				VeeamZipStartupSpec("xmlns":"http://www.veeam.com/ent/v1.0", "xmlns:xsd":"http://www.w3.org/2001/XMLSchema", "xmlns:xsi":"http://www.w3.org/2001/XMLSchema-instance") {
					VmRef(vmId)
					RepositoryUid(repositoryId)
					BackupRetention("Never")
					Compressionlevel(3)
					if(opts.vmwToolsInstalled) {
						// doesn't work well with vmware tools quiescensce
						DisableGuestQuiescence(true)
					}
				}
			}
			def body = bodyXml.toString()
			HttpApiClient.RequestOptions requestOpts = new HttpApiClient.RequestOptions(headers:headers, queryParams: query, body: body)
			HttpApiClient httpApiClient = new HttpApiClient()
			def results = httpApiClient.callXmlApi(authConfig.apiUrl, apiPath, null, null, requestOpts, 'POST')
			log.debug("veeamzip backup start request got: ${results}")
			def backupStartDate = results.headers?.Date
			log.debug("backupStartDate: ${backupStartDate}")
			rtn.success = results?.success
			if (results?.success == true) {
				taskId = results.data.TaskId.toString()
			}
			log.debug("taskId: $taskId")
			if (taskId) {
				def taskResults = waitForTask(authConfig, taskId, ['Finished'])
				rtn.success = taskResults?.success
				if(taskResults.success && !taskResults.error) {
					log.debug("veeamzip task results: ${taskResults}")
					def jobSessionLink = taskResults.links.find { it.type == "BackupJobSession"}?.href
					log.debug("jobSessionLink: ${jobSessionLink}")
					if(jobSessionLink) {
						rtn.data = [
							backupSessionId: VeeamUtils.extractVeeamUuid(jobSessionLink),
							startDate: backupStartDate
						]
					} else {
						rtn.success = false
						rtn.errorMsg = "Job session ID not found in Veeam task results."
					}
				} else{
					rtn.success = false
					rtn.status = "FAILED"
					def resultData = XmlUtils.xmlToMap(taskResults.data, true)
					rtn.errorMsg = resultData.result.message
				}
			}
		}
		return rtn
	}


	//not supported by API
	static deleteBackupJob(url, token, backupJobId) {
		def rtn = [success:false, data:[:]]
		try {
			def headers = buildHeaders([:], token)
			HttpApiClient httpApiClient = new HttpApiClient()
			HttpApiClient.RequestOptions requestOpts = new HttpApiClient.RequestOptions(headers:headers)
			def results = httpApiClient.callXmlApi(url, "/api/jobs/${backupJobId}", null, null, requestOpts, 'DELETE')
			log.debug("got: ${results}")
			rtn.success = results?.success
			if(results?.success == true) {
				def response = new groovy.util.XmlSlurper().parseText(results.content)
				rtn.data.taskId = response.TaskId.toString()
			} else if(results.errorCode?.toString() == "400") {
				rtn.success = true
			}
		} catch(Exception e) {
			log.error("deleteBackupJob error: {}", e, e)
		}

		return rtn
	}

	//turn off backup schedule
	static disableBackupJobSchedule(url, token, backupJobId){
		def rtn = [success:false]
		def taskId = ""
		def backupJob = getBackupJob(url, token, backupJobId)
		if(backupJob.scheduleEnabled == "false" && backupJob.ScheduleConfigured == "false"){
			//schedule is already off
			rtn.success = true
			return rtn
		}

		def bodyXmlBuilder = backupJob.results.data
		bodyXmlBuilder.ScheduleConfigured = "false"
		bodyXmlBuilder.ScheduleEnabled = "false"
		bodyXmlBuilder.jobScheduleOptions = ""

		def headers = buildHeaders([:], token)
		def query = [format: "Entity"]
		def body = new StreamingMarkupBuilder().bindNode(bodyXmlBuilder).toString()
		HttpApiClient httpApiClient = new HttpApiClient()
		HttpApiClient.RequestOptions requestOpts = new HttpApiClient.RequestOptions(headers:headers, queryParams: query, body: body)
		def results = httpApiClient.callXmlApi(url, "/api/jobs/${backupJobId}", null, null, requestOpts, 'PUT')
		log.debug("disableBackupJobSchedule got: ${results}")
		rtn.success = results?.success
		if(results?.success == true) {
			def response = new groovy.util.XmlSlurper().parseText(results.content)
			rtn.taskId = response.TaskId.toString()
		}
		return rtn
	}

	static removeVmFromBackupJob(url, token, backupJobId, vmId) {
		def rtn = [success:false]
		def headers = buildHeaders([:], token)
		def query = [format:'Entity']
		HttpApiClient.RequestOptions requestOpts = new HttpApiClient.RequestOptions(headers:headers, queryParams: query)
		HttpApiClient httpApiClient = new HttpApiClient()
		def apiPath = '/api/jobs/' + backupJobId + '/includes/' + vmId
		def results = httpApiClient.callXmlApi(url, apiPath, null, null, requestOpts, 'DELETE')
		rtn.taskId = results.data?.TaskId.toString()
		rtn.success = results?.success
		rtn.data = results.data
		log.debug "remove vm results ${results}"
		return rtn
	}

	static getBackupResult(url, token, backupSessionId) {
		def rtn = [success:false]
		def backupResult = [:]
		def headers = buildHeaders([:], token, [format:"json"])
		def query = [format: "Entity"]
		HttpApiClient httpApiClient = new HttpApiClient()
		HttpApiClient.RequestOptions requestOpts = new HttpApiClient.RequestOptions(headers:headers, queryParams: query)
		def results = httpApiClient.callJsonApi(url, "/api/backupSessions/${backupSessionId}", requestOpts, 'GET')
		log.debug("getBackupResult got: ${results}")
		rtn.success = results?.success
		if(results?.success == true) {
			backupResult = [
				backupSessionId: backupSessionId,
				backupJobName: results.data?.JobName?.toString(),
				startTime: results.data?.CreationTimeUTC?.toString(),
				endTime: results.data?.EndTimeUTC?.toString(),
				state: results.data?.State?.toString(),
				result: results.data?.Result?.toString(),
				progress: results.data?.Progress?.toString(),
				links: results.data?.Links
			]
			log.debug("getBackupResult Links: ${results.data?.Links}")
			if(backupResult.result == "Success" || backupResult.result == "Warning"){
				def stats = getBackupResultStats(url, token, backupSessionId)
				backupResult.totalSize = stats?.totalSize ?: 0
			}
		}
		rtn.result = backupResult
		return rtn
	}

	static getBackupResults(url, token, backupJobId) {
		def rtn = [success:false]
		def backupJobUid = "urn:veeam:Job:${backupJobId}".toString()
		def backupResults = []
		def headers = buildHeaders([:], token)
		def query = [type:'backupJobSession', filter:"jobUid==\"${backupJobUid}\"", format:'entities']
		HttpApiClient httpApiClient = new HttpApiClient()
		HttpApiClient.RequestOptions requestOpts = new HttpApiClient.RequestOptions(headers:headers, queryParams: query)
		def results = httpApiClient.callXmlApi(url, "/api/query", null, null, requestOpts, 'GET')
		log.debug("got: ${results}")
		rtn.success = results?.success
		if(results?.success == true) {
			def response = new groovy.util.XmlSlurper().parseText(results.content)
			response.Entities.BackupJobSessions.BackupJobSession.each { backupJobSession ->
				def uid = backupJobSession.JobUid.toString()

				def backupJobSessionUid = backupJobSession['@UID'].toString()
				def backupJobSessionId = backupJobSessionUid.substring(backupJobSessionUid.lastIndexOf(":")+1)
				def jobName = backupJobSession.JobName.toString()
				def startTime = backupJobSession.CreationTimeUTC.toString()
				def endTime = backupJobSession.EndTimeUTC.toString()
				def state = backupJobSession.State.toString()
				def result = backupJobSession.Result.toString()
				def progress = backupJobSession.Progress.toString()
				def backupResult = [backupSessionId: backupJobSessionId, backupJobName: jobName, startTime: startTime, endTime: endTime, state: state, result: result, progress: progress]
				if(result == "Success" || result == "Warning"){
					def stats = getBackupResultStats(url, token, backupJobSessionId)
					backupResult.totalSize = stats?.totalSize ?: 0
				}
				backupResults << backupResult
			}
		}
		rtn.results = backupResults
		return rtn
	}

	static getLastBackupResult(Map authConfig, backupJobId, opts=[:]) {
		log.debug "getLastBackupResult: ${backupJobId}"
		def rtn = [success:false]
		def backupResult
		def backupJobUid = "urn:veeam:Job:${VeeamUtils.extractVeeamUuid(backupJobId)}"
		//get hiearchy	 root for the VM cloud
		def tokenResults = getToken(authConfig)
		if(tokenResults.success == true) {
			def apiPath = authConfig.basePath + '/query'
			def headers = buildHeaders([:], tokenResults.token)
			def queryFilter = "jobUid==\"${backupJobUid}\""
			if(opts.startRefDateStr) {
				queryFilter += ";CreationTime>=\"${opts.startRefDateStr}\""
			}
			def query = [type: 'backupJobSession', filter: queryFilter, format: 'entities', sortDesc: 'CreationTime', pageSize: 1 ]
			HttpApiClient.RequestOptions requestOpts = new HttpApiClient.RequestOptions(headers: headers, queryParams: query)
			HttpApiClient httpApiClient = new HttpApiClient()
			log.info("getLastBackupResult query: ${query}")
			def attempt = 0
			def keepGoing = true
			def response
			while(keepGoing == true && attempt < maxTaskAttempts) {
				def results = httpApiClient.callXmlApi(authConfig.apiUrl, apiPath, null, null, requestOpts, 'GET')
				rtn.success = results?.success
				if(rtn.success == true) {
					response = XmlUtils.xmlToMap(results.data, true)
					if(response.entities.backupJobSessions?.size() > 0) {
						def tmpJobSession = response.entities.backupJobSessions.backupJobSession
						def tmpJobSessionUid = tmpJobSession.uid
						def tmpBackupSessionId = tmpJobSessionUid.substring(tmpJobSessionUid.lastIndexOf(":")+1)
						if(!opts.lastBackupSessionId || tmpBackupSessionId != opts.lastBackupSessionId) {
							keepGoing = false
						}
					}

				} else {
					keepGoing = false
					return rtn
				}
				sleep(taskSleepInterval)
				attempt++
			}

			if(response.entities?.backupJobSessions?.size() > 0) {
				def backupJobSession = response.entities.backupJobSessions.backupJobSession
				def backupJobSessionUid = backupJobSession.uid
				def backupJobSessionId = backupJobSessionUid.substring(backupJobSessionUid.lastIndexOf(":") + 1)
				def jobName = backupJobSession.jobName.toString()
				def startTime = backupJobSession.creationTimeUTC.toString()
				def endTime = backupJobSession.endTimeUTC.toString()
				def state = backupJobSession.state.toString()
				def result = backupJobSession.result.toString()
				def progress = backupJobSession.progress.toString()
				backupResult = [backupSessionId: backupJobSessionId, backupJobName: jobName, startTime: startTime, endTime: endTime, state: state, result: result, progress: progress]
			}
			rtn.backupResult = backupResult
		}

		return rtn
	}

	static getBackupSessionTaskSessions(String url, String token, String backupSessionId) {
		def rtn = [success:false]
		String apiPath = "/api/backupSessions/${backupSessionId}/taskSessions"
		Map headers = buildJsonHeaders([:], token)
		Map query = [format: "Entity"]
		HttpApiClient httpApiClient = new HttpApiClient()
		HttpApiClient.RequestOptions requestOpts = new HttpApiClient.RequestOptions(headers:headers, queryParams: query)
		rtn = httpApiClient.callJsonApi(url, apiPath, requestOpts, 'GET')

		return rtn
	}

	static getBackupSession(Map authConfig, String backupSessionId) {
		def rtn = [success:false]
		def tokenResults = getToken(authConfig)
		if(tokenResults.success == true) {
			def headers = buildHeaders([:], tokenResults.token)
			def query = [format: "Entity"]
			HttpApiClient httpApiClient = new HttpApiClient()
			HttpApiClient.RequestOptions requestOpts = new HttpApiClient.RequestOptions(headers:headers, queryParams: query)
			rtn = httpApiClient.callXmlApi(authConfig.apiUrl, "/api/backupSessions/${backupSessionId}", requestOpts, 'GET')
		}
		return rtn
	}

	static getRestorePoint(Map authConfig, String objectRef, Map opts=[:]) {
		log.debug "getLatestRestorePoint: ${objectRef}"
		def rtn = [success:false, data: [:]]
		def tokenResults = getToken(authConfig)
		if(tokenResults.success == true) {
			def headers = buildHeaders([:], tokenResults.token)
			def queryFilter = "HierarchyObjRef==\"${objectRef}\""
			if(opts.startDate) {
				def formattedStartDate= DateUtility.formatDate(opts.startDate)
				queryFilter += ";CreationTime>=\"${formattedStartDate}\""
			}
			log.debug("getRestorePoint queryFilter: ${queryFilter}")
				def query = [type: 'VmRestorePoint', filter: queryFilter, format: 'Entities', sortDesc: 'CreationTime', pageSize: "1" ]

			def attempt = 0
			def keepGoing = true
			while(keepGoing == true && attempt < maxTaskAttempts) {
				HttpApiClient httpApiClient = new HttpApiClient()
				HttpApiClient.RequestOptions requestOpts = new HttpApiClient.RequestOptions(headers:headers, queryParams: query)
				def restorePointsResults = httpApiClient.callXmlApi(authConfig.apiUrl, "/api/query", null, null, requestOpts, 'GET')
				if(restorePointsResults.success) {
					def restorePointsResponse = new groovy.util.XmlSlurper().parseText(restorePointsResults.content)
					def restoreRef = restorePointsResponse.Entities.VmRestorePoints.VmRestorePoint.getAt(0)
					if(restoreRef) {
						rtn.data.externalId = restoreRef["@UID"].toString()
						rtn.success = true
						keepGoing = false
					}
				} else {
					keepGoing = false
					return rtn
				}
				sleep(taskSleepInterval)
				attempt++
			}
		}

		return rtn
	}

	static getVmRestorePointsFromRestorePointId(Map authConfig, String restorePointId, Map opts=[:]) {
		log.debug "getVmRestorePointsFromRestorePointId: ${restorePointId}"
		def rtn = [success:false, data: [:]]
		def tokenResults = getToken(authConfig)
		if(tokenResults.success == true) {
			def headers = buildHeaders([:], tokenResults.token)
			def query = [format: "Entity"]
			HttpApiClient httpApiClient = new HttpApiClient()
			HttpApiClient.RequestOptions requestOpts = new HttpApiClient.RequestOptions(headers:headers, queryParams: query)
			rtn = httpApiClient.callXmlApi(authConfig.apiUrl, "/api/restorePoints/${restorePointId}/vmRestorePoints", requestOpts, 'GET')
		}
		return rtn
	}

	static getRestorePointFromRestorePointId(Map authConfig, String restorePointId) {
		log.debug "getRestorePointFromRestorePointId: ${restorePointId}"
		def rtn = [success:false, data: [:]]
		def tokenResults = getToken(authConfig)
		if(tokenResults.success == true) {
			def headers = buildHeaders([:], tokenResults.token)
			def query = [format: "Entity"]
			HttpApiClient httpApiClient = new HttpApiClient()
			HttpApiClient.RequestOptions requestOpts = new HttpApiClient.RequestOptions(headers:headers, queryParams: query)
			rtn = httpApiClient.callXmlApi(authConfig.apiUrl, "/api/vmRestorePoints/${restorePointId}", requestOpts, 'GET')
		}
		return rtn
	}

	static getRestoreResult(url, token, restoreSessionId) {
		log.debug("getRestoreResult: ${restoreSessionId}, url: ${url}, token: ${token}")
		def rtn = [success:false]
		def restoreResult = [:]
		def headers = buildHeaders([:], token)
		def query = [format: "Entity"]
		HttpApiClient httpApiClient = new HttpApiClient()
		HttpApiClient.RequestOptions requestOpts = new HttpApiClient.RequestOptions(headers:headers, queryParams: query)
		def results = httpApiClient.callXmlApi(url, "/api/restoreSessions/${restoreSessionId}", requestOpts, 'GET')
		log.debug("getRestoreResult results: ${results}")
		rtn.success = results?.success
		if(results?.success == true) {
			def response = new groovy.util.XmlSlurper().parseText(results.content)
			def startTime = response.CreationTimeUTC.toString()
			def endTime = response.EndTimeUTC?.toString()
			def state = response.State.toString()
			def result = response.Result.toString()
			def progress = response.Progress.toString()
			def vmId
			def vmRef = response.RestoredObjRef.toString()
			if(vmRef && vmRef != ""){
				vmId = vmRef?.substring(vmRef?.lastIndexOf(".")+1)
			}
			restoreResult = [restoreSessionId: restoreSessionId, vmId:vmId, startTime: startTime, endTime: endTime, state: state, result: result, progress: progress]
		}
		rtn.result = restoreResult
		return rtn
	}

	//lookup backup task sessions for backup size
	static getBackupResultStats(url, token, backupJobSessionId){
		def rtn = [success:false]
		rtn.totalSize = 0
		def headers = buildHeaders([:], token)
		def query = [format: "Entity"]
		HttpApiClient httpApiClient = new HttpApiClient()
		HttpApiClient.RequestOptions requestOpts = new HttpApiClient.RequestOptions(headers:headers, queryParams: query)
		def results = httpApiClient.callXmlApi(url, "/api/backupSessions/${backupJobSessionId}/taskSessions", null, null, requestOpts, 'GET')
		log.debug("got: ${results}")
		rtn.success = results?.success
		if(rtn.success == true) {
			def totalSize = 0
			def response = new groovy.util.XmlSlurper().parseText(results.content)
			response.BackupTaskSession.each { backupTaskSession ->
				rtn.totalSize += backupTaskSession.TotalSize.toLong()
			}
		}
		return rtn
	}

	// this should just take a restore point or a restore endpoint URL, finding the restore info can go in the restore execution service
	static restoreVM(String url, String token, String restorePath, String restoreSpec, opts=[:]) {
		log.debug("restoreVM: ${url}, ${restorePath}, ${restoreSpec} ${opts}")
		def rtn = [success:false]
		def headers = buildHeaders([:], token)
		def query = [format: "Entity"]
		def restoreTaskId

		// initiate the restore
		if(restorePath) {
			log.debug("Performing restore with endpoint: ${restorePath}")
			def body = restoreSpec
			log.debug("body: ${body}")
			def restoreQuery = query + [action: 'restore']
			HttpApiClient httpApiClient = new HttpApiClient()
			HttpApiClient.RequestOptions requestOpts = new HttpApiClient.RequestOptions(headers:headers, queryParams:restoreQuery, body: body)
			def results = httpApiClient.callXmlApi(url, restorePath, requestOpts, 'POST')
			rtn.success = results?.success
			if(rtn.success == true) {
				def response = new groovy.util.XmlSlurper().parseText(results.content)
				//get the restore session id
				restoreTaskId = response.TaskId
			}
		} else if(!rtn.msg) {
			log.debug("Unable to perform restore, no restore link found.")
			rtn.msg = "Veeam restore link not found"
			rtn.success = false
			log.error(rtn.msg)
		}


		// get the restore task details
		if(restoreTaskId) {
			def restoreTaskResults = waitForTask([token: token, apiUrl: url, basePath:'/api'], restoreTaskId.toString())
			rtn.success = restoreTaskResults?.success
			if(rtn.success == true) {
				restoreTaskResults.links.each{ link ->
					if(link.type == "RestoreSession"){
						def restoreSessionUrl =  new URL(link.href?.toString())
						def restoreSessionId = restoreSessionUrl.path.substring(restoreSessionUrl.path.lastIndexOf("/")+1)
						rtn.restoreSessionId = restoreSessionId
					}
				}
			} else {
				rtn.success = false
				rtn.msg = restoreTaskResults.msg
			}
		}
		return rtn
	}

	//lookup the veeam VM ID by searching a single hierarchy root for the VM name
	static lookupVmByName(url, token, managedServerId, vmName) {
		def rtn = [success:false]
		rtn = getVmIdByName(url, token, managedServerId, vmName)
		if(!rtn.vmId) {
			log.error("Failed to find VM object in Veeam: ${vmName}")
		}
		return rtn
	}

	//get the veeam VM ID given the veeam managed server ID (hiearchy root) and VM name
	static getVmIdByName(url, token, managedServerId, vmName) {
		def rtn = [success:false]
		//find the VM under the VM cloud
		def vmId
		if(managedServerId) {
			def hierarchyRoot = managedServerId.contains("HierarchyRoot") ? managedServerId : "urn:veeam:HierarchyRoot:${managedServerId}"
			def headers = buildHeaders([:], token)
			def query = [host: hierarchyRoot, name: vmName, type: 'Vm']
			HttpApiClient httpApiClient = new HttpApiClient()
			HttpApiClient.RequestOptions requestOpts = new HttpApiClient.RequestOptions(headers:headers, queryParams: query)
			def results = httpApiClient.callXmlApi(url, "/api/lookup", null, null, requestOpts, 'GET')
			log.debug("got vmbyid results: ${results}")
			rtn.success = results?.success
			if(rtn.success == true) {
				def response = new groovy.util.XmlSlurper().parseText(results.content)
				vmId = response.HierarchyItem.ObjectRef.toString()
				rtn.vmId = vmId
			}
		}
		return rtn
	}

	//lookup the veeam VM ID given the veeam managed server and the vmware VM ref ID
	static lookupVm(url, token, vmHierarchyRef) {
		def rtn = [success:false]
		rtn = getVmId(url, token, vmHierarchyRef)
		if(!rtn.vmId){
			log.error("Failed to find VM object in Veeam: ${vmHierarchyRef}")
		}
		return rtn
	}

	//get the veeam VM ID given the veeam managed server ID (hiearchy root) and vmware VM ref ID
	static getVmId(url, token, vmHierachyRef) {
		def rtn = [success:false]
		def headers = buildHeaders([:], token)
		def query = [hierarchyRef: vmHierachyRef]
		log.debug("getVmId query: ${query}")
		HttpApiClient httpApiClient = new HttpApiClient()
		HttpApiClient.RequestOptions requestOpts = new HttpApiClient.RequestOptions(headers:headers, queryParams: query)
		def results = httpApiClient.callXmlApi(url, "/api/lookup", requestOpts, 'GET')
		log.debug("getVmId results: ${results}")
		rtn.success = results?.success
		if(rtn.success == true) {
			def response = new groovy.util.XmlSlurper().parseText(results.content)
			rtn.vmId = results.data.HierarchyItem.ObjectRef.toString()
			rtn.vmName = results.data.HierarchyItem.ObjectName.toString()
		}
		return rtn
	}

	static fetchQuery(Map authConfig, String objType, Map filters, Boolean entityFormat=false, Map opts=[:]) {
		def rtn = [success:false]
		def apiPath = authConfig.basePath + '/query'
		def apiUrl = authConfig.apiUrl
		def headers = buildHeaders([:], authConfig.token)
		def query = [type: objType, filter:""]
		if(entityFormat) {
			query.format = 'entities'
		}
		for(filter in filters) {
			if(query.filter.size() > 0) {
				query.filter += "&"
			}
			query.filter += "${filter.key}==\"${filter.value}\""
		}

		HttpApiClient httpApiClient = new HttpApiClient()
		HttpApiClient.RequestOptions requestOpts = new HttpApiClient.RequestOptions(headers:headers, queryParams: query)
		rtn = httpApiClient.callJsonApi(apiUrl, apiPath, requestOpts, 'GET')
		log.debug("fetchQuery results: ${rtn}")

		return rtn
	}

	//tasks
	static waitForTask(Map authConfig, String taskId, waitForState=['Finished']) {
		def rtn = [success:false, error:false, data:null, state:null, links:[]]
		def apiPath = authConfig.basePath + '/tasks/' + taskId
		def headers = buildHeaders([:], authConfig.token)
		def query = [format:'Entity']
		HttpApiClient.RequestOptions requestOpts = new HttpApiClient.RequestOptions(headers:headers, queryParams: query)
		HttpApiClient httpApiClient = new HttpApiClient()
		def attempt = 0
		def keepGoing = true
		while(keepGoing == true && attempt < maxTaskAttempts) {
			//load the task
			def results = httpApiClient.callXmlApi(authConfig.apiUrl, apiPath, null, null, requestOpts, 'GET')
			//check results
			if(results?.success == true) {
				def taskState = results.data.State.text()
				if(waitForState.contains(taskState)) {
					rtn.success = true
					rtn.data = results.data
					rtn.state = taskState
					keepGoing = false
					//parse results
					def taskSuccess = results.data.Result['@Success']
					if(taskSuccess == 'true') {
						results.data.Links?.Link?.each { link ->
							def linkType = link['@Type']?.toString()
							def linkHref = link['@Href']?.toString()
							if(linkType && linkHref)
								rtn.links << [type: linkType, href: linkHref]
						}
					} else if(taskSuccess == 'false') {
						rtn.success = false
						def msg = results.data?.Result?.Message?.text()
						if(msg?.indexOf('not found') > -1 && attempt < 3) {
							//try again
							sleep(taskSleepInterval)
							keepGoing = true
						} else {
							rtn.msg = msg
							rtn.error = true
							keepGoing = false
						}
					}
				} else {
					sleep(taskSleepInterval)
				}
			} else if(results.errorCode?.toString() == "500") {
				def errorMessage
				try {
					def response = new groovy.util.XmlSlurper(false,true).parseText(results.content)
					errorMessage = response["@Message"]
				} catch (Exception ex1) {
					try {
						// we might encounter json here?
						def response = new groovy.json.JsonSlurper().parseText(results.content)
						errorMessage = response.Message
					} catch (Exception ex2) {
						// if all else fails, just treat it as a string
						errorMessage = results.data?.toString()
					}
				}
				if(errorMessage =~ /^.*?no\s.*?\stask\swith\sid/) {
					// "There is no backup task with id [task-297] in current rest session"
					// the task has completed and cleaned up???
					rtn.success = true
					rtn.error = true
				} else {
					rtn.msg = errorMessage
				}
				keepGoing = false
			} else {
				sleep(taskSleepInterval)
			}
			attempt++
		}
		return rtn
	}
	
	static callXmlApi(Map authConfig, String apiUri, String method='GET', Map opts=[:]) {
		log.debug "callXmlApi: ${apiUri}"
		def rtn = [success:false, data: [:]]
		def tokenResults = getToken(authConfig)
		if(tokenResults.success == true) {
			def uri = new URI(apiUri)
			def headers = buildHeaders([:], tokenResults.token)
			def query = [format: "Entity"]
			HttpApiClient httpApiClient = new HttpApiClient()
			HttpApiClient.RequestOptions requestOpts = new HttpApiClient.RequestOptions(headers:headers, queryParams: query)
			rtn = httpApiClient.callXmlApi(authConfig.apiUrl, uri.path, requestOpts, method)
		}
		return rtn
	}

	static callJsonApi(Map authConfig, String apiUri, String method='GET', Map opts=[:]) {
		log.debug "callJsonApi: ${apiUri}"
		def rtn = [success:false, data: [:]]
		def tokenResults = getToken(authConfig)
		if(tokenResults.success == true) {
			def uri = new URI(apiUri)
			def headers = buildHeaders([:], tokenResults.token, [format:'json'])
			def query = [format: "Entity"]
			HttpApiClient httpApiClient = new HttpApiClient()
			HttpApiClient.RequestOptions requestOpts = new HttpApiClient.RequestOptions(headers:headers, queryParams: query)
			rtn = httpApiClient.callJsonApi(authConfig.apiUrl, uri.path, requestOpts, method)
		}
		return rtn
	}

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
		// retain veeam 11 API functionality
		rtn.'x-api-version' = '1.0-rev2'

		return rtn + headers
	}

	static buildJsonHeaders(Map headers, String token, Map opts=[:]) {
		buildHeaders(headers, token, opts + [format: 'json'])
	}
}
