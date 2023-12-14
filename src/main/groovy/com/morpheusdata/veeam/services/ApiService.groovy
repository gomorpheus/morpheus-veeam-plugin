package com.morpheusdata.veeam.services

import com.morpheusdata.core.util.DateUtility
import com.morpheusdata.core.util.HttpApiClient
import com.morpheusdata.model.BackupProvider
import com.morpheusdata.veeam.utils.VeeamUtils
import com.morpheusdata.veeam.utils.VeeamScheduleUtils
import groovy.util.logging.Slf4j
import groovy.xml.StreamingMarkupBuilder

import java.util.regex.Matcher
import java.util.regex.Pattern

@Slf4j
class ApiService {

	static taskSleepInterval = 5l * 1000l //5 seconds
	static maxTaskAttempts = 36

	def getApiUrl(BackupProvider backupProvider) {
		backupProvider.serviceUrl
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
					log.debug("Veeam support API versions: ${supportedVersion}")
					def row = xmlToMap(supportedVersion, true)
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
						def row = xmlToMap(job, true)
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
						def row = xmlToMap(managedServer, true)
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
						def row = xmlToMap(backupServer, true)
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
				rtn.job = xmlToMap(results.data, true)
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
		def rtn = [success:false]
		def headers = buildHeaders([:], token)
		def query = [format: "Entity"]
		HttpApiClient httpApiClient = new HttpApiClient()
		HttpApiClient.RequestOptions requestOpts = new HttpApiClient.RequestOptions(headers:headers, queryParams: query)
		def results = httpApiClient.callXmlApi(url, "/api/jobs/${backupJobId}", null, null, requestOpts, 'GET')
		rtn.results = results
		log.debug("got: ${results}")
		rtn.success = results?.success
		def job = new groovy.util.XmlSlurper().parseText(results.content)
		def name = job['@Name'].toString()
		rtn.jobId = backupJobId
		rtn.jobName = name
		rtn.scheduleEnabled = job.ScheduleEnabled.toString()
		rtn.scheduleCron = VeeamScheduleUtils.decodeScheduling(job)
		return rtn
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
		if(tokenResults.success == true) {
			def sourceJob = getBackupJob(authConfig.apiUrl, tokenResults.token, cloneId)
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
						def jobLink = taskResults.links?.find{ it.type == 'Job' }
						if(jobLink) {
							rtn.jobId = VeeamUtils.parseEntityId(jobLink.href)
							def jobInfo = getBackupJob(authConfig.apiUrl, tokenResults.token, rtn.jobId)
							rtn.scheduleCron = jobInfo.scheduleCron
							rtn.success = true
						} else {
							rtn.msg = taskResults.msg
							log.error("Error cloning job: ${taskResults.msg}")
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
	static findVm(Map authConfig, String externalId, List managedServers, Map opts) {
		log.debug("findVm: ${externalId}, ManagedServers: ${managedServers}, opts: ${opts}")
		def rtn = [success:false, vmId:null]
		def tokenResults = getToken(authConfig)
		if(tokenResults.success == true) {
			//search all servers
			managedServers?.each { managedServer ->
				if(rtn.success != true) {
					def hostRoot = managedServer.contains("HierarchyRoot") ? managedServer : "urn:veeam:HierarchyRoot:${managedServer}"
					def results = lookupVm(authConfig.apiUrl, tokenResults.token, opts.cloudType, hostRoot, externalId)
					//check results
					log.debug("findVm results: ${results}")
					if(results.success == true) {
						rtn.vmId = results.vmId
						rtn.vmName = results.vmName
						rtn.hierarchyRoot = hostRoot
						rtn.success = true
					}
				}
			}
		}

		return rtn
	}

	static waitForVm(Map authConfig, String vmExternalId, List managedServers, Map opts) {
		def rtn = [success:false, error:false, data:null, vmId:null]
		def attempt = 0
		def keepGoing = true
		while(keepGoing == true && attempt < 2) {
			//load the vm
			def results = findVm(authConfig, vmExternalId, managedServers, opts)
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
	static createBackupJobBackup(Map authConfig, String jobId, List managedServers, Map opts) {
		log.debug("createBackupJobBackup: ${opts}")
		def rtn = [success:false, backupId:null, data:null]
		def tokenResults = getToken(authConfig)
		if(tokenResults.success == true) {
			def apiPath = authConfig.basePath + '/jobs/' + jobId + '/includes'
			def headers = buildHeaders([:], tokenResults.token)
			def query = [format:'Entity']
			HttpApiClient.RequestOptions requestOpts = new HttpApiClient.RequestOptions(headers:headers, queryParams: query)
			HttpApiClient httpApiClient = new HttpApiClient()
			//need managed servers
			def findResults = waitForVm(authConfig, opts.externalId, managedServers, opts)
			log.info("wait results: ${findResults}")
			if(findResults.success == true) {
				def vmId = findResults.vmId
				def vmName = findResults.vmName
				rtn.hierarchyObjecRef = findResults.vmId
				rtn.hierarchyRoot = findResults.hierarchyRoot
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
												def jobObjMor = VeeamUtils.extractMOR(tmpJobObj.hierarchyObjRef)
												def jobObjUid = VeeamUtils.extractVeeamUuid(tmpJobObj.hierarchyObjRef)
												if(opts.externalId == jobObjMor || opts.externalId.contains(jobObjUid)) {
													jobObject = tmpJobObj
												}
											}
										} else {
											log.debug("FOUND MULTIPLE JOB OBJECTS, FIND THE RIGHT ONE")
											jobObject = jobResults.job.jobInfo?.backupJobInfo?.includes?.objectInJob?.find {
												if(opts.externalId) {
													def itMor = VeeamUtils.extractMOR(it.hierarchyObjRef)
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
			} else {
				//count not find the vm
				rtn.msg = "Failed to find VM in Veeam. Ensure the VM is accessible to the Veeam backup server."
				log.error("Unable to locate VM in Veeam: ${findResults}")
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
			def jobStartDate = results.headers.Date
			rtn.success = results?.success
			if (results?.success == true) {
				def response = xmlToMap(results.data, true)
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
					def resultData = xmlToMap(taskResults.data, true)
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
				def response = xmlToMap(results.data, true)
				taskId = response.taskId
			}
			if (taskId) {
				def taskResults = waitForTask(authConfig, taskId, ['Finished'])
				rtn.success = taskResults?.success
				if(taskResults.success && !taskResults.error) {
					log.debug("quick backup task results: ${taskResults}")
					def jobSessionLink = taskResults.links.find { it.type == "BackupJobSession"}?.href
					if(jobSessionLink) {
						rtn.backupSessionId = VeeamUtils.extractVeeamUuid(jobSessionLink)
						rtn.startDate = backupStartDate
					} else {
						rtn.success = false
						rtn.errorMsg = "Job session ID not found in Veeam task results."
					}
				} else{
					rtn.success = false
					rtn.status = "FAILED"
					def resultData = xmlToMap(taskResults.data, true)
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
			def backupStartDate = results.headers.Date
			rtn.success = results?.success
			if (results?.success == true) {
				def response = xmlToMap(results.data, true)
				taskId = response.taskId
			}
			if (taskId) {
				def taskResults = waitForTask(authConfig, taskId, ['Finished'])
				rtn.success = taskResults?.success
				if(taskResults.success && !taskResults.error) {
					log.debug("veeamzip task results: ${taskResults}")
					def jobSessionLink = taskResults.links.find { it.type == "BackupJobSession"}?.href
					if(jobSessionLink) {
						rtn.backupSessionId = VeeamUtils.extractVeeamUuid(jobSessionLink)
						rtn.startDate = backupStartDate
					} else {
						rtn.success = false
						rtn.errorMsg = "Job session ID not found in Veeam task results."
					}
				} else{
					rtn.success = false
					rtn.status = "FAILED"
					def resultData = xmlToMap(taskResults.data, true)
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
			} else if(results.errorCode == 400) {
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
		def headers = buildHeaders([:], token)
		def query = [format: "Entity"]
		HttpApiClient httpApiClient = new HttpApiClient()
		HttpApiClient.RequestOptions requestOpts = new HttpApiClient.RequestOptions(headers:headers, queryParams: query)
		def results = httpApiClient.callXmlApi(url, "/api/backupSessions/${backupSessionId}", null, null, requestOpts, 'GET')
		log.debug("got: ${results}")
		rtn.success = results?.success
		if(results?.success == true) {
			def response = new groovy.util.XmlSlurper().parseText(results.content)
			def jobUid = response.JobUid.toString()
			def jobName = response.JobName.toString()
			def startTime = response.CreationTimeUTC.toString()
			def endTime = response.EndTimeUTC.toString()
			def state = response.State.toString()
			def result = response.Result.toString()
			def progress = response.Progress.toString()
			backupResult = [backupSessionId: backupSessionId, backupJobName: jobName, startTime: startTime, endTime: endTime, state: state, result: result, progress: progress, links: []]
			response.Links.Link.each { link ->
				backupResult.links << [href: link['@Href'], type: link['@Type']]
			}
			if(result == "Success" || result == "Warning"){
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
					response = xmlToMap(results.data, true)
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

	static getRestorePoint(Map authConfig, String objectRef, Map opts=[:]) {
		log.debug "getLatestRestorePoint: ${objectRef}"
		def rtn = [success:false, data: [:]]
		def tokenResults = getToken(authConfig)
		if(tokenResults.success == true) {
			def headers = buildHeaders([:], tokenResults.token)
			def queryFilter = "HierarchyObjRef==\"${objectRef}\""
			if(opts.startRefDateStr) {
				queryFilter += ";CreationTime>=\"${opts.startRefDateStr}\""
			}
			def query = [type: 'VmRestorePoint', filter: queryFilter, format: 'Entities', sortDesc: 'CreationTime', pageSize: 1 ]

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

	static getRestoreResult(url, token, restoreSessionId) {
		def rtn = [success:false]
		def restoreResult = [:]
		def headers = buildHeaders([:], token)
		def query = [format: "Entity"]
		HttpApiClient httpApiClient = new HttpApiClient()
		HttpApiClient.RequestOptions requestOpts = new HttpApiClient.RequestOptions(headers:headers, queryParams: query)
		def results = httpApiClient.callXmlApi(url, "/api/restoreSessions/${restoreSessionId}", null, null, requestOpts, 'GET')
		log.debug("got: ${results}")
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

	static restoreVM(url, token, objectRef, backupSessionId, opts=[:]) {
		def rtn = [success:false]
		def headers = buildHeaders([:], token)
		def query = [format: "Entity"]
		def restoreLink

		if(opts.restoreHref) {
			def restoreType = opts.restoreType
			def uri = new URI(opts.restoreHref)
			HttpApiClient httpApiClient = new HttpApiClient()
			HttpApiClient.RequestOptions requestOpts = new HttpApiClient.RequestOptions(headers:headers, queryParams: query)
			def results = httpApiClient.callXmlApi(url, uri.path, null, null, requestOpts, 'GET')
			log.debug("got: ${results}")
			rtn.success = results?.success
			if(rtn.success == true) {
				def response = new groovy.util.XmlSlurper().parseText(results.content)
				response[restoreType].Links.Link.each { link ->
					if(link['@Type'] == "Restore" || link['@Rel'] == "Restore") {
						def restoreUrl = new URI(link['@Href']?.toString())
						restoreLink = restoreUrl.path
					}
				}
			}
		}

		// for everything not vcd
		// the restore point is set in the backup result config
		if(!restoreLink && (opts.restorePointId || opts.restoreRef)) {
			def restorePointId = opts.restorePointId ?: opts.restoreRef
			HttpApiClient httpApiClient = new HttpApiClient()
			HttpApiClient.RequestOptions requestOpts = new HttpApiClient.RequestOptions(headers:headers, queryParams: query)
			def results = httpApiClient.callXmlApi(url, "/api/restorePoints/${restorePointId}/vmRestorePoints", null, null, requestOpts, 'GET')
			log.debug("got: ${results}")
			rtn.success = results?.success
			if(rtn.success == true) {
				def response = new groovy.util.XmlSlurper().parseText(results.content)
				response.VmRestorePoint.Links.Link.each { link ->
					if(link['@Type'] == "Restore" || link['@Rel'] == "Restore") {
						def restoreUrl = new URI(link['@Href']?.toString())
						restoreLink = restoreUrl.path
					}
				}
			}
		}
		if(opts.vmRestorePointId || (opts.restorePointId && !restoreLink)) {
			HttpApiClient httpApiClient = new HttpApiClient()
			HttpApiClient.RequestOptions requestOpts = new HttpApiClient.RequestOptions(headers:headers, queryParams: query)
			def results = httpApiClient.callXmlApi(url, "/api/vmRestorePoints/${opts.vmRestorePointId ?: opts.restorePointId}", null, null, requestOpts, 'GET')
			log.debug("got: ${results}")
			rtn.success = results?.success
			if(rtn.success == true) {
				def response = new groovy.util.XmlSlurper().parseText(results.content)
				response.Links.Link.each { link ->
					if(link['@Rel'] == "Restore") {
						def restoreUrl = new URI(link['@Href']?.toString())
						restoreLink = restoreUrl.path
					}
				}
			}
		}

		// we only have the backup session, find the restore resources
		if(!restoreLink) {
			HttpApiClient httpApiClient = new HttpApiClient()
			HttpApiClient.RequestOptions requestOpts = new HttpApiClient.RequestOptions(headers:headers, queryParams: query)
			def results = httpApiClient.callXmlApi(url, "/api/backupSessions/${backupSessionId}", null, null, requestOpts, 'GET')
			//find restore points
			def restorePointsLink
			rtn.success = results?.success
			if(rtn.success == true) {
				def response = new groovy.util.XmlSlurper().parseText(results.content)
				response.Links.Link.each { link ->
					if(link['@Type'] == "RestorePointReference") {
						def restorePointsUrl = new URI(link['@Href'].toString())
						restorePointsLink = "${restorePointsUrl.path}/vmRestorePoints"
					}
					if(link['@Type'] == "VmRestorePoint") {
						def restoreUrl = new URI(link['@Href']?.toString())
						restorePointsLink = restoreUrl.path
					}
				}
			}


			// probably a veeamzip, need to go find the restore point for the backup session
			if(!restorePointsLink) {
				def response = xmlToMap(results.content, true)
				def backupName = response.jobName // the backup session and the backup(result) should have the same name
				def backupResults = fetchQuery(opts.authConfig, "Backup", [Name: backupName])
				def restoreRefList = backupResults.data.refs?.ref?.links?.link?.find { it.type == "RestorePointReferenceList" }
				// get a list of restore points from the backup
				def refListLink = new URI(restoreRefList.href)
				requestOpts = new HttpApiClient.RequestOptions(headers:headers, queryParams: query)
				def refListResults = httpApiClient.callXmlApi(url, null, null, refListLink.path,requestOpts, 'GET')
				rtn.success = refListResults?.success
				if(rtn.success == true) {
					// we need the vm restore point to execute the restore
					def refListResponse = xmlToMap(refListResults.content)
					refListResponse.RestorePoint.Links.Link.each { link ->
						if(link['Type'] == "VmRestorePointReferenceList") {
							def restoreUrl = new URI(link['Href']?.toString())
							restorePointsLink = restoreUrl.path
						}
					}
				}
			}


			if(restorePointsLink) {
				requestOpts = new HttpApiClient.RequestOptions(headers:headers, queryParams: query)
				def restoreLinkResults = httpApiClient.callXmlApi(url, "${restorePointsLink}", null, null, requestOpts, 'GET')
				log.debug("got: ${results}")
				rtn.success = results?.success
				if(rtn.success == true) {
					def response = new groovy.util.XmlSlurper().parseText(restoreLinkResults.content)
					def restorePoint
					if(response.name() == "VmRestorePoints") {
						restorePoint = response.VmRestorePoint.find { it.HierarchyObjRef.text().toString().toLowerCase() == objectRef?.toLowerCase() || it.VmName.text().toString() == opts.vmName }
						if(!restorePoint && opts.vCenterVmId) {
							restorePoint = response.VmRestorePoint.find { it.HierarchyObjRef.text().toString().endsWith(opts.vCenterVmId) }
						}
					} else {
						restorePoint = response
					}

					if(restorePoint) {
						restorePoint.Links.Link.each { link ->
							if(link['@Rel']?.toString() == "Restore") {
								def restoreUrl = new URI(link['@Href']?.toString())
								restoreLink = restoreUrl.path
							}
						}
					} else {
						rtn.msg = "Veeam restore point not found for VM"
						rtn.success = false
						log.error(rtn.msg)
					}
				}
			}
		}

		//execute the restore
		def taskId
		if(restoreLink) {
			log.debug("Performing restore with endpoint: ${restoreLink}")
			def xml
			if(opts.cloudTypeCode == 'vcd' && opts.backupType != "veeamzip") {
				def restorePointUid = VeeamUtils.extractVeeamUuid(restoreLink)
				def hierarchyRootUid = VeeamUtils.extractVeeamUuid(opts.hierarchyRoot)
				xml = new StreamingMarkupBuilder().bind() {
					RestoreSpec("xmlns": "http://www.veeam.com/ent/v1.0", "xmlns:xsd": "http://www.w3.org/2001/XMLSchema", "xmlns:xsi": "http://www.w3.org/2001/XMLSchema-instance") {
						vCloudVmRestoreSpec() {
							"PowerOnAfterRestore"(true)
							"HierarchyRootUid"(hierarchyRootUid)
							vAppRef("urn:${VeeamUtils.CLOUD_TYPE_VCD}:Vapp:${hierarchyRootUid}.urn:vcloud:vapp:${opts.vAppId}")
							VmRestoreParameters() {
								VmRestorePointUid("urn:veeam:VmRestorePoint:${restorePointUid}")
							}
						}
					}
				}
			} else {
				xml = new StreamingMarkupBuilder().bind() {
					RestoreSpec("xmlns": "http://www.veeam.com/ent/v1.0", "xmlns:xsd": "http://www.w3.org/2001/XMLSchema", "xmlns:xsi": "http://www.w3.org/2001/XMLSchema-instance") {
						VmRestoreSpec() {
							"PowerOnAfterRestore"(true)
							"QuickRollback"(false)
						}
					}
				}
			}
			def body = xml.toString()
			log.debug "body: ${body}"
			def restoreQuery = query + [action: 'restore']
			HttpApiClient httpApiClient = new HttpApiClient()
			HttpApiClient.RequestOptions requestOpts = new HttpApiClient.RequestOptions(headers:headers, query:restoreQuery, body: body)
			def results = httpApiClient.callXmlApi(url, restoreLink, null, null, requestOpts, 'POST')
			rtn.success = results?.success
			if(rtn.success == true) {
				def response = new groovy.util.XmlSlurper().parseText(results.content)
				//get the restore session id
				taskId = response.TaskId
			}
		} else if(!rtn.msg) {
			log.debug("Unable to perform restore, no restore link found.")
			rtn.msg = "Veeam restore link not found"
			rtn.success = false
			log.error(rtn.msg)
		}
		if(taskId) {
			def restoreTaskResults = waitForTask([token: token, apiUrl: url, basePath:'/api'], taskId.toString())
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
	static lookupVm(url, token, cloudType, hierarchyRoot, vmRefId) {
		def rtn = [success:false]
		rtn = getVmId(url, token, cloudType, hierarchyRoot, vmRefId)
		if(!rtn.vmId){
			log.error("Failed to find VM object in Veeam: ${vmRefId}")
		}
		return rtn
	}

	//get the veeam VM ID given the veeam managed server ID (hiearchy root) and vmware VM ref ID
	static getVmId(url, token, cloudType, hierarchyRoot, vmRefId) {
		def rtn = [success:false]
		//construct the object ref
		def vmId = VeeamUtils.getVmHierarchyObjRef(vmRefId, hierarchyRoot, cloudType)
		def headers = buildHeaders([:], token)
		def query = [hierarchyRef: vmId]
		HttpApiClient httpApiClient = new HttpApiClient()
		HttpApiClient.RequestOptions requestOpts = new HttpApiClient.RequestOptions(headers:headers, queryParams: query)
		def results = httpApiClient.callXmlApi(url, "/api/lookup", null, null, requestOpts, 'GET')
		log.debug("got: ${results}")
		rtn.success = results?.success
		if(rtn.success == true) {
			def response = new groovy.util.XmlSlurper().parseText(results.content)
			rtn.vmId = results.data.HierarchyItem.ObjectRef.toString()
			rtn.vmName = results.data.HierarchyItem.ObjectName.toString()
		}
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
			} else if(results.errorCode == 500) {
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

	// XML Utils
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
