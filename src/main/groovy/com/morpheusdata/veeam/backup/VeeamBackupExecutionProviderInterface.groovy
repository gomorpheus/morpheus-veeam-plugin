package com.morpheusdata.veeam.backup

import com.morpheusdata.core.MorpheusContext
import com.morpheusdata.core.Plugin
import com.morpheusdata.core.backup.BackupExecutionProvider
import com.morpheusdata.core.backup.BackupJobProvider
import com.morpheusdata.core.backup.response.BackupExecutionResponse
import com.morpheusdata.core.data.DataFilter
import com.morpheusdata.core.data.DataQuery
import com.morpheusdata.core.util.ComputeUtility
import com.morpheusdata.core.util.DateUtility
import com.morpheusdata.model.Backup
import com.morpheusdata.model.BackupProvider
import com.morpheusdata.model.BackupRepository
import com.morpheusdata.model.BackupResult
import com.morpheusdata.model.Cloud
import com.morpheusdata.model.ComputeServer
import com.morpheusdata.model.ReferenceData
import com.morpheusdata.model.Workload
import com.morpheusdata.response.ServiceResponse
import com.morpheusdata.veeam.services.ApiService
import com.morpheusdata.veeam.utils.VeeamUtils
import groovy.util.logging.Slf4j

@Slf4j
interface VeeamBackupExecutionProviderInterface extends BackupExecutionProvider {

	Plugin getPlugin()

	MorpheusContext getMorpheus()

	ApiService getApiService()

	VeeamBackupTypeProvider getBackupTypeProvider()

	/**
	 * Add additional configurations to a backup. Morpheus will handle all basic configuration details, this is a
	 * convenient way to add additional configuration details specific to this backup provider.
	 * @param backupModel the current backup the configurations are applied to.
	 * @param config the configuration supplied by external inputs.
	 * @param opts optional parameters used for configuration.
	 * @return a {@link ServiceResponse} object. A ServiceResponse with a false success will indicate a failed
	 * configuration and will halt the backup creation process.
	 */
	default ServiceResponse configureBackup(Backup backup, Map config, Map opts) {
		log.debug("configureBackup: {}, {}, {}", backup, config, opts)
		if(config.veeamManagedServer) {
			def managedServerId = config.veeamManagedServer.split(":").getAt(0)
			def managedServerRef = morpheus.services.referenceData.find(new DataQuery().withFilters([
					new DataFilter("code", "veeam.backup.managedServer.${backup.backupProvider.id}.urn:veeam:ManagedServer:${managedServerId}")
			]))
			if(managedServerRef) {
				backup.setConfigProperty("veeamHierarchyRootUid", managedServerRef.getConfigProperty("hierarchyRootUid"))
				backup.setConfigProperty("veeamManagedServerId", config.veeamManagedServer)
			}
		}

		//storage provider
		if(backup.backupJob.backupRepository) {
			backup.backupRepository = backup.backupJob.backupRepository
		}

		return ServiceResponse.success(backup)
	}

	/**
	 * Validate the configuration of the backup. Morpheus will validate the backup based on the supplied option type
	 * configurations such as required fields. Use this to either override the validation results supplied by the
	 * default validation or to create additional validations beyond the capabilities of option type validation.
	 * @param backupModel the backup to validate
	 * @param config the original configuration supplied by external inputs.
	 * @param opts optional parameters used for
	 * @return a {@link ServiceResponse} object. The errors field of the ServiceResponse is used to send validation
	 * results back to the interface in the format of {@code errors['fieldName'] = 'validation message' }. The msg
	 * property can be used to send generic validation text that is not related to a specific field on the model.
	 * A ServiceResponse with any items in the errors list or a success value of 'false' will halt the backup creation
	 * process.
	 */
	default ServiceResponse validateBackup(Backup backup, Map config, Map opts) {
		return ServiceResponse.success(backup)
	}

	/**
	 * Create the backup resources on the external provider system.
	 * @param backupModel the fully configured and validated backup
	 * @param opts additional options used during backup creation
	 * @return a {@link ServiceResponse} object. A ServiceResponse with a success value of 'false' will indicate the
	 * creation on the external system failed and will halt any further backup creation processes in morpheus.
	 */
	default ServiceResponse createBackup(Backup backup, Map opts) {
		log.debug("createBackup {}:{} to job {} with opts: {}", backup.id, backup.name, backup.backupJob.id, opts)
		ServiceResponse rtn = ServiceResponse.prepare()
		try {
			def backupProvider = backup.backupProvider
			def apiVersion = VeeamUtils.getApiVersion(backupProvider)
			def backupJob = backup.backupJob
			def authConfig = apiService.getAuthConfig(backupProvider)
			//config
			Workload workload = morpheus.services.workload.get(backup.containerId)
			ComputeServer server = morpheus.services.computeServer.get(workload.server.id)
			Cloud cloud = morpheus.services.cloud.get(server.cloud.id)
			//names
			def vmName = server.name
			def backupName = backup.name
			//external id
			def externalId = server?.externalId
			//build api opts
			def apiOpts = [
					jobId : VeeamUtils.extractVeeamUuid(backupJob.externalId),
					jobName: backupJob.name,
					repositoryId: backupJob.backupRepository?.internalId,
					vmName: vmName,
					externalId: externalId,
					backupName: backupName,
					cloudType: backupTypeProvider.getCloudType()
			]
			if(opts.newBackup && backupJob.sourceJobId) {
				apiOpts.removeJobs = true
			}
			//get managed servers
			def managedServers = []
			def hierarchyRoot = VeeamUtils.getHierarchyRoot(backup)
			log.debug("createBackup hierarchyRoot: ${hierarchyRoot}")

			if(hierarchyRoot) {
				managedServers << hierarchyRoot
			} else {
				def objCategory = "veeam.backup.managedServer.${backupProvider.id}"
				log.debug("createBackup objCategory: ${objCategory} typeFilter: ${getManagedServerType()}")
				def managedServerResults = morpheus.services.referenceData.list(new DataQuery().withFilters(
				        new DataFilter("account.id", backupProvider.account.id),
				        new DataFilter("category", objCategory),
				        new DataFilter("typeValue", getManagedServerType())
				))
				managedServerResults.each {
					if(apiVersion > 1.3) {
						managedServers << it.getConfigProperty('hierarchyRootUid')
					} else {
						managedServers << VeeamUtils.extractVeeamUuid(it.keyValue)
					}
				}
			}

			List possibleVmObjectRefs = []
			managedServers.each { managedServerId ->
				possibleVmObjectRefs << backupTypeProvider.getVmHierarchyObjRef(externalId, managedServerId)
			}
			def findResults = apiService.waitForVm(authConfig, possibleVmObjectRefs, 600, apiOpts)
			log.info("wait results: ${findResults}")
			if(findResults.success == true) {
				//call the api
				def createResults = apiService.createBackupJobBackup(authConfig, apiOpts.jobId, findResults.vmId, findResults.vmName, apiOpts)
				log.info("Backup job backup create results: ${createResults}")
				if(createResults.success == true) {
					//return some stuff?
					backup.internalId = createResults.backupId
					backup.externalId = createResults.backupId
					backup.setConfigProperty('hierarchyObjRef',findResults.vmId)
					backup.setConfigProperty('hierarchyRoot', findResults.hierarchyRoot)
					backup.statusMessage = "Ready"
					rtn.success = true
				} else {
					backup.statusMessage = "Failed"
					backup.errorMessage = "Unable to create backup: " + (createResults.msg)
				}
			} else {
				backup.statusMessage = "Failed"
				backup.errorMessage = "Unable to create backup: " + (createResults.msg)
				//count not find the vm
				rtn.msg = "Failed to find VM in Veeam. Ensure the VM is accessible to the Veeam backup server."
				log.error("Unable to locate VM in Veeam: ${findResults}")
			}

			rtn.success = true
		} catch(e) {
			log.error("createBackup error: ${e}", e)
		}
		return rtn
	}

	/**
	 * Delete the backup resources on the external provider system.
	 * @param backupModel the backup details
	 * @param opts additional options used during the backup deletion process
	 * @return a {@link ServiceResponse} indicating the results of the deletion on the external provider system.
	 * A ServiceResponse object with a success value of 'false' will halt the deletion process and the local reference
	 * will be retained.
	 */
	default ServiceResponse deleteBackup(Backup backup, Map opts) {
		log.debug("deleteBackup: {}", backup)

		ServiceResponse rtn = ServiceResponse.prepare()
		try {
			def backupJob = backup.backupJob
			if(!backupJob.backupProvider) {
				// the backup provider was not included in the backup job object, so we need to fetch the job to include the job provider
				backupJob = morpheus.services.backupJob.get(backup.backupJob.id)
			}
			if(!backupJob) {
				rtn.success = true
				return rtn
			}
			def backupJobId = VeeamUtils.extractVeeamUuid(backupJob?.externalId ?: backup.getConfigProperty("backupJobId"))
			try{
				def backupProvider = backup.backupProvider
				def authConfig = apiService.getAuthConfig(backupProvider)
				def session = apiService.loginSession(backupProvider)
				def token = session.token
				def sessionId = session.sessionId
				String backupId = backup.externalId
				if(!backupId) {
					def backupJobBackups = apiService.getBackupJobBackups(authConfig.apiUrl, session.token, backupJobId)
					// find by MOR first
					Workload workload = morpheus.services.workload.get(backup.containerId)
					if(workload?.server?.id) {
						ComputeServer server = morpheus.services.computeServer.get(workload.server.id)
						backupId = backupJobBackups.data?.find { VeeamUtils.extractVmIdFromObjectRef(it.objectRef.toString()) == server.externalId }?.objectId
						log.info("deleteBackup, found backup by objectRef: ${backupId}")
					}
					// otherwise find by backup name
					if(!backupId) {
						backupId = backupJobBackups.data?.find { it.name?.toString() == backup.name }?.objectId?.toString()
						log.info("deleteBackup, found backup by name: ${backupId}")
					}
				}
				log.debug("deleteBackup backupId: ${backupId}")
				if(backupId) {
					def removeTask = apiService.removeVmFromBackupJob(authConfig.apiUrl, session.token, backupJobId, backupId)
					if(removeTask.success && removeTask.taskId) {
						def removeTaskResults = apiService.waitForTask(authConfig + [token: session.token], removeTask.taskId.toString())
						log.debug("deleteBackup removeTaskResults: ${removeTaskResults}")
						if(removeTaskResults.success || removeTaskResults.msg?.toLowerCase()?.contains("only one object")) {
							def backupJobBackups = apiService.getBackupJobBackups(authConfig.apiUrl, session.token, backupJobId)
							// veeam backup jobs must contain at least one vm, so its safe to disable (9.5u4-) or delete (v10+ only)
							// when the only vm remaining is the one we're removing
							if(backupJobBackups.success && backupJobBackups.data.size() <= 1 && backupJobBackups.data?.getAt(0)?.objectId?.toString() == backupId) {
								log.debug("The current backup is the last object in the job, removing the backup job")
								BackupJobProvider jobProvider = ((VeeamBackupProvider)this.plugin.getProviderByCode('veeam')).backupJobProvider
								ServiceResponse deleteResults = jobProvider.deleteBackupJob(backupJob, [force:true])
								log.debug("Delete job deleteResults: ${deleteResults}")
								if(deleteResults.success && deleteResults.data?.taskId) {
									// todo: wait for backup job not found???
									log.debug("delete job, waiting for task: ${deleteResults.data.taskId}")
									def taskResults = apiService.waitForTask(authConfig + [token: session.token], deleteResults.data.taskId.toString())
									log.debug("task results: ${taskResults}")
									if(taskResults.success) {
										if(taskResults.error) {
											def waitDeleteResults = waitForJobDeleted(authConfig, session.token, backupJobId)
											if(waitDeleteResults.success) {
												rtn.success = true
											} else {
												rtn.success = false
												rtn.msg = waitDeleteResults.msg ?: "Unable to delete backup job"
											}
										} else {
											rtn.success = true
										}
									}
								} else {
									rtn.success = false
									rtn.msg = deleteResults.msg ?: "Unable to delete backup job"
									log.debug("deleteBackup: {}", rtn.msg)
								}
							} else {
								log.debug("Backup Job ${backupJobId} contains one or more objects and can't be deleted.")
								rtn.success = true
							}
						} else {
							rtn.success = false
							rtn.msg = removeTaskResults.msg
						}
					} else {
						// older veeam didn't allow backup removal, so we just disable the job. Remove this when the minimum version
						// of veeam support job delete via the API
						def backupJobBackups = apiService.getBackupJobBackups(authConfig.apiUrl, session.token, backupJobId)
						if(backupJobBackups.success && !backupJobBackups.data?.find { it.objectId?.toString() == backupId}) {
							// the vm wasn't in the job so consider the delete a success
							rtn.success = true
							if(backupJobBackups.data.size() <= 1 && backupJobBackups.data?.getAt(0)?.objectId?.toString() == backupId) {
								apiService.disableBackupJobSchedule(authConfig.apiUrl, token, backupJobId)
							}
						}
					}
				} else {
					// Backup was already removed from job, continue on
					rtn.success = true
				}
				apiService.logoutSession(authConfig.apiUrl, token, sessionId)
			} catch(Exception ex) {
				log.error("Failed to disable Veeam backup job: ${backupJobId}", ex)
			}
		} catch(e) {
			log.error("error deleting veeam backup: ${e.message}", e)
			throw new RuntimeException("Unable to remove backup: ${e.message}", e)
		}
		return rtn
	}

	/**
	 * Delete the results of a backup execution on the external provider system.
	 * @param backupResultModel the backup results details
	 * @param opts additional options used during the backup result deletion process
	 * @return a {@link ServiceResponse} indicating the results of the deletion on the external provider system.
	 * A ServiceResponse object with a success value of 'false' will halt the deletion process and the local reference
	 * will be retained.
	 */
	default ServiceResponse deleteBackupResult(BackupResult backupResult, Map opts) {
		return ServiceResponse.success()
	}

	/**
	 * A hook into the execution process. This method is called before the backup execution occurs.
	 * @param backupModel the backup details associated with the backup execution
	 * @param opts additional options used during the backup execution process
	 * @return a {@link ServiceResponse} indicating the success or failure of the execution preparation. A success value
	 * of 'false' will halt the execution process.
	 */
	default ServiceResponse prepareExecuteBackup(Backup backup, Map opts) {
		return ServiceResponse.success()
	}

	/**
	 * Provide additional configuration on the backup result. The backup result is a representation of the output of
	 * the backup execution including the status and a reference to the output that can be used in any future operations.
	 * @param backupResultModel
	 * @param opts
	 * @return a {@link ServiceResponse} indicating the success or failure of the method. A success value
	 * of 'false' will halt the further execution process.
	 */
	default ServiceResponse prepareBackupResult(BackupResult backupResultModel, Map opts) {
		return ServiceResponse.success()
//		def rtn = [success: false]
//		try {
//			def backup = result.backup
//			def container = Container.read(backup.containerId)
//			def infrastructureConfig = backupService.serializeContainerConfig(container)
//			result.setConfigProperty("infrastructureConfig", infrastructureConfig)
//			rtn.success = true
//		} catch (Exception e) {
//			log.error("prepareBackupResult error: {}", e, e)
//		}
//		return rtn
	}

	/**
	 * Initiate the backup process on the external provider system.
	 * @param backup the backup details associated with the backup execution.
	 * @param backupResult the details associated with the results of the backup execution.
	 * @param executionConfig original configuration supplied for the backup execution.
	 * @param cloud cloud context of the target of the backup execution
	 * @param computeServer the target of the backup execution
	 * @param opts additional options used during the backup execution process
	 * @return a {@link ServiceResponse} indicating the success or failure of the backup execution. A success value
	 * of 'false' will halt the execution process.
	 */
	default ServiceResponse<BackupExecutionResponse> executeBackup(Backup backup, BackupResult backupResult, Map executionConfig, Cloud cloud, ComputeServer computeServer, Map opts) {
		log.debug("executeBackup: {}", backup)
		ServiceResponse<BackupExecutionResponse> rtn = ServiceResponse.prepare(new BackupExecutionResponse(backupResult))
		if(!backup.backupProvider.enabled) {
			rtn.error = "Veeam backup provider is disabled"
			return rtn
		}
		try {
			def backupProvider = backup.backupProvider
			def authConfig = apiService.getAuthConfig(backupProvider)
			def session = apiService.loginSession(backupProvider)
			String token = session.token
			def sessionId = session.sessionId
			log.debug("token: ${token}")
			if(token) {
				log.debug("getting last result")
				def lastResult = morpheus.services.backup.backupResult.find(new DataQuery().withFilters(
					new DataFilter("backup.id", backup.id),
					new DataFilter("status", "ne", BackupResult.Status.START_REQUESTED)
				).withSort("dateCreated", DataQuery.SortOrder.desc))

				log.debug("last result: ${lastResult}")
				def backupServerId = VeeamUtils.getBackupServerId(backup)
				if(backupServerId) {
					// get hierarchy ref and object ref, this should probably be moved up to creatBackup
					String veeamObjectRef = backupTypeProvider.getVeeamObjectRef(authConfig, token, backup, backupProvider, computeServer)
					String veeamHierarchyRef = backupTypeProvider.getVmHierarchyObjRef(backup, computeServer, veeamObjectRef)
					backupTypeProvider.updateObjectAndHierarchyRefs(backup, veeamObjectRef, veeamHierarchyRef)

					log.debug("executeBackup vmId: ${veeamObjectRef}")
					if(veeamObjectRef || veeamHierarchyRef) {
						def resultCount = morpheus.services.backup.backupResult.count(new DataQuery().withFilters(
							new DataFilter("backup.id", backup.id),
							new DataFilter("status", BackupResult.Status.SUCCEEDED.toString()),
							new DataFilter("backupType", "in", ["default", "quickbackup"])
						))
						def hasFullBackup = (resultCount > 0)

						if(hasFullBackup && veeamObjectRef) {
							def startResponse = startQuickBackup(authConfig, backupServerId, veeamObjectRef, lastResult)
							updateBackupExecutionResponse((BackupExecutionResponse) rtn.data, startResponse, 'quickbackup')
							rtn.success = startResponse.success
							if(startResponse.success == false) {
								rtn.data.backupResult.status = BackupResult.Status.FAILED.toString()
								rtn.data.backupResult.statusMessage = startResponse.msg
								rtn.msg = startResponse.msg
								rtn.success = false
							}
						} else if(veeamHierarchyRef) {
							def startResponse = startVeeamZip(backup, backupProvider, authConfig, backupServerId, veeamHierarchyRef, lastResult, computeServer)
							updateBackupExecutionResponse((BackupExecutionResponse)rtn.data, startResponse, 'veeamzip')
							rtn.success = startResponse.success
							if(startResponse.success == false) {
								rtn.data.backupResult.status = BackupResult.Status.FAILED.toString()
								rtn.data.backupResult.statusMessage = startResponse.msg
								rtn.msg = startResponse.msg
								rtn.success = false
							}
						} else {
							rtn.success = false
							rtn.data.backupResult.status = BackupResult.Status.FAILED.toString()
							rtn.data.backupResult.statusMessage = "No hierarchy object ref found."
							rtn.data.updates = true
							rtn.msg = rtn.data.backupResult.statusMessage
							rtn.success = false
						}

						log.debug("executeBackup result: " + rtn)
					} else {
						rtn.success = false
						rtn.error = "Could not find a VM with the VMWare ID ${computeServer.externalId} on the root ${hierarchyRoot}"
					}
				} else {
					rtn.success = false
					rtn.error = "Managed server id required to start a quick backup"
				}
			}
			apiService.logoutSession(authConfig.apiUrl, token, sessionId)
		}


	catch (e) {
			log.error("executeBackup error: ${e}", e)
		}
		return rtn
	}

	default ServiceResponse startVeeamZip(Backup backup, BackupProvider backupProvider, LinkedHashMap<String, Object> authConfig, backupServerId, String veeamHierarchyRef, BackupResult lastResult, ComputeServer computeServer) {
		ServiceResponse rtn = ServiceResponse.prepare()
		BackupRepository repository = backup.backupRepository
		if(!repository) {
			repository = morpheus.services.backupRepository.find(new DataQuery().withFilter("category", "veeam.repository.${backupProvider.id}"))
		}
		if(repository) {
			def startResponse = apiService.startVeeamZip(authConfig, backupServerId, repository.externalId, veeamHierarchyRef, [lastBackupSessionId: lastResult?.getConfigProperty("backupSessionId"), vmwToolsInstalled: computeServer.toolsInstalled])
			log.debug("startVeeamZip response: ${startResponse}")
			rtn.success = startResponse.success
			if(startResponse.success) {
				rtn.success = true
				rtn.data = startResponse.data
			} else {
				rtn.msg = startResponse.errorMessage ?: startResponse.msg
				rtn.success = false
			}
		} else {
			rtn.msg = "Backup repository not found."
		}

		return rtn
	}

	default ServiceResponse startQuickBackup(LinkedHashMap<String, Object> authConfig, backupServerId, String veeamObjectRef, BackupResult lastResult) {
		ServiceResponse rtn = ServiceResponse.prepare()

		def startResponse = apiService.startQuickBackup(authConfig, backupServerId, veeamObjectRef, [lastBackupSessionId: lastResult?.getConfigProperty("backupSessionId")])
		if(startResponse.success) {
			rtn.success = true
			rtn.data = startResponse.data
		} else {
			rtn.msg = startResponse.errorMessage ?: startResponse.msg
			rtn.success = false
		}

		return rtn
	}

	default updateBackupExecutionResponse(BackupExecutionResponse executionResponse, ServiceResponse backupStartResponse, String backupType) {
		log.debug("updateBackupExecutionResponse: ${backupStartResponse}, ${backupType}")
		if(backupStartResponse.success) {
			executionResponse.backupResult.externalId = backupStartResponse.data.backupSessionId
			executionResponse.backupResult.startDate = DateUtility.parseDate(backupStartResponse.data.startDate as CharSequence)
			executionResponse.backupResult.backupType = backupType
		} else {
			executionResponse.backupResult.status = BackupResult.Status.FAILED.toString()
			executionResponse.backupResult.statusMessage = backupStartResponse.msg
		}
		executionResponse.updates = true
	}

	default waitForJobDeleted(Map authConfig, String token, String backupJobId) {
		def rtn = [success:false, error:false, data:null, vmId:null]
		def attempt = 0
		def keepGoing = true
		def sleepInterval = 5l * 1000l //5 seconds
		def maxAttempts = 300000 / sleepInterval
		while(keepGoing == true && attempt < maxAttempts) {
			//load the vm
			def results = apiService.getBackupJob(authConfig.apiUrl, token, backupJobId)
			if(results.success) {
				attempt++
				sleep(sleepInterval)
			} else {
				if(results.errorCode.toString() == "404") {
					rtn.success = true
				} else {
					rtn = results
				}
				keepGoing = false

			}


		}
		return rtn
	}

	/**
	 * Periodically call until the backup execution has successfully completed. The default refresh interval is 60 seconds.
	 * @param backupResult the reference to the results of the backup execution including the last known status. Set the
	 *                     status to a canceled/succeeded/failed value from one of the {@link BackupResult.Status} values
	 *                     to end the execution process.
	 * @return a {@link ServiceResponse} indicating the success or failure of the method. A success value
	 * of 'false' will halt the further execution process.n
	 */
	default ServiceResponse<BackupExecutionResponse> refreshBackupResult(BackupResult backupResult) {
		ServiceResponse<BackupExecutionResponse> rtn = ServiceResponse.prepare(new BackupExecutionResponse(backupResult))
		if(backupResult.backup.backupProvider.enabled) {
			try {
				def backup = backupResult.backup
				def backupProvider = backup.backupProvider
				def backupResultConfig = backupResult.configMap
				def authConfig = apiService.getAuthConfig(backupProvider)
				def session = apiService.loginSession(backupProvider)
				def token = session.token
				def sessionId = session.sessionId
				def apiUrl = authConfig.apiUrl
				def backupSessionId = backupResult.externalId ?: backupResultConfig.backupSessionId
				Workload workload = morpheus.services.workload.find(new DataQuery().withFilter("id", backup.containerId).withJoins("server", "server.zone", "server.zone.zoneType"))
				def server = workload?.server
				def cloud = server.cloud

				log.debug("refreshBackupResult backupSessionId: ${backupSessionId}")
				if(backupSessionId) {
					log.info("Fetching veeam info for backup ${backupResult.id} session ${backupSessionId ?: "<No session ID found>"}")
					def getBackupResult = apiService.getBackupResult(apiUrl, token, backupSessionId)
					def backupSession = getBackupResult.result

					if(backupSession) {
						boolean doUpdate = false

						def updatedStatus = VeeamUtils.getBackupStatus(backupSession.result)
						if(rtn.data.backupResult.status != updatedStatus) {
							rtn.data.backupResult.status = updatedStatus
							doUpdate = true
						}

						long sizeInMb = (backupSession.totalSize ?: 0 ).div(ComputeUtility.ONE_MEGABYTE)
						if(rtn.data.backupResult.sizeInMb != sizeInMb) {
							rtn.data.backupResult.sizeInMb = sizeInMb
							doUpdate = true
						}

						if(backupSession.startTime && backupSession.endTime) {
							def startDate = DateUtility.parseDate(backupSession.startTime)
							def endDate = DateUtility.parseDate(backupSession.endTime)
							if(startDate && rtn.data.backupResult.startDate != startDate) {
								rtn.data.backupResult.startDate = startDate
								doUpdate = true
							}
							if(endDate && rtn.data.backupResult.endDate != endDate) {
								rtn.data.backupResult.endDate = endDate
								doUpdate = true
							}
							def durationMillis = (endDate && startDate) ? (endDate.time - startDate.time) : 0
							if(rtn.data.backupResult.durationMillis != durationMillis) {
								rtn.data.backupResult.durationMillis = durationMillis
								doUpdate = true
							}

							if(rtn.data.backupResult.status == BackupResult.Status.SUCCEEDED.toString()) {
								def restoreLink = backupSession.links.find { ["RestorePointReference", "VmRestorePoint", "vAppRestorePoint"].contains(it.type)  }
								log.debug("restoreLink: ${restoreLink}")
								log.debug("backup result session links: ${backupSession.links}")
								def config = rtn.data.backupResult.getConfigMap()
								if(restoreLink) {
									config.restoreLink = restoreLink
									config.restoreType = restoreLink.type.toString()
									config.restoreHref = restoreLink.href.toString()
									config.restoreRef = VeeamUtils.extractVeeamUuid(config.restoreHref.toString())
								} else {
									def doSaveBackup = false
									def veeamHierarchyRef = backupResult.backup.getConfigProperty("veeamHierarchyRef")
									def veeamObjectRef = backupResult.backup.getConfigProperty("veeamObjectRef")

									if(!veeamHierarchyRef) {
										veeamHierarchyRef = backupTypeProvider.getVmHierarchyObjRef(backupResult.backup, server)
										backup.setConfigProperty("veeamHierarchyRef", veeamHierarchyRef)
										doSaveBackup
									}

									if(!veeamObjectRef) {
										def hierarchyRoot = VeeamUtils.getHierarchyRoot(backup)
										if(veeamHierarchyRef) {
											def vmRefId = backupTypeProvider.getVmRefId(server)
											def vmIdResults = apiService.lookupVm(authConfig.apiUrl, token, backupTypeProvider.getVmHierarchyObjRef(vmRefId, hierarchyRoot))
											veeamObjectRef = vmIdResults.vmId
										} else { //no veeamHierarchyRef, lookup by name
											def vmName = server.name
											def vmIdResults = apiService.lookupVmByName(authConfig.apiUrl, token, hierarchyRoot, vmName)
											veeamObjectRef = vmIdResults.vmId
										}

										if(veeamObjectRef) {
											backup.setConfigProperty("veeamObjectRef", veeamObjectRef)
											doSaveBackup = true
										}
									}

									if(doSaveBackup) {
										morpheus.async.backup.save(backup).subscribe().dispose()
									}

									def restorePoint = apiService.getRestorePoint(authConfig, veeamObjectRef, [startRefDateStr: startDate])
									if(!restorePoint.data?.externalId) {
										log.debug("No restore point found by veeamObjectRef, trying by veeamHierarchyRef")
										//try by veeamHierarchyRef
										restorePoint = apiService.getRestorePoint(authConfig, veeamHierarchyRef, [startRefDateStr: startDate])
									}
									if(restorePoint?.data?.externalId) {
										config.restorePointRef = restorePoint.data.externalId
									}
								}
								rtn.data.backupResult.setConfigMap(config)
								doUpdate = true
							} else if(rtn.data.backupResult.status == BackupResult.Status.FAILED.toString()) {
								def vmObjRef = backupTypeProvider.getVmHierarchyObjRef(backupResult.backup, server)
								ServiceResponse taskSessionsResponse = apiService.getBackupSessionTaskSessions(apiUrl, token, backupSessionId)
								Map vmBackupTaskSession = taskSessionsResponse.data.getAt("BackupTaskSessions").find { it.getAt("VmUid").toString() == vmObjRef.toString() }
								if(vmBackupTaskSession && vmBackupTaskSession.getAt("Reason")) {
										rtn.data.backupResult.errorOutput = vmBackupTaskSession.getAt("Reason")
								}
								doUpdate = true
							}
						}
						rtn.data.updates = doUpdate
						rtn.success = true
						if(sessionId) {
							log.debug("Logout session ${sessionId}")
							apiService.logoutSession(apiUrl, token, sessionId)
						}
					}
				} else {
					rtn.success = true
				}
			} catch(Exception e) {
				rtn.msg = e.getMessage()
				log.error("syncBackupResult error: ${e}", e)
			}
		}
		return rtn
	}
	
	/**
	 * Cancel the backup execution process without waiting for a result.
	 * @param backupResultModel the details associated with the results of the backup execution.
	 * @param opts additional options.
	 * @return a {@link ServiceResponse} indicating the success or failure of the backup execution cancellation.
	 */
	default ServiceResponse cancelBackup(BackupResult backupResult, Map opts) {
		log.debug("canceling backup, result ID: {} opts {}", backupResult.id, opts)
		ServiceResponse rtn = ServiceResponse.prepare()
		if(backupResult == null) {
			return rtn
		}
		try {
			def backup = backupResult.backup
			def backupProvider = backup.backupProvider
			def authConfig = apiService.getAuthConfig(backupProvider)
			def apiUrl = authConfig.apiUrl
			def session = apiService.loginSession(backupProvider)
			def token = session.token
			def sessionId = session.sessionId
			if(token) {
				//def backup = Backup.get(backupResult.backupId)
				def backupJobId = backup.externalId
				if(backupJobId) {
					rtn = apiService.stopBackupJob(apiUrl, token, backupJobId)
				} else {
					rtn.success = true
				}
			}
			apiService.logoutSession(apiUrl, token, sessionId)
		} catch (e) {
			log.error("cancelBackup error: ${e}", e)
		}
		return rtn
	}

	/**
	 * Extract the results of a backup. This is generally used for packaging up a full backup for the purposes of
	 * a download or full archive of the backup.
	 * @param backupResultModel the details associated with the results of the backup execution.
	 * @param opts additional options.
	 * @return a {@link ServiceResponse} indicating the success or failure of the backup extraction.
	 */
	default ServiceResponse extractBackup(BackupResult backupResultModel, Map opts) {
		return ServiceResponse.success()
	}

	default findManagedServerVmId(Map authConfig, token, cloudType, BackupProvider backupProvider, vmRefId) {
		def rtn = [success:true, data:[:]]
		try {
			morpheus.services.referenceData.list(new DataQuery().withFilters(
			        new DataFilter("category", "veeam.backup.managedServer.${backupProvider.id}"),
			        new DataFilter("typeValue", getManagedServerType())
			)).each {ReferenceData managedServer ->
				if(!rtn.data.size()) {
					def rootRef = managedServer.getConfigProperty("hierarchyRootUid")
					if (rootRef) {
						def vmHierarchyObjRef = backupTypeProvider.getVmHierarchyObjRef(vmRefId, rootRef)
						def result = apiService.getVmId(authConfig.apiUrl, token, vmHierarchyObjRef)
						if (result.vmId) {
							rtn.data = [vmId: result.vmId, managedServer: managedServer]
						}
					}
				}
			}
		} catch(Exception e) {
			rtn.success = false
			log.error("findManagedServerForVm error: {}", e, e)
		}

		return rtn
	}
}		
