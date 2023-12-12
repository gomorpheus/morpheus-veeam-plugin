package com.morpheusdata.veeam

import com.morpheusdata.core.MorpheusContext
import com.morpheusdata.core.Plugin
import com.morpheusdata.core.backup.BackupExecutionProvider
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
class VeeamBackupExecutionProvider implements BackupExecutionProvider {

	Plugin plugin
	MorpheusContext morpheus
	ApiService apiService

	VeeamBackupExecutionProvider(Plugin plugin, MorpheusContext morpheus) {
		this.plugin = plugin
		this.morpheus = morpheus
		this.apiService = new ApiService()
	}

	/**
	 * Add additional configurations to a backup. Morpheus will handle all basic configuration details, this is a
	 * convenient way to add additional configuration details specific to this backup provider.
	 * @param backupModel the current backup the configurations are applied to.
	 * @param config the configuration supplied by external inputs.
	 * @param opts optional parameters used for configuration.
	 * @return a {@link ServiceResponse} object. A ServiceResponse with a false success will indicate a failed
	 * configuration and will halt the backup creation process.
	 */
	@Override
	ServiceResponse configureBackup(Backup backup, Map config, Map opts) {
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
	@Override
	ServiceResponse validateBackup(Backup backup, Map config, Map opts) {
		return ServiceResponse.success(backup)
	}

	/**
	 * Create the backup resources on the external provider system.
	 * @param backupModel the fully configured and validated backup
	 * @param opts additional options used during backup creation
	 * @return a {@link ServiceResponse} object. A ServiceResponse with a success value of 'false' will indicate the
	 * creation on the external system failed and will halt any further backup creation processes in morpheus.
	 */
	@Override
	ServiceResponse createBackup(Backup backup, Map opts) {
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
					jobId : apiService.extractVeeamUuid(backupJob.externalId),
					jobName: backupJob.name,
					repositoryId: backupJob.backupRepository?.internalId,
					vmName: vmName,
					externalId: externalId,
					backupName: backupName,
					cloudType: apiService.getCloudTypeFromZoneType(cloud.cloudType.code)
			]
			if(opts.newBackup && backupJob.sourceJobId) {
				apiOpts.removeJobs = true
			}
			//get managed servers
			def managedServers = []
			def hierarchyRoot = VeeamUtils.getHierarchyRoot(backup)

			if(hierarchyRoot) {
				managedServers << hierarchyRoot
			} else {
				def objCategory = "veeam.backup.managedServer.${backupProvider.id}"
				def typeFilter = apiService.getManagedServerTypeFromZoneType(cloud.cloudType.code)
				def managedServerResults = morpheus.services.referenceData.list(new DataQuery().withFilters(
				        new DataFilter("account.id", backupProvider.account.id),
				        new DataFilter("category", objCategory),
				        new DataFilter("typeValue", typeFilter)
				))
				managedServerResults.each {
					if(apiVersion > 1.3) {
						managedServers << it.getConfigProperty('hierarchyRootUid')
					} else {
						managedServers << apiService.extractVeeamUuid(it.keyValue)
					}
				}
			}
			//call the api
			def createResults = apiService.createBackupJobBackup(authConfig, apiOpts.jobId, managedServers, apiOpts)
			log.info("Backup job backup create results: ${createResults}")
			if(createResults.success == true) {
				//return some stuff?
				backup.internalId = createResults.backupId
				backup.externalId = createResults.backupId
				backup.setConfigProperty('hierarchyObjRef', createResults.hierarchyObjRef)
				backup.setConfigProperty('hierarchyRoot', createResults.hierarchyRoot)
				backup.statusMessage = "Ready"
				rtn.success = true
			} else {
				backup.statusMessage = "Failed"
				backup.errorMessage = "Unable to create backup: " + (createResults.msg)
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
	 * A ServiceResponse object with a success value of 'false' will halt the deletion process and the local refernce
	 * will be retained.
	 */
	@Override
	ServiceResponse deleteBackup(Backup backup, Map opts) {
		log.debug("deleteBackup: {}", backup)

		ServiceResponse rtn = ServiceResponse.prepare()
		//Note: Veeam API does not allow delete backup job, best we can do is turn off schedule
		try {
			def backupJob = backup.backupJob
			if(!backupJob) {
				rtn.success = true
				return rtn
			}
			def backupJobId = apiService.extractVeeamUuid(backupJob?.externalId ?: backup.getConfigProperty("backupJobId"))
			try{
				def backupProvider = backup.backupProvider
				def authConfig = apiService.getAuthConfig(backupProvider)
				def session = apiService.loginSession(backupProvider)
				def token = session.token
				def sessionId = session.sessionId
				def backupId = backup.externalId
				if(!backupId) {
					def backupJobBackups = apiService.getBackupJobBackups(authConfig.apiUrl, session.token, backupJobId)
					// find by MOR first
					Workload workload = morpheus.services.workload.get(backup.containerId)
					if(workload.server?.id) {
						ComputeServer server = morpheus.services.computeServer.get(workload.server.id)
						backupId = backupJobBackups.data?.find { apiService.extractMOR(it.objectRef.toString()) == server.externalId }?.objectId
						log.info("deleteBackup, found backup by objectRef: ${backupId}")
					}
					// otherwise find by backup name
					if(!backupId) {
						backupId = backupJobBackups.data?.find { it.name?.toString() == backup.name }?.objectId
						log.info("deleteBackup, found backup by name: ${backupId}")
					}
				}
				log.debug("deleteBackup backupId: ${backupId}")
				if(backupId) {
					def removeTask = apiService.removeVmFromBackupJob(authConfig.apiUrl, session.token, backupJobId, backupId)
					if(removeTask.success && removeTask.taskId) {
						def removeTaskResults = apiService.waitForTask(authConfig + [token: session.token], removeTask.taskId.toString())
						if(removeTaskResults.success || removeTaskResults.msg?.toLowerCase()?.contains("only one object")) {
							def backupJobBackups = apiService.getBackupJobBackups(authConfig.apiUrl, session.token, backupJobId)
							if(backupJobBackups.success && backupJobBackups.data.size() <= 1 && backupJobBackups.data?.getAt(0)?.objectId?.toString() == backupId) {
								// veeam backup jobs must contain at least one vm, so its safe to disable (9.5u4-) or delete (v10+ only)
								// when the only vm remaining is the one we're removing
								ServiceResponse deleteResults = ((VeeamBackupProvider)this.plugin.getProviderByCode('veeam')).backupJobProvider.deleteBackupJob(backupJob)
								if(deleteResults.success && deleteResults.data?.taskId) {
									def taskResults = apiService.waitForTask(authConfig + [token: session.token], deleteResults.data.taskId.toString())
									if(taskResults.success) {
										rtn.success = true
									}
								} else {
									rtn.success = false
									rtn.msg = deleteResults.msg ?: "Unable to delete backup job"
									log.debug(rtn.msg)
								}
							} else {
								log.debug("Job contains only one object and can't be deleted.")
								rtn.success = true
							}
						} else {
							rtn.success = false
							rtn.msg = removeTaskResults.msg
						}
					} else {
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
	 * A ServiceResponse object with a success value of 'false' will halt the deletion process and the local refernce
	 * will be retained.
	 */
	@Override
	ServiceResponse deleteBackupResult(BackupResult backupResult, Map opts) {
		return ServiceResponse.success()
	}

	/**
	 * A hook into the execution process. This method is called before the backup execution occurs.
	 * @param backupModel the backup details associated with the backup execution
	 * @param opts additional options used during the backup execution process
	 * @return a {@link ServiceResponse} indicating the success or failure of the execution preperation. A success value
	 * of 'false' will halt the execution process.
	 */
	@Override
	ServiceResponse prepareExecuteBackup(Backup backup, Map opts) {
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
	@Override
	ServiceResponse prepareBackupResult(BackupResult backupResultModel, Map opts) {
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
	@Override
	ServiceResponse<BackupExecutionResponse> executeBackup(Backup backup, BackupResult backupResult, Map executionConfig, Cloud cloud, ComputeServer computeServer, Map opts) {
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
			def token = session.token
			def sessionId = session.sessionId
			if(token) {
				def lastResult = morpheus.services.backup.backupResult.find(new DataQuery().withFilters(
					new DataFilter("backup.id", backup.id),
					new DataFilter("status", "ne", BackupResult.Status.START_REQUESTED)
				).withSort("dateCreated", "desc"))

				def hierarchyRoot = VeeamUtils.getHierarchyRoot(backup)
				def backupServerId = VeeamUtils.getBackupServerId(backup)
				if(hierarchyRoot && backupServerId) {
					def vmRefId
					def vmName = computeServer.name
					def vmId
					def hasFullBackup = false

					def resultCountQuery = new DataQuery().withFilters(
						new DataFilter("backup.id", backup.id),
						new DataFilter("status", BackupResult.Status.START_REQUESTED)
					)
					if(cloud.cloudType.code == "hyperv" || cloud.cloudType.code == "scvmm") {
						opts.cloudType = apiService.CLOUD_TYPE_HYPERV
						//we don't store the GUID for hyper-v servers, just the name - so use the name lookup
						vmName = vmRefId ?: vmName
						vmRefId = null
					} else if(cloud.cloudType.code == 'vcd') {
						opts.cloudType = apiService.CLOUD_TYPE_VCD
						def managedServerVmIdResults = findManagedServerVmId(authConfig, token, apiService.CLOUD_TYPE_VMWARE, backupProvider, computeServer.uniqueId)
						if(managedServerVmIdResults.success && managedServerVmIdResults.data.vmId) {
							vmId = managedServerVmIdResults.data.vmId
						}
						vmRefId = null
					} else { // if(zone.zoneType.equals("vmware")) {
						opts.cloudType = apiService.CLOUD_TYPE_VMWARE
						vmRefId = computeServer.externalId
					}
					def resultCount = morpheus.services.backup.backupResult.count(resultCountQuery.withFilter("backupType", 'default'))
					hasFullBackup = (resultCount > 0)

					if(!vmId) {
						def vmIdResults
						if(vmRefId) {
							vmIdResults = apiService.lookupVm(authConfig.apiUrl, token, opts.cloudType, hierarchyRoot, vmRefId)
							vmId = vmIdResults.vmId
						} else { //no vmRefId, lookup by name
							vmIdResults = apiService.lookupVmByName(authConfig.apiUrl, token, hierarchyRoot, vmName)
							vmId = vmIdResults.vmId
						}
					}

					if(vmId) {
						if(hasFullBackup) {
							rtn = apiService.startQuickBackup(authConfig, backupServerId, vmId, [lastBackupSessionId: lastResult?.getConfigProperty("backupSessionId")])
							rtn.data.backupResult.backupType = 'quickbackup'
						} else {
							BackupRepository repository = backup.backupRepository
							if(!repository) {
								repository = morpheus.services.backupRepository.find(new DataQuery().withFilter("category", "veeam.repository.${backupProvider.id}"))
							}
							if(repository) {
								rtn = apiService.startVeeamZip(authConfig, backupServerId, repository.externalId, vmId, [lastBackupSessionId: lastResult?.getConfigProperty("backupSessionId"), vmwToolsInstalled: computeServer.toolsInstalled])
								rtn.data.backupResult.backupType = 'veeamzip'
							} else {
								rtn.msg = "Backup repository not found."
							}
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
		} catch (e) {
			log.error("executeBackup error: ${e}", e)
		}
		return rtn
	}

	/**
	 * Periodically call until the backup execution has successfully completed. The default refresh interval is 60 seconds.
	 * @param backupResult the reference to the results of the backup execution including the last known status. Set the
	 *                     status to a canceled/succeeded/failed value from one of the {@link BackupStatusUtility} values
	 *                     to end the execution process.
	 * @return a {@link ServiceResponse} indicating the success or failure of the method. A success value
	 * of 'false' will halt the further execution process.n
	 */
	@Override
	ServiceResponse<BackupExecutionResponse> refreshBackupResult(BackupResult backupResult) {
		ServiceResponse<BackupExecutionResponse> rtn = ServiceResponse.prepare(new BackupExecutionResponse(backupResult))
		if(backupResult.backup.backupProvider.enabled) {
			try{
				def backup = backupResult.backup
				def backupProvider = backup.backupProvider
				def backupResultConfig = backupResult.configMap
				def authConfig = apiService.getAuthConfig(backupProvider)
				def session = apiService.loginSession(backupProvider)
				def token = session.token
				def sessionId = session.sessionId
				def apiUrl = authConfig.apiUrl
				if(backupResultConfig.backupSessionId) {
					log.info("Fetching veeam info for backup ${backupResult.id} session ${backupResultConfig.backupSessionId ?: "<No session ID found>"}")
					def getBackupResult = apiService.getBackupResult(apiUrl, token, backupResultConfig.backupSessionId)
					def backupSession = getBackupResult.result
					apiService.logoutSession(apiUrl, token, sessionId)

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
								def config = rtn.data.backupResult.getConfigMap()
								if(restoreLink) {
									config.restoreLink = restoreLink
									config.restoreType = restoreLink.type.toString()
									config.restoreHref = restoreLink.href.toString()
									config.restoreRef = apiService.extractVeeamUuid(config.restoreHref.toString())
								} else {
									def objectRef = apiService.getVmHierarchyObjRef(backupResult.backup)
									def restorePoint = apiService.getRestorePoint(authConfig, objectRef, [startRefDateStr: startDate])
									if(restorePoint.data?.externalId) {
										config.restorePointRef = restorePoint.data.externalId
									}
								}
								rtn.data.backupResult.setConfigMap(config)
								doUpdate = true
							}
						}
						rtn.data.updates = doUpdate
						rtn.success = true
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
	@Override
	ServiceResponse cancelBackup(BackupResult backupResult, Map opts) {
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
				rtn = apiService.stopBackupJob(apiUrl, token, backupJobId)
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
	@Override
	ServiceResponse extractBackup(BackupResult backupResultModel, Map opts) {
		return ServiceResponse.success()
	}



	def findManagedServerVmId(Map authConfig, token, cloudType, BackupProvider backupProvider, vmRefId) {
		def rtn = [success:true, data:[:]]
		try {
			morpheus.services.referenceData.list(new DataQuery().withFilters(
			        new DataFilter("category", "veeam.backup.managedServer.${backupProvider.id}"),
			        new DataFilter("typeValue", 'VC')
			)).each {ReferenceData managedServer ->
				if(!rtn.data.size()) {
					def rootRef = managedServer.getConfigProperty("hierarchyRootUid")
					if (rootRef) {
						def result = apiService.getVmId(authConfig.apiUrl, token, cloudType, rootRef, vmRefId)
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
