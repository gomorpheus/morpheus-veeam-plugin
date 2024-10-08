package com.morpheusdata.veeam.backup

import com.morpheusdata.core.MorpheusContext
import com.morpheusdata.core.Plugin
import com.morpheusdata.core.backup.BackupRestoreProvider
import com.morpheusdata.core.backup.response.BackupRestoreResponse
import com.morpheusdata.core.data.DataFilter
import com.morpheusdata.core.data.DataQuery
import com.morpheusdata.core.util.DateUtility
import com.morpheusdata.model.BackupProvider
import com.morpheusdata.model.Cloud
import com.morpheusdata.model.ComputeServer
import com.morpheusdata.model.Workload
import com.morpheusdata.response.ServiceResponse;
import com.morpheusdata.model.BackupRestore;
import com.morpheusdata.model.BackupResult;
import com.morpheusdata.model.Backup;
import com.morpheusdata.model.Instance
import com.morpheusdata.veeam.services.ApiService
import com.morpheusdata.veeam.utils.VeeamUtils
import groovy.util.logging.Slf4j
import groovy.xml.StreamingMarkupBuilder

@Slf4j
interface VeeamBackupRestoreProviderInterface extends BackupRestoreProvider {

	Plugin getPlugin()

	MorpheusContext getMorpheus()

	ApiService getApiService()

	VeeamBackupTypeProvider getBackupTypeProvider()

	/**
	 * Add additional configurations to a backup restore. Morpheus will handle all basic configuration details, this is a
	 * convenient way to add additional configuration details specific to this backup restore provider.
	 * @param backupResultModel backup result to be restored
	 * @param config the configuration supplied by external inputs
	 * @param opts optional parameters used for configuration.
	 * @return a {@link ServiceResponse} object. A ServiceResponse with a false success will indicate a failed
	 * configuration and will halt the backup restore process.
	 */
	default ServiceResponse configureRestoreBackup(BackupResult backupResult, Map config, Map opts) {
		return ServiceResponse.success()
	}

	/**
	 * Build the configuration for the restored instance.
	 * @param backupResultModel backup result to be restored
	 * @param instanceModel the instance the backup was created from, if it still exists. Retained backups will not have a reference to the instance.
	 * @param restoreConfig the restore configuration generated by morpheus.
	 * @param opts optional parameters used for configuration.
	 * @return a {@link ServiceResponse} object. A ServiceResponse with a false success will indicate a failed
	 * configuration and will halt the backup restore process.
	 */
	default ServiceResponse getBackupRestoreInstanceConfig(BackupResult backupResult, Instance instanceModel, Map restoreConfig, Map opts) {
		return ServiceResponse.success()
	}

	/**
	 * Verify the backup restore is valid. Generally used to check if the backup and instance are both in a state
	 * compatible for executing the restore process.
	 * @param backupResultModel backup result to be restored
	 * @param opts optional parameters used for configuration.
	 * @return a {@link ServiceResponse} object. A ServiceResponse with a false success will indicate a failed
	 * configuration and will halt the backup restore process.
	 */
	default ServiceResponse validateRestoreBackup(BackupResult backupResult, Map opts) {
		return ServiceResponse.success()
	}

	/**
	 * Get restore options to configure the restore wizard. Although the {@link com.morpheusdata.core.backup.BackupProvider } and
	 * {@link com.morpheusdata.core.backup.BackupTypeProvider} supply configuration, there may be situations where the instance
	 * configuration will determine which options need to be presented in the restore wizard.
	 * <p>
	 * Available Restore options:
	 * 		<ul>
	 * 		 	<li>
	 * 		 	    restoreExistingEnabled (Boolean) -- determines the visibility of the restore to existing option
	 * 		 	</li>
	 * 		 	<li>
	 * 		 	  	restoreNewEnabled (Boolean) -- determines the visibility of the restore to new option
	 * 		 	</li>
	 * 		 	<li>
	 * 		 	  	name (String) -- default name of the restored instance
	 * 		 	</li>
	 * 		 	<li>
	 * 		 		hostname (String) -- default hostname of the restored instance
	 * 		 	</li>
	 * 		</ul>
	 *
	 * @param backupModel the backup
	 * @param opts optional parameters
	 * @return a {@link ServiceResponse} object. A ServiceResponse with a false success will indicate a failed
	 * configuration and will halt the backup restore process.
	 */
	default ServiceResponse getRestoreOptions(Backup backup, Map opts) {
		log.debug "getRestoreOptions: backup: ${backup}, opts: ${opts}"
		ServiceResponse rtn = ServiceResponse.prepare()

		//If the backup has infrastructure config save, but the hostname no longer exists in cloud then we need to create a new VM.
		//If the hostname still exists in the cloud then we can restore to the existing VM.
		def restoreOptions = [restoreExistingEnabled:true]
		Workload workload = morpheus.services.workload.get(backup.containerId)
		//if original workload still exists, restore to that
		if(workload) {
			restoreOptions.restoreExistingEnabled = true
			restoreOptions.hostname = workload.server?.hostname
		} else {
			def infrastructureConfig = backup.getConfigProperty('infrastructureConfig')
			if(infrastructureConfig) {
				//original workload was removed and backup was preserved
				def name = infrastructureConfig.server?.displayName
				Cloud cloud = morpheus.services.cloud.get(infrastructureConfig.server?.zoneId)
				def serverResults = []
				morpheus.services.computeServer.list(new DataQuery().withFilters(
						new DataFilter<>('zone.id', cloud.id),
						new DataFilter<>('name', name)
				)).each { ComputeServer server ->
					if(server.externalId != null) {
						serverResults << server
					}
				}
				def server = serverResults.size() == 1 ? serverResults.getAt(0) : null

				//if there is a server with the same name in the same cloud, it was already re-created so restore there
				if(server) {
					restoreOptions.name = name
					Workload restoreWorkload = morpheus.services.workload.find(new DataQuery().withFilter('server.id', server.id))
					if(restoreWorkload) {
						restoreOptions.restoreExistingEnabled = true
						restoreOptions.restoreContainerId = restoreWorkload?.id
					} else {
						// only a portion of the infrastructure remains, probably an incomplete
						// delete of the source instance, force a new restore.
						restoreOptions.restoreNewEnabled = true
						restoreOptions.restoreExistingEnabled = false
					}
				} else {
					restoreOptions.restoreNewEnabled = true
					restoreOptions.restoreExistingEnabled = false
				}
			}
		}
		rtn.data = restoreOptions
		rtn.success = true
		return rtn
	}

	/**
	 * Execute the backup restore on the external system
	 * @param backupRestoreModel restore to be executed
	 * @param backupResultModel reference to the backup result
	 * @param backupModel reference to the backup associated with the backup result
	 * @param opts optional parameters
	 * @return a {@link ServiceResponse} object. A ServiceResponse with a false success will indicate a failed
	 * configuration and will halt the backup restore process.
	 */
	default ServiceResponse restoreBackup(BackupRestore backupRestore, BackupResult backupResult, Backup backup, Map opts) {
		log.debug("restoreBackup, restore: {}, source: {}, opts: {}", backupRestore, backupResult, opts)
		ServiceResponse rtn = ServiceResponse.prepare(new BackupRestoreResponse(backupRestore))
		def backupSessionId = backupResult.externalId ?: backupResult.getConfigProperty('backupSessionId')
		log.info("Restoring backupResult {} - opts: {}", backupResult, opts)
		try {
			def containerId = opts.containerId ?: backup?.containerId
			Workload workload = containerId ? morpheus.services.workload.find(new DataQuery().withFilter("id", containerId).withJoins("server", "server.zone", "server.zone.zoneType")) : null
			ComputeServer server = workload?.server
			Cloud cloud = server.cloud
			def hierarchyRoot = backup.getConfigProperty('hierarchyRoot')
			def objectRef = getRestoreObjectRef(backupTypeProvider.getVmHierarchyObjRef(backup, server))
			def authConfig = apiService.getAuthConfig(backup.backupProvider)
			def tokenResults = apiService.getToken(authConfig)
			def token = tokenResults.token
			def sessionId = tokenResults.sessionId
			def infrastructureConfig = backupResult.getConfigProperty('infrastructureConfig') ?: backup.getConfigProperty('infrastructureConfig')


			def restoreOpts = buildApiRestoreOpts(authConfig, backupResult, backup, server, cloud)
			log.debug("restoreBackup:[apiUrl: {}, vmId: {}, backupSessionId: {}, opts: {}", authConfig.apiUrl, objectRef, backupSessionId, restoreOpts)
			String restorePath = getRestorePath(authConfig, token, backupResult, objectRef, backupSessionId, restoreOpts)
			String restoreSpec = buildRestoreSpec(restorePath, hierarchyRoot, opts.backupType, restoreOpts)
			def restoreResults = apiService.restoreVM(authConfig.apiUrl, token, restorePath, restoreSpec)

			log.debug("restoreBackup result: {}", restoreResults)
			if(restoreResults.success) {
				//update instance status to restoring
				if(workload.instance?.id) {
					def instance = morpheus.services.instance.get(workload.instance.id)
					if(instance) {
						instance.status = Instance.Status.restoring.toString()
						morpheus.services.instance.save(instance)
					}
				}

				server = morpheus.services.computeServer.get(server.id)
				server.internalName = infrastructureConfig?.server?.name
				morpheus.services.computeServer.save(server)

				backupRestore.status = BackupRestore.Status.IN_PROGRESS.toString()
				backupRestore.externalStatusRef = restoreResults.restoreSessionId
				backupRestore.containerId = workload.id
				rtn.data.updates = true
				rtn.success = true
			} else {
				backupRestore.status = BackupRestore.Status.FAILED.toString()
				backupRestore.errorMessage = restoreResults.msg
				rtn.data.updates = true

				if(workload.instance?.id) {
					def instance = morpheus.services.instance.get(workload.instance.id)
					if(instance) {
						instance.status = Instance.Status.failed.toString()
						morpheus.services.instance.save(instance)
					}
				}
			}

			if(sessionId) {
				apiService.logoutSession(authConfig.apiUrl, token, sessionId)
			}
		} catch(e) {
			log.error("restoreBackup error", e)
			rtn.error = "Failed to restore Veeam backup: ${e}"
		}
		return rtn
	}

	/**
	 * Build the restore options for the restore operation. This is a convenient way to build the restore options for the
	 * restore operation. The restore options are used to configure the restore operation.
	 * @param authConfig the authentication configuration
	 * @param backupResult the backup result
	 * @param backup the backup
	 * @param server the compute server
	 * @param cloud the cloud
	 * @return the restore options as a map
	 */
	default Map buildApiRestoreOpts(Map authConfig, BackupResult backupResult, Backup backup, ComputeServer server, Cloud cloud) {
		def restoreOpts = [authConfig: authConfig, cloudTypeCode: cloud?.cloudType?.code, backupType: backupResult.backupType]
		if(backupResult.getConfigProperty("restoreHref")) {
			restoreOpts += backupResult.getConfigMap()
		} else {
			restoreOpts.restorePointRef = backupResult.getConfigProperty('restorePointRef') ?: backupResult.getConfigProperty('vmRestorePointRef')
			if(backupResult.getConfigProperty('restorePointRef')) {
				restoreOpts.restorePointId = VeeamUtils.extractVeeamUuid(backupResult.getConfigProperty('restorePointRef'))
			}
			if(backupResult.getConfigProperty('vmRestorePointRef')) {
				restoreOpts.vmRestorePointid = VeeamUtils.extractVeeamUuid(backupResult.getConfigProperty('vmRestorePointRef'))
			}
		}
		restoreOpts.vmId = server.externalId
		restoreOpts.vmName = server.displayName

		return restoreOpts
	}

	default String getRestoreObjectRef(String objectRef) {
		return objectRef
	}

	/**
	 * Get the restore path for the restore operation. There are several way the backup could store the restore link
	 * based on the type of backup (job, quickbackup, veeamzip) and version of veeam (9.4u4a, 10, 11, 12). The restore
	 * path is a URI that is used to execute the restore operation.
	 * @param authConfig the authentication configuration
	 * @param token the authentication token
	 * @param backupResult the backup result
	 * @param objectRef the object reference
	 * @param backupSessionId the backup session id
	 * @param opts optional parameters used for configuration.
	 * @return the restore path as a string
	 */
	default String getRestorePath(Map authConfig, String token, BackupResult backupResult, String objectRef, String backupSessionId, Map opts) {
		log.debug("getRestorePath: ${authConfig}, ${token}, ${backupResult}, ${objectRef}, ${backupSessionId}, ${opts}")
		String restoreLink = null
		if(opts.restoreHref) {
			restoreLink = getRestoreLinkFromRestoreHref(authConfig, opts.restoreHref, opts.restoreType)
		}
		// for everything not vcd, the restore point is set in the backup result config
		if(!restoreLink && (opts.restorePointId || opts.restoreRef)) {
			restoreLink = getRestoreLinkFromVmRestorePointId(authConfig, opts.restorePointId ?: opts.restoreRef)
		}
		if(opts.vmRestorePointId || (opts.restorePointId && !restoreLink)) {
			restoreLink = getRestoreLinkFromRestorePointId(authConfig, opts.vmRestorePointId ?: opts.restorePointId)
		}
		if(!restoreLink) {
			// we only have the backup session, find the restore resources
			def restorePointsLink = getRestorePointsLinkfromBackupSession(authConfig, backupSessionId)
			// probably a veeamzip, need to go find the restore point for the backup
			if(!restorePointsLink) {
				restorePointsLink = getRestorePointsLinkFromBackup(authConfig, backupSessionId)
			}

			if(restorePointsLink) {
				restoreLink = getRestoreLinkFromRestorePoints(authConfig, restorePointsLink, objectRef, opts.vmName, opts.vCenterVmId)
			}
		}

		return restoreLink
	}

	/**
	 * Get the restore link from the restore href. This is used to find the restore link from the restore href.
	 * @param authConfig the authentication configuration
	 * @param restoreHref the restore href
	 * @param restoreType the restore type
	 * @return the restore link as a string
	 */
	default String getRestoreLinkFromRestoreHref(Map authConfig, String restoreHref, String restoreType) {
		String restoreLink = null
		def response = apiService.callXmlApi(authConfig, restoreHref)
		log.debug("getRestoreLinkFromRestoreHref response: ${response}")
		if(response.success == true) {
			response.data[restoreType].Links.Link.each { link ->
				if(link['@Type'] == "Restore" || link['@Rel'] == "Restore") {
					restoreLink = new URI(link['@Href']?.toString()).path
				}
			}
		}

		return restoreLink
	}

	/**
	 * Get the restore link from the vm restore points. This is used to find the restore link from the vm restore points.
	 * @param authConfig the authentication configuration
	 * @param restorePointId the restore point id
	 * @return the restore link as a string
	 */
	default String getRestoreLinkFromVmRestorePointId(Map authConfig, String restorePointId) {
		String restoreLink = null
		def response = apiService.getVmRestorePointsFromRestorePointId(authConfig, restorePointId)
		log.debug("getVmRestorePointsFromRestorePointId response: ${response}")
		if(response.success == true) {
			response.data.VmRestorePoint.Links.Link.each { link ->
				if(link['@Type'] == "Restore" || link['@Rel'] == "Restore") {
					def restoreUrl = new URI(link['@Href']?.toString())
					restoreLink = restoreUrl.path
				}
			}
		}

		return restoreLink
	}

	/**
	 * Get the restore link from the vm restore points. This is used to find the restore link from the vm restore points.
	 * @param authConfig the authentication configuration
	 * @param restorePointId the restore point id
	 * @return the restore link as a string
	 */
	default String getRestoreLinkFromRestorePointId(Map authConfig, String restorePointId) {
		String restoreLink = null
		def response = apiService.getRestorePointFromRestorePointId(authConfig, restorePointId)
		log.debug("getRestoreLinkFromRestorePointId response: ${response}")
		if(response.success == true) {
			response.data.Links.Link.each { link ->
				if(link['@Rel'] == "Restore") {
					restoreLink = new URI(link['@Href']?.toString()).path
				}
			}
		}

		return restoreLink
	}

	/**
	 * Get the restore points link from the backup session. This is used to find the restore points link from the backup session.
	 * @param authConfig the authentication configuration
	 * @param backupSessionId the backup session id
	 * @return the restore points link as a string
	 */
	default String getRestorePointsLinkfromBackupSession(Map authConfig, String backupSessionId) {
		def restorePointsLink = null
		def backupSessionResponse = apiService.getBackupSession(authConfig, backupSessionId)
		log.debug("backupSession results: ${backupSessionResponse}")
		//find restore points
		if(backupSessionResponse?.success) {
			def backupSessionData = backupSessionResponse.data
			log.debug("backup results retore links: ${backupSessionData.Links.Link}")
			backupSessionData.Links.Link.each { link ->
				if(link['@Type'] == "RestorePointReference") {
					def restorePointsUrl = new URI(link['@Href'].toString())
					restorePointsLink = "${restorePointsUrl.path}/vmRestorePoints"
				}
				if(link['@Type'] == "VmRestorePoint") {
					restorePointsLink = new URI(link['@Href']?.toString()).path
				}
			}
		}

		return restorePointsLink
	}

	/**
	 * Get the restore points link from the backup. This is used to find the restore points link from the backup.
	 * @param authConfig the authentication configuration
	 * @param backupSessionId the backup session id
	 * @return the restore points link as a string
	 */
	default String getRestorePointsLinkFromBackup(Map authConfig, String backupSessionId) {
		def restorePointsLink = null
		def backupSessionResponse = apiService.getBackupSession(authConfig, backupSessionId)
		if(backupSessionResponse.success) {
			def backupName =  backupSessionResponse.data.jobName // the backup session and the backup(result) should have the same name
			def backupResults = apiService.fetchQuery(authConfig, "Backup", [Name: backupName])
			def restoreRefList = backupResults.data.refs?.ref?.links?.link?.find { it.type == "RestorePointReferenceList" }
			if(restoreRefList) {
				// get a list of restore points from the backup
				def refListResponse = apiService.callJsonApi(authConfig, restoreRefList.href)
				if(refListResponse?.success) {
					// we need the vm restore point to execute the restore
					refListResponse.data.RestorePoint.Links.Link.each { link ->
						if(link['Type'] == "VmRestorePointReferenceList") {
							restorePointsLink = new URI(link['Href']?.toString()).path
						}
					}
				}
			}
		}

		return restorePointsLink
	}

	/**
	 * Get the restore link from the restore points. This is used to find the restore link from the restore points.
	 * @param authConfig the authentication configuration
	 * @param restorePointsLink the restore points link
	 * @param objectRef the object reference
	 * @param vmName the vm name
	 * @param vCenterVmId the vCenter vm id
	 * @return the restore link as a string
	 */
	default String getRestoreLinkFromRestorePoints(Map authConfig, String restorePointsLink, String objectRef, String vmName, String vCenterVmId) {
		String restoreLink = null
		def restoreLinkResponse = apiService.callXmlApi(authConfig, restorePointsLink.toString())
		log.debug("got: ${restoreLinkResponse}")
		if(restoreLinkResponse?.success) {
			def restorePoint
			if(restoreLinkResponse.data.name() == "VmRestorePoints") {
				restorePoint = restoreLinkResponse.data.VmRestorePoint.find { it.HierarchyObjRef.text().toString().toLowerCase() == objectRef?.toLowerCase() || it.VmName.text().toString() == vmName }
				if(!restorePoint && opts.vCenterVmId) {
					restorePoint = restoreLinkResponse.data.VmRestorePoint.find { it.HierarchyObjRef.text().toString().endsWith(vCenterVmId) }
				}
			} else {
				restorePoint = restoreLinkResponse
			}

			if(restorePoint) {
				restorePoint.Links.Link.each { link ->
					if(link['@Rel']?.toString() == "Restore") {
						restoreLink = new URI(link['@Href']?.toString()).path
					}
				}
			}
		}

		return restoreLink
	}

	/**
	 * Build the restore spec for the restore operation. This is a convenient way to build the restore spec for the
	 * restore operation. The restore spec is an XML document that is used to configure the restore operation.
	 * @param restorePath the restore path
	 * @param hierarchyRoot the hierarchy root
	 * @param backupType the backup type
	 * @param opts optional parameters used for configuration.
	 * @return the restore spec as a string
	 */
	default String buildRestoreSpec(String restorePath, String hierarchyRoot, String backupType, Map opts) {
		def xml = new StreamingMarkupBuilder().bind() {
			RestoreSpec("xmlns": "http://www.veeam.com/ent/v1.0", "xmlns:xsd": "http://www.w3.org/2001/XMLSchema", "xmlns:xsi": "http://www.w3.org/2001/XMLSchema-instance") {
				VmRestoreSpec() {
					"PowerOnAfterRestore"(true)
					"QuickRollback"(false)
				}
			}
		}

		return xml.toString()
	}

	/**
	 * Periodically check for any updates to an in-progress restore. This method will be executed every 60 seconds for
	 * the restore while the restore has a status of `START_REQUESTED` or `IN_PROGRESS`. Any other status will indicate
	 * the restore has completed and does not need to be refreshed. The primary use case for this method is long-running
	 * restores to avoid consuming resources during the restore process.
	 * @param backupRestore the running restore
	 * @param backupResult backup result referencing the backup to be restored
	 * @return a {@link ServiceResponse} object. A ServiceResponse with a false success will indicate a failed
	 * configuration and will halt the backup restore process.
	 */
	default ServiceResponse refreshBackupRestoreResult(BackupRestore backupRestore, BackupResult backupResult) {
		log.debug "refreshBackupRestoreResult: ${backupRestore}, ${backupResult}"

		ServiceResponse<BackupRestoreResponse> rtn = ServiceResponse.prepare(new BackupRestoreResponse(backupRestore))
		Backup backup = (Backup) morpheus.services.backup.find(new DataQuery().withFilter("id", backupResult.backup.id).withJoins(["backupProvider", "backupProvider.account"]))
		BackupProvider backupProvider = backup.backupProvider
		if(!backupProvider.enabled) {
			rtn.msg = "Veeam not enabled"
			return rtn
		}

		try{
			log.debug("refreshBackupRestoreResult backupProvider: ${backupProvider}, accountId: ${backupProvider.account?.id}")
			def apiUrl = apiService.getApiUrl(backupProvider)
			def session = apiService.loginSession(backupProvider)
			def token = session.token
			def sessionId = session.sessionId
			def restoreSessionId = backupRestore.externalStatusRef
			if(!restoreSessionId) {
				rtn.msg = "No restore session found."
				return rtn
			}
			log.debug("get restore session: ${restoreSessionId}")
			def result = apiService.getRestoreResult(apiUrl, token, restoreSessionId)
			log.debug("restore session result: ${result}")
			def restoreSession = result.result
			if(sessionId) {
				apiService.logoutSession(apiUrl, token, sessionId)
			}

			log.debug "restoreSession: ${restoreSession}"
			if(restoreSession) {
				//update the restore with what we got back from veeam
				rtn.data.backupRestore.externalStatusRef = restoreSession.restoreSessionId
				rtn.data.backupRestore.externalId = restoreSession.vmId
				rtn.data.backupRestore.status = VeeamUtils.getBackupStatus(restoreSession.result)
				def startDate = restoreSession.startTime
				def endDate = restoreSession.endTime
				if(startDate && endDate) {
					def start = DateUtility.parseDate(startDate)
					def end = DateUtility.parseDate(endDate)
					rtn.data.backupRestore.startDate = start
					if(rtn.data.backupRestore.status == BackupResult.Status.SUCCEEDED.toString() || rtn.data.backupRestore.status == BackupResult.Status.FAILED.toString()) {
						rtn.data.backupRestore.endDate = end
					}
					rtn.data.backupRestore.lastUpdated = new Date()
					rtn.data.backupRestore.duration = (start && end) ? (end.time - start.time) : 0
					rtn.data.updates = true
				}

				if(rtn.data.backupRestore.status == BackupResult.Status.SUCCEEDED.toString() && rtn.data.backupRestore.externalId) {
					finalizeRestore(rtn.data.backupRestore)
				}
			}
		} catch(Exception ex) {
			log.error("syncBackupRestoreResult error", ex)
		}

		return rtn
	}

	/**
	 * Finalize the restore operation. This method is called after the restore has completed. This is a convenient way
	 * to perform any finalization steps after the restore has completed. This could include operations like updating
	 * the instance status, renewing the IP address, or any other post-restore operations.
	 * @param restore the restore to be finalized
	 * @return a {@link ServiceResponse} object. A ServiceResponse with a false success will indicate a failed
	 * configuration and will halt the backup restore process.
	 */
	default ServiceResponse finalizeRestore(BackupRestore restore) {
		log.info("finalizeRestore: {}", restore)
		def instance
		try {
			// Need to update the externalId as it has changed
			def targetWorkload = morpheus.services.workload.get(restore.containerId)
			// def server = targetWorkload?.server

			morpheus.async.backup.backupRestore.finalizeRestore(targetWorkload)

			// do we still need this with finalizeRestore above?
			// if(server?.zone?.zoneType?.code == 'vmware' && server?.serverOs?.vendor == 'centos') {
			// 	log.debug("Finalizing restore: renew IP for CentOS VM.")
			// 	morpheus.executeCommandOnServer(server, "dhclient", null, null, null, null, null, null, null, true, true)
			// }
		} catch(e) {
			log.error("Error in finalizeRestore: ${e}", e)
			instance?.status = Instance.Status.failed
			instance?.save(flush:true)
		}

		return ServiceResponse.success()
	}
}
