package com.morpheusdata.veeam.backup

import com.morpheusdata.core.MorpheusContext
import com.morpheusdata.core.Plugin
import com.morpheusdata.core.backup.AbstractBackupTypeProvider
import com.morpheusdata.model.Backup
import com.morpheusdata.model.BackupProvider
import com.morpheusdata.model.ComputeServer
import com.morpheusdata.veeam.services.ApiService
import com.morpheusdata.veeam.utils.VeeamUtils
import groovy.util.logging.Slf4j

@Slf4j
abstract class VeeamBackupTypeProvider extends AbstractBackupTypeProvider {

	ApiService apiService

	VeeamBackupTypeProvider(Plugin plugin, MorpheusContext context, ApiService apiService) {
		super(plugin, context)
		this.apiService = apiService
	}

	abstract String getCloudType();

	abstract String getManagedServerType();

	/**
	 * Get the VM reference ID for the given compute server
	 * @param computeServer
	 * @return the VM reference ID for the given compute server in the backup provider. This should be the ID of the VM in the virtualization provider of the VM.
	 */
	abstract String getVmRefId(ComputeServer computeServer)

	String getVeeamObjectRef(Map authConfig, String token, Backup backup, BackupProvider backupProvider, ComputeServer computeServer) {
		def objRef = null
		String hierarchyRoot = VeeamUtils.getHierarchyRoot(backup)
		def vmRefId = getVmRefId(computeServer)
		if(vmRefId) {
			def vmIdResults = apiService.lookupVm(authConfig.apiUrl, token, getVmHierarchyObjRef(vmRefId, hierarchyRoot))
			objRef = vmIdResults.vmId
		} else {
			//no vmRef, lookup by name
			def vmIdResults = apiService.lookupVmByName(authConfig.apiUrl, token, hierarchyRoot, computeServer.name)
			objRef = vmIdResults.vmId
		}

		return objRef
	}

	String getVmHierarchyObjRef(Backup backup, ComputeServer server) {
		def objRef = backup.getConfigProperty('hierarchyObjRef')
		if(!objRef) {
			def hierarchyRootUid = backup.getConfigProperty("veeamHierarchyRootUid")
			def managedServerId = hierarchyRootUid ? hierarchyRootUid : backup.getConfigProperty("veeamManagedServerId")?.split(":")?.getAt(0)
			log.debug("server: ${server}, cloud: ${server?.cloud}, cloudType: ${server?.cloud?.cloudType}, cloudId: ${server?.cloud?.id}}")
			if(server) {
				objRef = getVmHierarchyObjRef(server.externalId, managedServerId)
			}
		}

		return objRef
	}

	String getVmHierarchyObjRef(Backup backup, String vmRefId) {
		String managedServerId = VeeamUtils.getHierarchyRoot(backup)
		return getVmHierarchyObjRef(vmRefId, managedServerId)
	}

	String getVmHierarchyObjRef(vmRefId, managedServerId) {
		if(!vmRefId || !managedServerId) {
			return null
		}
		def parentServerId = managedServerId
		if(managedServerId.contains("urn:veeam")) {
			parentServerId = VeeamUtils.extractVeeamUuid(managedServerId)
		}
		return "urn:${cloudType}:Vm:${parentServerId}.${vmRefId}"
	}

	String updateObjectAndHierarchyRefs(Backup backup, String veeamObjectRef, String veeamHierarchyRef) {
		def doSaveBackup = false
		if(backup.getConfigProperty("veeamHierarchyRef") != veeamHierarchyRef) {
			backup.setConfigProperty("veeamHierarchyRef", veeamHierarchyRef)
			doSaveBackup = true
		}

		if(backup.getConfigProperty("veeamObjectRef") != veeamObjectRef) {
			backup.setConfigProperty("veeamObjectRef", veeamObjectRef)
			doSaveBackup = true
		}

		if(doSaveBackup) {
			morpheus.services.backup.save(backup)
		}
	}

}
