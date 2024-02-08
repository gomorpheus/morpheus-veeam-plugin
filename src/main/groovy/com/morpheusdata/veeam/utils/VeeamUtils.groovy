package com.morpheusdata.veeam.utils

import com.morpheusdata.model.Backup
import com.morpheusdata.model.BackupProvider
import com.morpheusdata.model.BackupResult
import com.morpheusdata.model.ComputeServer
import com.morpheusdata.model.Workload
import groovy.util.logging.Slf4j

@Slf4j
class VeeamUtils {

	static CLOUD_TYPE_VMWARE = "VMware"
	static CLOUD_TYPE_HYPERV = "HyperV"
	static CLOUD_TYPE_SCVMM = "Scvmm"
	static CLOUD_TYPE_VCD = "vCloud"

	static getBackupStatus(backupSessionState) {
		log.debug("getBackupStatus: ${backupSessionState}")
		def status = BackupResult.Status.IN_PROGRESS.toString()
		if(backupSessionState == "Failed") {
			status = BackupResult.Status.FAILED.toString()
		} else if(backupSessionState == "Running" || backupSessionState == "None") {
			status = BackupResult.Status.IN_PROGRESS.toString()
		} else if(backupSessionState == "Warning" || backupSessionState == "Success") {
			status = BackupResult.Status.SUCCEEDED.toString()
		}
		return status
	}

	static getApiVersion(BackupProvider backupProvider) {
		backupProvider.getConfigProperty('apiVersion')?.toFloat()
	}

	static getHierarchyRoot(Backup backup) {
		def veeamApiVersion = getApiVersion(backup.backupProvider)
		def hierarchyRootUid = backup.getConfigProperty("veeamHierarchyRootUid")
		def managedServerId =  backup.getConfigProperty("veeamManagedServerId")?.split(':')?.getAt(0)

		def hierarchyRoot = "urn:veeam:HierarchyRoot:${managedServerId}"
		if(veeamApiVersion && veeamApiVersion > 1.3) {
			hierarchyRoot = hierarchyRootUid
		}

		return hierarchyRoot
	}

	static getBackupServerId(Backup backup) {
		backup.getConfigProperty("veeamManagedServerId")?.split(':')?.getAt(1)
	}

	static getManagedServerId(Backup backup) {
		backup.getConfigProperty("veeamManagedServerId")?.split(":").getAt(0)
	}

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

	static extractMOR(String uuid) {
		def rtn = uuid
		def lastPeriod = rtn?.lastIndexOf('.')
		if(lastPeriod > -1)
			rtn = rtn.substring(lastPeriod + 1)
		def lastQuestion = rtn?.lastIndexOf('?')
		if(lastQuestion > -1)
			rtn = rtn.substring(0, lastQuestion)

		return rtn
	}

	static parseEntityId(String href) {
		def rtn
		def firstSlash = href.lastIndexOf('/')
		if(firstSlash > -1) {
			def firstQuestion = href.indexOf('?')
			if(firstQuestion > -1)
				rtn = href.substring(firstSlash + 1, firstQuestion)
			else
				rtn = href.substring(firstSlash + 1)
		}
		return rtn
	}

	static getVmHierarchyObjRef(Backup backup, ComputeServer server, String cloudType) {
		def objRef = backup.getConfigProperty('hierarchyObjRef')
		if(!objRef) {
			def hierarchyRootUid = backup.getConfigProperty("veeamHierarchyRootUid")
			def managedServerId = hierarchyRootUid ? hierarchyRootUid : backup.getConfigProperty("veeamManagedServerId")?.split(":")?.getAt(0)
			log.debug("server: ${server}, cloud: ${server?.cloud}, cloudType: ${server?.cloud?.cloudType}, cloudId: ${server?.cloud?.id}}")
			if(server) {
				objRef = getVmHierarchyObjRef(server.externalId, managedServerId, cloudType)
			}
		}


		return objRef
	}
	static getVmHierarchyObjRef(Backup backup, String vmRefId, String cloudType) {
		String managedServerId = getHierarchyRoot(backup)
		return getVmHierarchyObjRef(vmRefId, managedServerId, cloudType)
	}

	static getVmHierarchyObjRef(vmRefId, managedServerId, cloudType) {
		def parentServerId = managedServerId
		if(managedServerId.contains("urn:veeam")) {
			parentServerId = extractVeeamUuid(managedServerId)
		}
		return "urn:${cloudType}:${cloudType == CLOUD_TYPE_VCD ? "Vapp" : "Vm"}:${parentServerId}.${vmRefId}"
	}

}
