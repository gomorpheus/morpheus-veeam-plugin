package com.morpheusdata.veeam.utils

import com.morpheusdata.model.Backup
import com.morpheusdata.model.BackupProvider
import com.morpheusdata.model.BackupResult

class VeeamUtils {

	static getBackupStatus(backupSessionState) {
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
		def veeamApiVersion = getApiVersion(backup)
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

}
