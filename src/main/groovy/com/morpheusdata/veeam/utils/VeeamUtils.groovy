package com.morpheusdata.veeam.utils

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
}
