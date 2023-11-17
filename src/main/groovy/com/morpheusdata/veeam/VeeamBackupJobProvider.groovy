package com.morpheusdata.veeam

import com.morpheusdata.core.MorpheusContext
import com.morpheusdata.core.Plugin
import com.morpheusdata.core.backup.DefaultBackupJobProvider
import com.morpheusdata.core.backup.response.BackupExecutionResponse
import com.morpheusdata.core.data.DataFilter
import com.morpheusdata.core.data.DataQuery
import com.morpheusdata.model.Account
import com.morpheusdata.model.Backup
import com.morpheusdata.model.BackupJob
import com.morpheusdata.model.BackupProvider
import com.morpheusdata.model.BackupResult
import com.morpheusdata.response.ServiceResponse
import com.morpheusdata.veeam.services.ApiService

class VeeamBackupJobProvider extends DefaultBackupJobProvider {

	ApiService apiService
	VeeamBackupExecutionProvider executionProvider

	public VeeamBackupJobProvider(Plugin plugin, MorpheusContext morpheus) {
		super(plugin, morpheus)
		this.apiService = new ApiService()
		this.executionProvider = new VeeamBackupExecutionProvider(plugin, morpheus)
	}

	@Override
	public ServiceResponse cloneBackupJob(BackupJob sourceBackupJobModel, BackupJob backupJobModel, Map opts) {
		def rtn = [success:false]
		try {
			def backupProvider = sourceBackupJobModel.backupProvider
			def authConfig = apiService.getAuthConfig(backupProvider)
			def cloneId = apiService.extractVeeamUuid(sourceBackupJobModel.externalId)
			def jobName = "${backupJobModel.name}-${backupJobModel.account.id}"
			//make api call
			def apiOpts = [jobName:jobName, repositoryId:backupJobModel.backupRepository?.internalId]
			def cloneResults = apiService.cloneBackupJob(authConfig, cloneId, apiOpts)
			log.info("cloneResults: {}", cloneResults)
			//check results
			if(cloneResults.success && cloneResults.jobId) {
				backupJobModel.internalId = cloneResults.jobId
				backupJobModel.externalId = 'urn:veeam:Job:' + cloneResults.jobId
				backupJobModel.category = 'veeam.job.' + backupProvider.id
				backupJobModel.code = backupJobModel.category + '.' + backupJobModel.externalId
				backupJobModel.cronExpression = cloneResults.scheduleCron
				rtn.success = true
			} else {
				rtn.success = false
				rtn.msg = cloneResults.msg ?: 'Unable to clone backup job'
			}
		} catch(e) {
			log.error("cloneBackupJob error: ${e}", e)
		}
		return ServiceResponse.create(rtn)
	}

	@Override
	public ServiceResponse deleteBackupJob(BackupJob backupJobModel, Map opts) {
		log.debug("deleteBackupJob: {}, {}", backupJobModel, opts)
		ServiceResponse rtn = ServiceResponse.prepare()
		try {
			def backupJobId = apiService.extractVeeamUuid(backupJobModel?.externalId)
			if(!backupJobId) {
				// no external ID just delete it.
				rtn.success = true
				return rtn
			}
			try{
				def backupProvider = backupJobModel.backupProvider
				def apiVersion = executionProvider.getApiVersion(backupProvider)
				def apiUrl = apiService.getApiUrl(backupProvider)
				def authConfig = apiService.getAuthConfig(backupProvider)
				def session = apiService.loginSession(authConfig)
				def token = session.token
				if(apiVersion > 1.4) {
					rtn = apiService.deleteBackupJob(apiUrl, token, backupJobId)
				} else {
					//Note: This Veeam API version does not allow delete backup job, best we can do is turn off schedule
					rtn = apiService.disableBackupJobSchedule(apiUrl, token, backupJobId)
				}
				if(!rtn.success) {
					rtn.msg = rtn.msg ?: "Unable to delete backup job"
				}
			} catch(Exception ex) {
				log.error("Failed to delete Veeam backup job: ${backupJobId}", ex)
			}
		} catch(e) {
			log.error("error deleting veeam backup: ${e.message}", e)
			throw new RuntimeException("Unable to remove backup: ${e.message}", e)
		}
		rtn
	}

	@Override
	public ServiceResponse executeBackupJob(BackupJob backupJobModel, Map opts) {
		log.debug("executeBackupJob: {}, {}", backupJobModel, opts)
		ServiceResponse<List<BackupExecutionResponse>> rtn = new ServiceResponse<List<BackupExecutionResponse>>()
		rtn.success = false
		if(backupJobModel.backupProvider.enabled) {
			rtn.msg = 'Veeam backup provider is disabled'
			return rtn
		}
		try {
			BackupProvider backupProvider = backupJobModel.backupProvider
			def authConfig = apiService.getAuthConfig(backupProvider)
			def session = apiService.loginSession(backupProvider)
			def token = session.token
			def sessionId = session.sessionId
			def veeamResult
			if(token) {
				veeamResult = apiService.startBackupJob(authConfig, backupJobModel.externalId, opts)
			} else {
				return ServiceResponse.error("Failed to start Veeam backup job, unable to acquire access token.")
			}

			Account tmpAccount = opts.account ?: backupJobModel.account
			List<Backup> jobBackups = []
			this.morpheus.async.backup.list(new DataQuery([
			        new DataFilter<>('account.id', tmpAccount.id),
			        new DataFilter<>('backupJob.id', backupJobModel.id),
					new DataFilter<>('active', true)
			])).blockingSubscribe { Backup backup ->
				if(!opts.user || backup.createdBy?.id == opts.user.id) {
					jobBackups << backup
				}
			}
			jobBackups = jobBackups.sort { it.instanceId }

			// For each backup associated to the job, save the results of the backup
			def currInstanceId
			def backupSetId = VeeamBackupExecutionProvider.generateDateKey(10)
			for(Backup backup in jobBackups) {
				if(currInstanceId != backup.instanceId) {
					currInstanceId = backup.instanceId
					backupSetId = VeeamBackupExecutionProvider.generateDateKey(10)
				}

				BackupResult backupResult = new BackupResult(backup: backup)
				executionProvider.updateBackupResult(backupResult, backup, veeamResult)
				veeamResult.backupSetId = backupSetId
				veeamResult.backupType = "default"
				rtn.data.add(new BackupExecutionResponse(backupResult: backupResult))
			}

			apiService.logoutSession(authConfig.apiUrl, token, sessionId)
		} catch(e) {
			log.error("Failed to execute backup job ${backupJobModel.id}: ${e}", e)
			rtn.msg = "Failed to execute backup job ${backupJobModel.id}"
		}
		rtn
	}
}
