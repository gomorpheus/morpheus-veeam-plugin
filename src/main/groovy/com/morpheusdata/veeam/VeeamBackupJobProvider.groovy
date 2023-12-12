package com.morpheusdata.veeam

import com.morpheusdata.core.MorpheusContext
import com.morpheusdata.core.Plugin
import com.morpheusdata.core.backup.BackupJobProvider
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
import com.morpheusdata.veeam.utils.VeeamUtils
import groovy.util.logging.Slf4j
import org.slf4j.Logger
import org.slf4j.LoggerFactory

@Slf4j
class VeeamBackupJobProvider implements BackupJobProvider {

	Plugin plugin;
	MorpheusContext morpheus;
	ApiService apiService

	VeeamBackupJobProvider(Plugin plugin, MorpheusContext morpheus) {
		this.plugin = plugin
		this.morpheus = morpheus
		this.apiService = new ApiService()
	}

	@Override
	ServiceResponse configureBackupJob(BackupJob backupJob, Map config, Map opts) {
		if(config.target) {
			backupJob.backupRepository = morpheus.services.backupRepository.get(config.target.toLong())
		} else if(config.backupRepository) {
			backupJob.backupRepository = morpheus.services.backupRepository.get(config.backupRepository.toLong())
		}
		def jobScheduleId = config.jobSchedule
		if (jobScheduleId) {
			// TODO: create execution schedule services
			def jobSchedule = morpheus.services.executeScheduleType.get(jobScheduleId?.toLong())
			backupJob.scheduleType = jobSchedule
			backupJob.nextFire =  morpheus.services.executeScheduleType.calculateNextFire(jobSchedule)
		}
		backupJob.retentionCount = null
	}

	@Override
	ServiceResponse validateBackupJob(BackupJob backupJob, Map config, Map opts) {
		return ServiceResponse.success()
	}

	@Override
	ServiceResponse createBackupJob(BackupJob backupJob, Map map) {
		return ServiceResponse.error("Create backup job not supported.")
	}

	@Override
	ServiceResponse cloneBackupJob(BackupJob sourceBackupJobModel, BackupJob backupJobModel, Map opts) {
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
	ServiceResponse addToBackupJob(BackupJob backupJob, Map map) {
		return ServiceResponse.error("Add to backup job not supported.")
	}

	@Override
	ServiceResponse deleteBackupJob(BackupJob backupJobModel, Map opts) {
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
				def apiVersion = VeeamUtils.getApiVersion(backupProvider)
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
	ServiceResponse executeBackupJob(BackupJob backupJobModel, Map opts) {
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
			this.morpheus.services.backup.list(new DataQuery().withFilters(
			        new DataFilter<>('account.id', tmpAccount.id),
			        new DataFilter<>('backupJob.id', backupJobModel.id),
					new DataFilter<>('active', true)
			)).each { Backup backup ->
				if(!opts.user || backup.createdBy?.id == opts.user.id) {
					jobBackups << backup
				}
			}
			jobBackups = jobBackups.sort { it.instanceId }

			// TODO: this should probably be moved into core: backupResult.create(backup: backup, backupJob: backupJobModel)
			// where the backup service also generates the backup set ID
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
