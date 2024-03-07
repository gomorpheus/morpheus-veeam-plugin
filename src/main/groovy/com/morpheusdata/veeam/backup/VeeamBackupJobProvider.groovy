package com.morpheusdata.veeam.backup

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
import com.morpheusdata.model.ExecuteScheduleType
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

	VeeamBackupJobProvider(Plugin plugin, MorpheusContext morpheus, ApiService apiService) {
		this.plugin = plugin
		this.morpheus = morpheus
		this.apiService = apiService
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
			def backupProvider = backupJobModel.backupProvider
			log.debug("cloneBackupJob, backupProvider: {}", backupProvider)
			def authConfig = apiService.getAuthConfig(backupProvider)
			def cloneId = VeeamUtils.extractVeeamUuid(sourceBackupJobModel.externalId)
			def jobName = "${backupJobModel.name}-${backupJobModel.account.id}"

			//make sure we have a repository
			Long repositoryId = null
			if(opts.target) {
				repositoryId = opts.target.toLong()
			} else if(opts.backupRepository) {
				repositoryId = opts.backupRepository.toLong()
			}
			if(repositoryId) {
				backupJobModel.backupRepository = morpheus.services.backupRepository.get(repositoryId)
			}

			// in 6.3.5/7.0 the plugin service will handle the schedule. If schedule is blank then
			// we're on version 6.3.4 or older and we'll need to set the schedule here
			if(backupJobModel.scheduleType == null) {
				Long jobScheduleId = opts.jobSchedule ? opts.jobSchedule.toLong() : null
				if(jobScheduleId) {
					ExecuteScheduleType jobSchedule = morpheus.services.executeScheduleType.get(jobScheduleId)
					backupJobModel.scheduleType = jobSchedule
					try {
						backupJobModel.nextFire = morpheus.services.executeScheduleType.calculateNextFire(jobSchedule)
					} catch(Exception e) {
						// ignore if next fire calculation fails
					}
				}
			}
			// clear retention count, we're not using it for veeam
			backupJobModel.retentionCount = null
			log.debug("cloneBackupJob, backupJobModel: {}, repository: {}", backupJobModel.id, backupJobModel.backupRepository?.internalId)
			//make api call
			def apiOpts = [jobName:jobName, repositoryId:backupJobModel.backupRepository?.internalId]
			log.debug("cloneBackupJob, apiOpts: {}", apiOpts)
			def cloneResults = apiService.cloneBackupJob(authConfig, cloneId, apiOpts)
			log.info("cloneResults: {}", cloneResults)
			//check results
			if(cloneResults.success && cloneResults.jobId) {
				backupJobModel.internalId = cloneResults.jobId
				backupJobModel.externalId = 'urn:veeam:Job:' + cloneResults.jobId
				backupJobModel.category = 'veeam.job.' + backupProvider.id
				backupJobModel.code = backupJobModel.category + '.' + backupJobModel.externalId
				backupJobModel.cronExpression = cloneResults.scheduleCron

				rtn.data = backupJobModel
				rtn.success = true
			} else {
				rtn.success = false
				rtn.msg = cloneResults.msg ?: 'Unable to clone backup job'
			}
		} catch(e) {
			log.error("cloneBackupJob error: ${e}", e)
		}

		log.debug("cloneBackupJob results: {}", rtn)
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
			def backupJobId = VeeamUtils.extractVeeamUuid(backupJobModel?.externalId)
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
				def backupsResponse = apiService.getBackupJobBackups(authConfig.apiUrl, session.token, backupJobId)
				// check if there are any backups for this job that morpheus is not aware of and would prevent the job from being deleted. If removing
				// the job and we're aware there are backups in the job but still want to remove it, we'll need to force it.
				log.debug("backup job backups: {}", backupsResponse.data?.size())
				if(backupsResponse.success && (backupsResponse.data?.size() <= 0 || opts.force == true)) {
					if(apiVersion > 1.4) {
						rtn = apiService.deleteBackupJob(apiUrl, token, backupJobId)
					} else {
						//Note: This Veeam API version does not allow delete backup job, best we can do is turn off schedule
						rtn = apiService.disableBackupJobSchedule(apiUrl, token, backupJobId)
					}
				} else {
					rtn.success = false
					rtn.msg = "Backup job is in use and cannot be deleted."
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
		log.debug("backupJobModel.externalId: {}", backupJobModel.backupProvider.enabled)
		ServiceResponse<List<BackupExecutionResponse>> rtn = ServiceResponse.prepare(new ArrayList<BackupExecutionResponse>())
		rtn.success = false
		if(backupJobModel.backupProvider.enabled == false) {
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
			for(Backup backup in jobBackups) {
				BackupResult backupResult = new BackupResult(backup: backup)
				backupResult.backupType = "default"
				backupResult.setConfigProperty("backupSessionId", veeamResult.backupSessionId)
				def executionResponse = new BackupExecutionResponse(backupResult)
				executionResponse.updates = true
				log.debug("rtn.data: ${rtn.data}")
				rtn.data.add(executionResponse)
			}

			rtn.success = true
			apiService.logoutSession(authConfig.apiUrl, token, sessionId)
		} catch(e) {
			log.error("Failed to execute backup job ${backupJobModel.id}: ${e}", e)
			rtn.msg = "Failed to execute backup job ${backupJobModel.id}"
		}
		return rtn
	}
}
