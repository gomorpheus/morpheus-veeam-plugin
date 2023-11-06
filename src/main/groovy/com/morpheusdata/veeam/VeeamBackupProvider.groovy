package com.morpheusdata.veeam

import com.morpheusdata.core.MorpheusContext
import com.morpheusdata.core.Plugin
import com.morpheusdata.core.backup.AbstractBackupProvider
import com.morpheusdata.core.backup.BackupJobProvider
import com.morpheusdata.core.backup.BackupTypeProvider
import com.morpheusdata.core.backup.DefaultBackupJobProvider
import com.morpheusdata.core.util.ConnectionUtils
import com.morpheusdata.model.BackupProvider as BackupProviderModel
import com.morpheusdata.model.Icon
import com.morpheusdata.model.OptionType
import com.morpheusdata.response.ServiceResponse
import com.morpheusdata.veeam.services.ApiService
import com.morpheusdata.veeam.sync.*
import groovy.json.JsonOutput
import groovy.util.logging.Slf4j

@Slf4j
class VeeamBackupProvider extends AbstractBackupProvider {

	ApiService apiService

	BackupJobProvider backupJobProvider;

	VeeamBackupProvider(Plugin plugin, MorpheusContext morpheusContext) {
		super(plugin, morpheusContext)
		apiService = new ApiService()

		VeeamVMWareBackupTypeProvider backupTypeProvider = new VeeamVMWareBackupTypeProvider(plugin, morpheus)
		plugin.registerProvider(backupTypeProvider)
		addScopedProvider(backupTypeProvider, "vmware", null)
	}

	/**
	 * A unique shortcode used for referencing the provided provider. Make sure this is going to be unique as any data
	 * that is seeded or generated related to this provider will reference it by this code.
	 * @return short code string that should be unique across all other plugin implementations.
	 */
	@Override
	String getCode() {
		return 'veeam'
	}

	/**
	 * Provides the provider name for reference when adding to the Morpheus Orchestrator
	 * NOTE: This may be useful to set as an i18n key for UI reference and localization support.
	 *
	 * @return either an English name of a Provider or an i18n based key that can be scanned for in a properties file.
	 */
	@Override
	String getName() {
		return 'Veeam'
	}
	
	/**
	 * Returns the integration logo for display when a user needs to view or add this integration
	 * @return Icon representation of assets stored in the src/assets of the project.
	 */
	@Override
	Icon getIcon() {
		return new Icon(path:"veeam.svg", darkPath: "veeam.svg")
	}

	/**
	 * Sets the enabled state of the provider for consumer use.
	 */
	@Override
	public Boolean getEnabled() { return true; }

	/**
	 * The backup provider is creatable by the end user. This could be false for providers that may be
	 * forced by specific CloudProvider plugins, for example.
	 */
	@Override
	public Boolean getCreatable() { return true; }
	
	/**
	 * The backup provider supports restoring to a new workload.
	 */
	@Override
	public Boolean getRestoreNewEnabled() { return false; }

	/**
	 * The backup provider supports backups. For example, a backup provider may be intended for disaster recovery failover
	 * only and may not directly support backups.
	 */
	@Override
	public Boolean getHasBackups() { return true; }

	/**
	 * The backup provider supports creating new jobs.
	 */
	@Override
	public Boolean getHasCreateJob() { return false; }

	/**
	 * The backup provider supports cloning a job from an existing job.
	 */
	@Override
	public Boolean getHasCloneJob() { return true; }

	/**
	 * The backup provider can add a workload backup to an existing job.
	 */
	@Override
	public Boolean getHasAddToJob() { return true; }

	/**
	 * The backup provider supports backups outside an encapsulating job.
	 */
	@Override
	public Boolean getHasOptionalJob() { return false; }

	/**
	 * The backup provider supports scheduled backups. This is primarily used for display of hte schedules and providing
	 * options during the backup configuration steps.
	 */
	@Override
	public Boolean getHasSchedule() { return true; }

	/**
	 * The backup provider supports running multiple workload backups within an encapsulating job.
	 */
	@Override
	public Boolean getHasJobs() { return true; }

	/**
	 * The backup provider supports retention counts for maintaining the desired number of backups.
	 */
	@Override
	public Boolean getHasRetentionCount() { return false; }

	@Override
	public Boolean getHasRepositories() { return true }

	@Override
	Boolean getHasServers() { return true }

	@Override
	String getDefaultJobType() { return null }

	/**
	 * Get the list of option types for the backup provider. The option types are used for creating and updating an
	 * instance of the backup provider.
	 */
	@Override
	Collection<OptionType> getOptionTypes() {
		Collection<OptionType> optionTypes = new ArrayList();
		optionTypes << new OptionType(
				code:"backupProviderType.${this.getCode()}.host", inputType:OptionType.InputType.TEXT, name:'host', category:"backupProviderType.${this.getCode()}",
				fieldName:'host', fieldCode: 'gomorpheus.optiontype.ApiUrl', fieldLabel:'Host', fieldContext:'domain', fieldGroup:'default',
				required:true, enabled:true, editable:true, global:false, placeHolder:null, helpBlock:'', defaultValue:null, custom:false,
				displayOrder:10, fieldClass:null
		)
		optionTypes << new OptionType(
				code:"backupProviderType.${this.getCode()}.port", inputType:OptionType.InputType.NUMBER, name:'port', category:"backupProviderType.${this.getCode()}",
				fieldName:'port', fieldCode: 'gomorpheus.optiontype.Port', fieldLabel:'Port', fieldContext:'domain', fieldGroup:'default',
				required:false, enabled:true, editable:true, global:false, placeHolder:null, helpBlock:'', defaultValue:null, custom:false,
				displayOrder:15, fieldClass:null
		)
		optionTypes << new OptionType(
				code:"backupProviderType.${this.getCode()}.credential", inputType:OptionType.InputType.CREDENTIAL, name:'credentials', category:"backupProviderType.${this.getCode()}",
				fieldName:'type', fieldCode:'gomorpheus.label.credentials', fieldLabel:'Credentials', fieldContext:'credential', optionSource:'credentials',
				required:true, enabled:true, editable:true, global:false, placeHolder:null, helpBlock:'', defaultValue:'local', custom:false,
				displayOrder:25, fieldClass:null, wrapperClass:null, config: JsonOutput.toJson([credentialTypes:['username-password']]).toString()
		)
		optionTypes << new OptionType(
				code:"backupProviderType.${this.getCode()}.username", inputType:OptionType.InputType.TEXT, name:'username', category:"backupProviderType.${this.getCode()}",
				fieldName:'username', fieldCode: 'gomorpheus.optiontype.Username', fieldLabel:'Username', fieldContext:'domain', fieldGroup:'default',
				required:false, enabled:true, editable:true, global:false, placeHolder:null, helpBlock:'', defaultValue:null, custom:false,
				displayOrder:30, fieldClass:null, localCredential:true
		)
		optionTypes << new OptionType(
				code:"backupProviderType.${this.getCode()}.password", inputType:OptionType.InputType.PASSWORD, name:'password', category:"backupProviderType.${this.getCode()}",
				fieldName:'password', fieldCode: 'gomorpheus.optiontype.Password', fieldLabel:'Password', fieldContext:'domain', fieldGroup:'default',
				required:false, enabled:true, editable:true, global:false, placeHolder:null, helpBlock:'', defaultValue:null, custom:false,
				displayOrder:35, fieldClass:null, localCredential:true
		)
		return optionTypes
	}

	/**
	 * Get the list of replication group option types for the backup provider. The option types are used for creating and updating
	 * replication groups.
	 */
	@Override
	Collection<OptionType> getReplicationGroupOptionTypes() {
		Collection<OptionType> optionTypes = []
		return optionTypes
	}
	
	/**
	 * Get the list of replication option types for the backup provider. The option types are used for creating and updating
	 * replications.
	 */
	@Override
	Collection<OptionType> getReplicationOptionTypes() {
		Collection<OptionType> optionTypes = new ArrayList()
		return optionTypes;
	}

	/**
	 * Get the list of backup job option types for the backup provider. The option types are used for creating and updating
	 * backup jobs.
	 */
	@Override
	Collection<OptionType> getBackupJobOptionTypes() {
		Collection<OptionType> optionTypes = []
		return optionTypes;
	}

	/**
	 * Get the list of backup option types for the backup provider. The option types are used for creating and updating
	 * backups.
	 */
	@Override
	Collection<OptionType> getBackupOptionTypes() {
		Collection<OptionType> optionTypes = []
		return optionTypes;
	}
	
	/**
	 * Get the list of replication group option types for the backup provider. The option types are used for creating
	 * replications on an instance during provisioning.
	 */
	@Override
	Collection<OptionType> getInstanceReplicationGroupOptionTypes() {
		Collection<OptionType> optionTypes = new ArrayList()
		return optionTypes;
	}

	/**
	 * Get the {@link BackupJobProvider} responsible for all backup job operations in this backup provider
	 * The {@link DefaultBackupJobProvider} can be used if the provider would like morpheus to handle all job operations.
	 * @return the {@link BackupJobProvider} for this backup provider
	 */
	@Override
	BackupJobProvider getBackupJobProvider() {
		// The default backup job provider allows morpheus to handle the
		// scheduling and execution of the jobs. Replace the default job provider
		// if jobs are to be managed on the external backup system.
		if(!this.backupJobProvider) {
			this.backupJobProvider = new DefaultBackupJobProvider(getPlugin(), morpheus);
		}
		return this.backupJobProvider
	}

	/**
	 * Apply provider specific configurations to a {@link com.morpheusdata.model.BackupProvider}. The standard configurations are handled by the core system.
	 * @param backupProviderModel backup provider to configure
	 * @param config the configuration supplied by external inputs.
	 * @param opts optional parameters used for configuration.
	 * @return a {@link ServiceResponse} object. A ServiceResponse with a false success will indicate a failed
	 * configuration and will halt the backup creation process.
	 */
	@Override
	ServiceResponse configureBackupProvider(BackupProviderModel backupProviderModel, Map config, Map opts) {
		return ServiceResponse.success(backupProviderModel)
	}

	/**
	 * Validate the configuration of the {@link com.morpheusdata.model.BackupProvider}. Morpheus will validate the backup based on the supplied option type
	 * configurations such as required fields. Use this to either override the validation results supplied by the
	 * default validation or to create additional validations beyond the capabilities of option type validation.
	 * @param backupProviderModel backup provider to validate
	 * @param opts additional options
	 * @return a {@link ServiceResponse} object. A ServiceResponse with a false success will indicate a failed
	 * validation and will halt the backup provider creation process.
	 */
	@Override
	ServiceResponse validateBackupProvider(BackupProviderModel backupProviderModel, Map opts) {
		log.debug "validateBackupProvider: ${backupProviderModel}"
		def rtn = [success:false, errors:[:]]
		try {
			def apiOpts = [:]

			def localCredentials = (backupProviderModel.credentialData?.type ?: 'local') == 'local'
			if(localCredentials && !backupProviderModel?.credentialData?.password) {
				rtn.msg = rtn.msg ?: 'Enter a password'
				rtn.errors.password = 'Enter a password'
			}
			if(localCredentials && !backupProviderModel?.credentialData?.username) {
				rtn.msg = rtn.msg ?: 'Enter a usrename'
				rtn.errors.username = 'Enter a username'
			}
			if(rtn.errors.size() == 0) {
				def testResults = verifyAuthentication(backupProviderModel, apiOpts)
				log.debug("veeam test results: {}", testResults)
				if(testResults.success == true)
					rtn.success = true
				else if(testResults.invalidLogin == true)
					rtn.msg = testResults.msg ?: 'unauthorized - invalid credentials'
				else if(testResults.found == false)
					rtn.msg = testResults.msg ?: 'veeam not found - invalid host'
				else
					rtn.msg = testResults.msg ?: 'unable to connect to veeam'
			}
		} catch(e) {
			log.error("error validating veeam configuration: ${e}", e)
			rtn.msg = 'unknown error connecting to veeam'
			rtn.success = false
		}
		if(rtn.success) {
			return ServiceResponse.success(backupProviderModel, rtn.msg)
		} else {
			return ServiceResponse.error(rtn.msg, rtn.errors as Map, backupProviderModel)
		}
	}

	/**
	 * Delete the backup provider. Typically used to clean up any provider specific data that will not be cleaned
	 * up by the default remove in the core system.
	 * @param backupProviderModel the backup provider being removed
	 * @param opts additional options
	 * @return a {@link ServiceResponse} object. A ServiceResponse with a false success will indicate a failed
	 * delete and will halt the process.
	 */
	@Override
	ServiceResponse deleteBackupProvider(BackupProviderModel backupProviderModel, Map opts) {
		log.debug "deleteBackupProvider: ${backupProviderModel}"

		def rtn = [success: true, data:backupProviderModel]

		def msCleanupResults = clearManagedServers(backupProviderModel, opts)
		if(!msCleanupResults.success) {
			rtn.success = false
			rtn.msg = msCleanupResults.msg
		}
		if(rtn.success) {
			def bsCleanupResults = clearBackupServers(backupProviderModel, opts)
			if(!bsCleanupResults.success) {
				rtn.success = false
				rtn.msg = bsCleanupResults.msg
			}
		}

		return ServiceResponse.create(rtn)
	}

	/**
	 * The main refresh method called periodically by Morpheus to sync any necessary objects from the integration.
	 * This can call sub services for better organization. It is recommended that {@link com.morpheusdata.core.util.SyncTask} is used.
	 * @param backupProvider the current instance of the backupProvider being refreshed
	 * @return the success state of the refresh
	 */
	@Override
	ServiceResponse refresh(BackupProviderModel backupProviderModel) {
		log.debug "refresh: ${backupProviderModel}"

		ServiceResponse rtn = new ServiceResponse(success: false)
		try {
			def authConfig = apiService.getAuthConfig(backupProviderModel)
			def apiUrl = authConfig.apiUrl
			def apiUri = new URI(apiUrl)
			def apiHost = apiUri.getHost()
			def apiPort = apiUri.getPort() ?: apiUrl?.startsWith('https') ? 443 : 80

			def hostOnline = ConnectionUtils.testHostConnectivity(apiHost, apiPort, true, true, null)
			log.debug("veeam host online: {}", hostOnline)
			if(hostOnline) {
				def testResults = verifyAuthentication(backupProviderModel)
				if(testResults.success) {
					morpheus.async.backupProvider.updateStatus(backupProviderModel, 'ok', null).subscribe().dispose()

					new BackupJobSync(backupProviderModel, plugin).execute()
					new BackupRepositorySync(backupProviderModel, plugin).execute()
					new ManagedServerSync(backupProviderModel, plugin).execute()
					new BackupServerSync(backupProviderModel, plugin).execute()

					updateApiVersion(backupProviderModel)
					rtn.success = true
				} else {
					if(testResults.invalidLogin == true) {
						log.debug("refreshBackupProvider: Invalid credentials")
						morpheus.async.backupProvider.updateStatus(backupProviderModel, 'error', 'invalid credentials').subscribe().dispose()
					} else {
						log.debug("refreshBackupProvider: error connecting to host")
						morpheus.async.backupProvider.updateStatus(backupProviderModel, 'error', 'error connecting').subscribe().dispose()
					}
				}
			} else {
				log.error("refreshBackupProvider: host not reachable")
				morpheus.async.backupProvider.updateStatus(backupProviderModel, 'offline', 'Veeam not reachable').subscribe().dispose()
			}
		} catch(e) {
			log.error("refreshBackupProvider error: ${e}", e)
		}
		rtn
	}

	private verifyAuthentication(BackupProviderModel backupProviderModel, Map opts=[:]) {
		def rtn = [success:false, invalidLogin:false, found:true]
		opts.authConfig = opts.authConfig ?: apiService.getAuthConfig(backupProviderModel)
		def tokenResults = apiService.loginSession(opts.authConfig)
		if(tokenResults.success == true) {
			rtn.success = true
			def token = tokenResults.token
			def sessionId = tokenResults.sessionId
			apiService.logoutSession(opts.authConfig.apiUrl, token, sessionId)
		} else {
			if(tokenResults?.errorCode == '404' || tokenResults?.errorCode == 404)
				rtn.found = false
			if(tokenResults?.errorCode == '401' || tokenResults?.errorCode == 401)
				rtn.invalidLogin = true
		}
		return rtn
	}

	def clearManagedServers(BackupProviderModel backupProvider, Map opts=[:]) {
		def rtn = [success: true]
		try {
			// TODO
		} catch (Exception e) {
			log.error("Error removing managed servers for backup provider {}[{}]", backupProvider.name, backupProvider.id)
			rtn.msg = "Error removing managed servers: ${e}"
			rtn.success = false
		}
		return rtn

	}

	def clearBackupServers(BackupProviderModel backupProvider, Map opts=[:]) {
		def rtn = [success: true]
		try {
			// TODO
		} catch (Exception e) {
			log.error("Error removing backup servers for backup provider {}[{}]", backupProvider.name, backupProvider.id)
			rtn.msg = "Error removing backup servers: ${e}"
			rtn.success = false
		}
		return rtn
	}

	def updateApiVersion(BackupProviderModel backupProviderModel) {
		def rtn = [success:false]
		try {
			log.debug("Checking API version updates.")
			def authConfig = apiService.getAuthConfig(backupProviderModel)
			def latestVersionInfo = ApiService.getLatestApiVersion(authConfig)
			if(latestVersionInfo.success) {
				def currApiVersion = backupProviderModel.getConfigProperty('apiVersion')
				if(currApiVersion != latestVersionInfo.apiVersion) {
					log.debug("Updating Veeam API version to ${latestVersionInfo.apiVersion}.")
					backupProviderModel.setConfigProperty('apiVersion', latestVersionInfo.apiVersion)
					morpheus.async.backupProvider.bulkSave([backupProviderModel]).blockingGet()
				}
				rtn.success = true
			}
		} catch(e) {
			log.error("updateApiVersion error: ${e}", e)
		}
	}
}
