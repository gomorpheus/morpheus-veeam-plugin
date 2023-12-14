package com.morpheusdata.veeam.utils

class VeeamScheduleUtils {

	static dayOfWeekList = [
		[index:1, name:'Sunday'],
		[index:2, name:'Monday'],
		[index:3, name:'Tuesday'],
		[index:4, name:'Wednesday'],
		[index:5, name:'Thursday'],
		[index:6, name:'Friday'],
		[index:7, name:'Saturday']
	]

	static monthList = [
		[index:1, name:'January'],
		[index:2, name:'February'],
		[index:3, name:'March'],
		[index:4, name:'April'],
		[index:5, name:'May'],
		[index:6, name:'June'],
		[index:7, name:'July'],
		[index:8, name:'August'],
		[index:9, name:'September'],
		[index:10, name:'October'],
		[index:11, name:'November'],
		[index:12, name:'December']
	]

	//scheduling
	static decodeScheduling(job) {
		def rtn
		//build a cron representation
		def scheduleSet = job.ScheduleConfigured
		def scheduleOn = job.ScheduleEnabled
		def optionsDaily = job.JobScheduleOptions?.OptionsDaily
		def optionsMonthly = job.JobScheduleOptions?.OptionsMonthly
		def optionsPeriodically = job.JobScheduleOptions?.OptionsPeriodically
		//build cron off the type
		if(optionsDaily['@Enabled'] == 'true') {
			//get the hour offset
			def timeOffset = optionsDaily.TimeOffsetUtc?.toLong()
			def hour = ((int)(timeOffset.div(3600l)))
			def minute = ((int)((timeOffset - (hour * 3600l)).div(60)))
			//build the string
			rtn = '0 ' + minute + ' ' + hour
			//get the days of the week
			if(optionsDaily.Kind == 'Everyday') {
				rtn = rtn + ' 	* * ?'
			} else {
				def dayList = []
				dayOfWeekList?.each { day ->
					if(optionsDaily.Days.find{ it.toString() == day.name }) {
						dayList << day.index
					}
				}
				rtn = rtn + ' ? * ' + dayList.join(',')
			}
		} else if(optionsMonthly['@Enabled'] == 'true') {
			def timeOffset = optionsMonthly.TimeOffsetUtc?.toLong()
			def hour = ((int)(timeOffset.div(3600l)))
			def minute = ((int)((timeOffset - (hour * 3600l)).div(60)))
			def day = optionsMonthly.DayOfMonth
			//cron can't handle the other style - fourth saturday of month
			//build the string
			rtn = '0 ' + minute + ' ' + hour + ' ' + day
			//get the days of the month
			def months = []
			monthList?.each { month ->
				if(optionsMonthly.Months.find { it.toString() == month.name })
					months << month.index
			}
			if(months?.size() == 12) {
				rtn = rtn + ' ' + '*'
			} else {
				rtn = rtn + ' ' + months.join(',')
			}
			rtn + ' ?'
		} else if(optionsPeriodically['@Enabled']== 'true') {
			//add continuously support
			def hour = optionsPeriodically.FullPeriod
			//build the string
			rtn = '0 0 ' + hour + ' * * ?'
		}
		return rtn
	}

}
