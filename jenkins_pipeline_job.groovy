import de.gebit.build.jenkins.PipelineJobBuilder

def builder = new PipelineJobBuilder(binding) {

	/**
	 * You can use a custom pipeline definition, here 'Jenkinsfile-gebit'.
	 */
	@Override
	protected String getPipelineScriptPath(String jobType, String scriptBranch) {
		if (jobType == "release-changelist-build") {
			return "Jenkinsfile-gebit"
		}
		return super.getPipelineScriptPath(jobType, scriptBranch)
	}
	 
	/**
	 * Enable your custom job (here e.g. only for the master branch) and disable all other jobs if not needed.
	 */
	@Override
	def isCreatingJobFor(String branchType, String jobType) {
		if (jobType == 'release-changelist-build') {
			return true
		}
		return false
	}
}

return builder
