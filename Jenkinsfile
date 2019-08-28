import hudson.tasks.test.AbstractTestResultAction
import hudson.model.Actionable

properties([
    [$class: 'BuildDiscarderProperty', strategy: [$class: 'LogRotator', daysToKeepStr: '2', numToKeepStr: '2']]
])

def label = "worker-${UUID.randomUUID().toString()}"

def deployingBranches = [   // The branches mentioned here will get their artifacts deployed to Nexus
    "master", "axonserver-se-4.2.x"
]
def dockerBranches = [      // The branches mentioned here will get Docker test images built
    "master", "axonserver-se-4.2.x"
]
def sonarBranches = [       // The branches mentioned here will get a SonarQube analysis
    "master", "axonserver-se-4.2.x"
]

/*
 * Check if we want to do something extra.
 */
def relevantBranch(thisBranch, branches) {
    for (br in branches) {
        if (thisBranch == br) {
            return true;
        }
    }
    return false;
}

/*
 * Prepare a textual summary of the Unit tests, for sending to Slack
 */
@NonCPS
def getTestSummary = { ->
    def testResultAction = currentBuild.rawBuild.getAction(AbstractTestResultAction.class)
    def summary = ""
    if (testResultAction != null) {
        def total = testResultAction.getTotalCount()
        def failed = testResultAction.getFailCount()
        def skipped = testResultAction.getSkipCount()
        summary = "Test results: Passed: " + (total - failed - skipped) + (", Failed: " + failed) + (", Skipped: " + skipped)
    } else {
        summary = "No tests found"
    }
    return summary
}

/*
 * Using the Kubernetes plugin for Jenkins, we run everything in a Maven pod.
 */
podTemplate(label: label,
    containers: [
        containerTemplate(name: 'maven', image: 'eu.gcr.io/axoniq-devops/maven-axoniq:latest',
            command: 'cat', ttyEnabled: true,
            resourceRequestCpu: '1000m', resourceLimitCpu: '1000m',
            resourceRequestMemory: '3200Mi', resourceLimitMemory: '4Gi',
            envVars: [
                envVar(key: 'MAVEN_OPTS', value: '-Xmx3200m -Djavax.net.ssl.trustStore=/docker-java-home/lib/security/cacerts -Djavax.net.ssl.trustStorePassword=changeit'),
                envVar(key: 'MVN_BLD', value: '-B -s /maven_settings/settings.xml')
            ])
    ],
    volumes: [
        secretVolume(secretName: 'cacerts', mountPath: '/docker-java-home/lib/security'), // For our Nexus certificates
        secretVolume(secretName: 'maven-settings', mountPath: '/maven_settings')          // For the settings.xml
    ]) {
        node(label) {
            def myRepo = checkout scm
            def gitCommit = myRepo.GIT_COMMIT
            def gitBranch = myRepo.GIT_BRANCH
            def shortGitCommit = "${gitCommit[0..10]}"

            pom = readMavenPom file: 'pom.xml'
            def pomVersion = pom.version
            def pomGroupId = 'io.axoniq.axonserver'
            def pomArtifactId = 'axonserver'

            def slackReport = "Maven build for Axon Server ${pomVersion} (branch \"${gitBranch}\")."
            stage ('Maven build') {
                container("maven") {
                    try {
                        sh "mvn \${MVN_BLD} -Dmaven.test.failure.ignore clean verify"   // Ignore test failures; we want the numbers only.
                    }
                    catch (err) {
                        slackReport = slackReport + "\nMaven build FAILED!"             // This means build itself failed, not 'just' tests
                        throw err
                    }
                    finally {
                        junit '**/target/surefire-reports/TEST-*.xml'                   // Read the test results
                        slackReport = slackReport + "\n" + getTestSummary()
                    }
                    when(relevantBranch(gitBranch, deployingBranches)) {                // Deploy artifacts to Nexus for some branches
                        sh "mvn \${MVN_BLD} -DskipTests deploy"
                    }
                }
            }

            stage ('Run SonarQube') {
                when(relevantBranch(gitBranch, sonarBranches)) {                        // Run SonarQube analyses for some branches
                    container("maven") {
                        sh "mvn \${MVN_BLD} -DskipTests -Psonar sonar:sonar"
                    }
                }
            }

            stage('Trigger followup') {
                /*
                 * Run a subsidiary build to make Docker test images.
                 */
                when(relevantBranch(gitBranch, dockerBranches)) {
                    def dockerBuild = build job: 'axon-server-dockerimages/master', propagate: false, wait: true,
                        parameters: [
                            string(name: 'groupId', value: pomGroupId),
                            string(name: 'artifactId', value: pomArtifactId),
                            string(name: 'projectVersion', value: pomVersion)
                        ]
                    if (dockerBuild.result == "FAILURE") {
                        slackReport = slackReport + "\nBuild of Docker images FAILED!"
                    }
                    else {
                        slackReport = slackReport + "\nNew Docker images have been pushed."
                    }
                }

                /*
                 * If we have Docker images and artifacts in Nexus, we can run Canary tests on them.
                 */
                when(relevantBranch(gitBranch, dockerBranches) && relevantBranch(gitBranch, deployingBranches)) {
                    def canaryTests = build job: 'axon-server-canary/master', propagate: false, wait: true,
                        parameters: [
                            string(name: 'groupId', value: pomGroupId),
                            string(name: 'artifactId', value: pomArtifactId),
                            string(name: 'projectVersion', value: pomVersion)
                        ]
                    if (canaryTests.result == "FAILURE") {
                        slackReport = slackReport + "\nCanary Tests FAILED!"
                    }
                }
            }
            // Tell the team what happened.
            slackSend(message: slackReport)
        }
    }
