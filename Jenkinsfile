import hudson.tasks.test.AbstractTestResultAction
import hudson.model.Actionable
properties([
    [$class: 'BuildDiscarderProperty', strategy: [$class: 'LogRotator', numToKeepStr: '5']]
])
def label = "worker-${UUID.randomUUID().toString()}"
def deployingBranches = [
    "master", "axonserver-ee-4.2.x"
]
def dockerBranches = [
    "master", "axonserver-ee-4.2.x"
]
def sonarBranches = [
    "master", "axonserver-ee-4.2.x"
]
def relevantBranch(thisBranch, branches) {
    for (br in branches) {
        if (thisBranch == br) {
            return true;
        }
    }
    return false;
}
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
podTemplate(label: label,
    containers: [
        containerTemplate(name: 'maven', image: 'eu.gcr.io/axoniq-devops/maven-axoniq:latest',
            command: 'cat', ttyEnabled: true,
            resourceRequestCpu: '1000m', resourceLimitCpu: '1000m',
            resourceRequestMemory: '3200Mi', resourceLimitMemory: '4Gi',
            envVars: [
                envVar(key: 'MAVEN_OPTS', value: '-Xmx3200m -Djavax.net.ssl.trustStore=/docker-java-home/lib/security/cacerts -Djavax.net.ssl.trustStorePassword=changeit'),
                envVar(key: 'MVN_BLD', value: '-B -s /maven_settings/settings.xml')
            ]),
        containerTemplate(name: 'gcloud', image: 'eu.gcr.io/axoniq-devops/gcloud-axoniq:latest', alwaysPullImage: true,
            command: 'cat', ttyEnabled: true)
    ],
    volumes: [
        hostPathVolume(mountPath: '/var/run/docker.sock', hostPath: '/var/run/docker.sock'),
        secretVolume(secretName: 'cacerts', mountPath: '/docker-java-home/lib/security'),
        secretVolume(secretName: 'dockercfg', mountPath: '/dockercfg'),
        secretVolume(secretName: 'jenkins-nexus', mountPath: '/nexus_settings'),
        secretVolume(secretName: 'test-settings', mountPath: '/axoniq'),
        secretVolume(secretName: 'maven-settings', mountPath: '/maven_settings')
    ]) {
        node(label) {
            def myRepo = checkout scm
            def gitCommit = myRepo.GIT_COMMIT
            def gitBranch = myRepo.GIT_BRANCH
            def shortGitCommit = "${gitCommit[0..10]}"
            def previousGitCommit = sh(script: "git rev-parse ${gitCommit}~", returnStdout: true)
            def tag = sh(returnStdout: true, script: "git tag --contains | head -1").trim()
            def onTag = sh(returnStdout: true, script: "git tag --points-at | head -1").trim()

            def testRun = gitBranch.equals("feature/release-builds")
            def releaseBuild = (!tag.isEmpty() && tag.equals(onTag)) || testRun

            pom = readMavenPom file: 'pom.xml'
            def pomVersion = pom.version

            stage ('Project setup') {
                container("maven") {
                    sh """
                        cat /maven_settings/*xml >./settings.xml
                        export AXONIQ_BRANCH=${gitBranch}
                        axoniq-templater -s ./settings.xml -P docker -pom pom.xml -mod axonserver-enterprise -env AXONIQ -envDot -q -dump >jenkins-build.properties
                    """
                }
            }
            def props = readProperties file: 'jenkins-build.properties'
            def gcloudRegistry = props ['gcloud.registry']
            def gcloudProjectName = props ['gcloud.project.name']
            def seVersion = props ['axonserver.se.version']

            def slackReport = "Maven build for Axon Server EE ${pomVersion} using SE version ${seVersion} (branch \"${gitBranch}\")."

            def mavenTarget = "clean verify"

            stage ('Maven build') {
                if (!releaseBuild) {
                    container("maven") {
                        if (relevantBranch(gitBranch, deployingBranches)) {                // Deploy artifacts to Nexus for some branches
                            mavenTarget = "clean deploy"
                        }
                        if (relevantBranch(gitBranch, dockerBranches)) {
                            mavenTarget = "-Pdocker " + mavenTarget
                        }
                        try {
                            sh "mvn \${MVN_BLD} -Dmaven.test.failure.ignore ${mavenTarget}"

                            if (relevantBranch(gitBranch, deployingBranches)) {                // Deploy artifacts to Nexus for some branches
                                slackReport = slackReport + "\nDeployed to dev-nexus"
                            }
                            if (relevantBranch(gitBranch, dockerBranches) || releaseBuild) {
                                slackReport = slackReport + "\nNew Docker images have been pushed"
                            }
                        }
                        catch (err) {
                            slackReport = slackReport + "\nMaven build FAILED!"
                            throw err
                        }
                        finally {
                            junit '**/target/surefire-reports/TEST-*.xml'
                            slackReport = slackReport + "\n" + getTestSummary()
                        }
                    }
                }
            }

            def testSettings = readProperties file: '/axoniq/test-settings.properties'

            def gceZone = testSettings ['io.axoniq.axonserver.infrastructure.gce.zone']
            def imgFamily = "axonserver-enterprise"
            def imgVersion = pomVersion.toLowerCase().replace('.', '-') + (testRun ? "-test" : "")

            stage ('VM image build') {
                if (releaseBuild) {
                    container("gcloud") {
                        sh "bin/build-image.sh --project ${gcloudProjectName} --zone ${gceZone} --img-family ${imgFamily} --img-version ${imgVersion} --cli-version ${seVersion} ${pomVersion}"
                    }
                    slackReport = slackReport + "\nNew VM image named \"${imgFamily}-${imgVersion}\" published in \"${gcloudProjectName}\"."
                }
            }

            stage ('Run SonarQube') {
                if (!releaseBuild && relevantBranch(gitBranch, sonarBranches)) {
                    container("maven") {
                        sh "mvn \${MVN_BLD} -DskipTests -Psonar sonar:sonar"
                    }
                    slackReport = slackReport + "\nSources analyzed in SonarQube."
                }
            }

            stage('Trigger followup') {
                if (!releaseBuild && relevantBranch(gitBranch, dockerBranches) && relevantBranch(gitBranch, deployingBranches)) {
                    def canaryTests = build job: 'axon-server-canary/master', propagate: false, wait: true,
                        parameters: [
                            string(name: 'serverEdition', value: 'ee'),
                            string(name: 'projectVersion', value: pomVersion),
                            string(name: 'cliVersion', value: seVersion)
                        ]
                    if (canaryTests.result == "FAILURE") {
                        slackReport = slackReport + "\nCanary Tests FAILED!"
                    }
                }
            }
            slackSend(message: slackReport)
        }
    }
