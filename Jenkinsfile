import hudson.tasks.test.AbstractTestResultAction
import hudson.model.Actionable

properties([
    [$class: 'BuildDiscarderProperty', strategy: [$class: 'LogRotator', daysToKeepStr: '2', numToKeepStr: '2']]
])

def label = "worker-${UUID.randomUUID().toString()}"

def deployingBranches = [   // The branches mentioned here will get their artifacts deployed to Nexus
    "master", "axonserver-se-4.5.x"
]
def dockerBranches = [      // The branches mentioned here will get Docker images built
    "master", "axonserver-se-4.5.x", "feature/integration-tests"
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
        containerTemplate(name: 'maven-jdk8', image: 'eu.gcr.io/axoniq-devops/maven-axoniq:8', alwaysPullImage: true,
            command: 'cat', ttyEnabled: true,
            resourceRequestCpu: '1000m', resourceLimitCpu: '1000m',
            resourceRequestMemory: '3200Mi', resourceLimitMemory: '4Gi',
            envVars: [
                envVar(key: 'MAVEN_OPTS', value: '-Xmx3200m'),
                envVar(key: 'MVN_BLD', value: '-B -s /maven_settings/settings.xml')
            ]),
        containerTemplate(name: 'maven-jdk11', image: 'eu.gcr.io/axoniq-devops/maven-axoniq:11', alwaysPullImage: true,
            command: 'cat', ttyEnabled: true,
            envVars: [
                envVar(key: 'MVN_BLD', value: '-B -s /maven_settings/settings.xml')
            ]),
        containerTemplate(name: 'docker', image: 'eu.gcr.io/axoniq-devops/docker-axoniq:latest', alwaysPullImage: true,
            command: 'cat', ttyEnabled: true,
            envVars: [
                envVar(key: 'AXONIQ_HOME', value: '/axoniq')
            ])
    ],
    volumes: [
        hostPathVolume(mountPath: '/var/run/docker.sock', hostPath: '/var/run/docker.sock'),
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

            def tag = sh(returnStdout: true, script: "git tag --contains | head -1").trim()
            def onTag = sh(returnStdout: true, script: "git tag --points-at HEAD | head -1").trim()

            def releaseBuild = (!tag.isEmpty() && tag.equals(onTag))

            pom = readMavenPom file: 'pom.xml'
            def pomVersion = pom.version
            def pomGroupId = 'io.axoniq.axonserver'
            def pomArtifactId = 'axonserver'

            def slackReport = "Maven build for Axon Server SE ${pomVersion} (branch \"${gitBranch}\")."

            def mavenTarget = "clean verify"

            stage ('Maven build') {
                container("maven-jdk8") {
                    sh "mvn \${MVN_BLD} clean"
                }

                if (!releaseBuild) {
                    container("maven-jdk8") {
                        if (relevantBranch(gitBranch, deployingBranches)) {                // Deploy artifacts to Nexus for some branches
                            mavenTarget = "clean deploy"
                        }
                        mavenTarget = "-Pcoverage " + mavenTarget

                        try {
                            // Ignore test failures; we want the numbers only.
                            // Also skip the integration tests during this first run.
                            sh "mvn \${MVN_BLD} -Dmaven.test.failure.ignore -DskipITs ${mavenTarget}"

                            if (relevantBranch(gitBranch, deployingBranches)) {                // Deploy artifacts to Nexus for some branches
                                slackReport = slackReport + "\nDeployed to Nexus"
                             }
                        }
                        catch (err) {
                            slackReport = slackReport + "\nMaven build FAILED!"             // This means build itself failed, not 'just' tests
                            throw err
                        }
                        finally {
                            junit '**/target/surefire-reports/TEST-*.xml'                   // Read the test results
                        }
                    }
                }
                else { // Release build
                    try {
                        container("maven-jdk8") {
                            // We need compiled code for SonarQube
                            sh "mvn \${MVN_BLD} -Pcoverage -Dmaven.test.failure.ignore verify"
                        }
                    } catch (err) {
                        // Ignore
                    }
                }
            }

            stage ('Docker image builds') {
                if (releaseBuild || relevantBranch(gitBranch, dockerBranches)) {
                    try {
                        def baseTag = "eu.gcr.io/axoniq-devops/axonserver"
                        def images = "--temurin"
                        def tags11 = "--tag-jdk"
                        def tagsDef = "--tag-jdk"

                        if (releaseBuild) {
                            // For a release build, make all image variants
                            images = images + " --prod --dev --nonroot"

                            // For JDK 11, apart from ":<version>-jdk-11", also tag this build as ":latest", ":latest-jdk-11" and ":<version>"
                            tags11 = tags11 + " --tag-latest --tag-latest-jdk --tag-version"
                            // For other JDKs, apart from ":<version>-jdk-<jdk>", also tag this build as ":latest-jdk-<jdk>"
                            tagsDef = tagsDef + " --no-tag-latest --tag-latest-jdk --no-tag-version"
                        } else {
                            // For SNAPSHOT builds, only make "dev" and "nonroot" variants
                            images = images + " --no-prod --dev --nonroot"

                            // For JDK 11, apart from ":<version>-jdk-11", also tag this build as ":<version>"
                            tags11 = tags11 + " --no-tag-latest --no-tag-latest-jdk --tag-version"
                            // For other JDKs, apart from ":<version>-jdk-<jdk>", don't add additional tags
                            tagsDef = tagsDef + " --no-tag-latest --no-tag-latest-jdk --no-tag-version"
                        }

                        container("docker") {
                            sh """
                                docker-credential-gcr config --token-source=env,store
                                docker-credential-gcr configure-docker

                                bin/build-docker-release.sh ${images} ${tagsDef} --jdk 8  --repo ${baseTag} --push ${pomVersion}
                                bin/build-docker-release.sh ${images} ${tags11}  --jdk 11 --repo ${baseTag} --push ${pomVersion}
                                bin/build-docker-release.sh ${images} ${tagsDef} --jdk 17 --repo ${baseTag} --push ${pomVersion}
                            """
                        }
                        slackReport = slackReport + "\nNew Docker images pushed."
                    } catch (err) {
                        slackReport = slackReport + "\nFAILED to push Docker images."
                    }
                }
            }

            stage ('Integration Tests') {
                container("maven-jdk8") {
                    try {
                        // Again, ignore test failures, but this time, skip the Unit tests.
                        sh "mvn \${MVN_BLD} -Dmaven.test.failure.ignore -DskipUTs -Pcoverage clean verify"
                    }
                    catch (err) {
                        slackReport = slackReport + "\nIntegration tests FAILED!"
                        throw err
                    }
                    finally {
                        junit '**/target/failsafe-reports/TEST-*.xml'                   // Read the test results
                        slackReport = slackReport + "\n" + getTestSummary()
                    }
                }
            }

            stage ('Run SonarQube') {
                def sonarOptions = "-Dsonar.branch.name=${gitBranch}"
                if (gitBranch.startsWith("PR-") && env.CHANGE_ID) {
                    sonarOptions = "-Dsonar.pullrequest.branch=" + gitBranch + " -Dsonar.pullrequest.key=" + env.CHANGE_ID
                }
                container("maven-jdk11") {
                    sh "mvn \${MVN_BLD} -DskipTests ${sonarOptions}  -Psonar sonar:sonar"
                    slackReport = slackReport + "\nSources analyzed in SonarQube."
                }
            }

            stage('Trigger followup') {
                if (!releaseBuild && relevantBranch(gitBranch, dockerBranches) && relevantBranch(gitBranch, deployingBranches)) {
                    def canaryTests = build job: 'axon-server-canary/master', propagate: false, wait: true,
                        parameters: [
                            string(name: 'serverEdition', value: 'se'),
                            string(name: 'projectVersion', value: pomVersion),
                            string(name: 'cliVersion', value: pomVersion)
                        ]
                    if (canaryTests.result == "FAILURE") {
                        slackReport = slackReport + "\nCanary Tests FAILED!"
                    }
                }
            }
            slackSend(message: slackReport)
        }
    }
