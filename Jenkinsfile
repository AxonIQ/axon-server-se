import hudson.tasks.test.AbstractTestResultAction
import hudson.model.Actionable

properties([
    [$class: 'BuildDiscarderProperty', strategy: [$class: 'LogRotator', daysToKeepStr: '2', numToKeepStr: '2']],
    parameters([
        string(name: 'namespace', defaultValue: 'devops')
    ])
])

def label = "worker-${UUID.randomUUID().toString()}"

def deployingBranches = [
    "master", "axonserver-4.0.x"
]
def dockerBranches = [
    "master", "axonserver-4.0.x"
]
def sonarBranches = [
    "master", "axonserver-4.0.x"
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
            ])
    ],
    volumes: [
        hostPathVolume(mountPath: '/var/run/docker.sock', hostPath: '/var/run/docker.sock'),
        secretVolume(secretName: 'cacerts', mountPath: '/docker-java-home/lib/security'),
        secretVolume(secretName: 'dockercfg', mountPath: '/dockercfg'),
        secretVolume(secretName: 'jenkins-nexus', mountPath: '/nexus_settings'),
        secretVolume(secretName: 'maven-settings', mountPath: '/maven_settings')
    ]) {
        node(label) {
            def myRepo = checkout scm
            def gitCommit = myRepo.GIT_COMMIT
            def gitBranch = myRepo.GIT_BRANCH
            def shortGitCommit = "${gitCommit[0..10]}"
            def previousGitCommit = sh(script: "git rev-parse ${gitCommit}~", returnStdout: true)
            pom = readMavenPom file: 'pom.xml'
            def pomVersion = pom.version

            slackSend(message: "Build Started for Axon Server, branch ${gitBranch} (<${env.BUILD_URL}|Open>)")

            stage ('Project setup') {
                container("maven") {
                    sh """
                        cat /maven_settings/*xml >./settings.xml
                        export AXONIQ_BRANCH=${gitBranch}
                        export AXONIQ_NS=${params.namespace}
                        axoniq-templater -s ./settings.xml -P docker -pom pom.xml -mod axonserver -env AXONIQ -envDot -q -dump >jenkins-build.properties
                    """
                }
            }

            def props = readProperties file: 'jenkins-build.properties'
            def gcloudRegistry = props ['gcloud.registry']
            def gcloudProjectName = props ['gcloud.project.name']

            stage ('Maven build') {
                container("maven") {
                    try {
                        sh "mvn \${MVN_BLD} -Dmaven.test.failure.ignore clean package"
                    }
                    catch (err) {
                        slackSend(message: "Maven build for Axon Server ${pomVersion} FAILED!")
                        throw err
                    }
                    finally {
                        junit '**/target/surefire-reports/TEST-*.xml'
                        slackSend(message: getSummary())
                    }

                    when(relevantBranch(gitBranch, deployingBranches)) {
                        sh "mvn \${MVN_BLD} -DskipTests deploy"
                        slackSend(message: "New artifacts have been deployed in Nexus for Axon Server ${pomVersion}.")
                    }
                }
            }

            stage ('Run SonarQube') {
                when(relevantBranch(gitBranch, sonarBranches)) {
                    container("maven") {
                        sh "mvn \${MVN_BLD} -DskipTests -Psonar sonar:sonar"
                        slackSend(message: "New SonarQube analysis is avialable for Axon Server ${pomVersion}.")
                    }
                }
            }

            stage('Trigger followup') {

                when(relevantBranch(gitBranch, dockerBranches)) {
// Axon Server - Build Docker Images
//        string(name: 'namespace', defaultValue: 'devops'),
//        string(name: 'groupId', defaultValue: 'io.axoniq.axonserver'),
//        string(name: 'artifactId', defaultValue: 'axonserver'),
//        string(name: 'projectVersion', defaultValue: '4.0-M3-SNAPSHOT')
                    def dockerBuild = build job: 'axon-server-dockerimages/master', propagate: false, wait: true,
                        parameters: [
                            string(name: 'namespace', value: params.namespace),
                            string(name: 'groupId', value: props ['project.groupId']),
                            string(name: 'artifactId', value: props ['project.artifactId']),
                            string(name: 'projectVersion', value: props ['project.version'])
                        ]
                    if (dockerBuild.result == "FAILURE") {
                        slackSend(message: "Build of Axon Server ${pomVersion} Docker images FAILED!")
                    }
                    else {
                        slackSend(message: "New Docker images have been pushed for Axon Server ${pomVersion}.")
                    }
                }

                when(relevantBranch(gitBranch, dockerBranches) && relevantBranch(gitBranch, deployingBranches)) {
// Axon Server - Canary tests
//        string(name: 'namespace', defaultValue: 'axon-server-canary'),
//        string(name: 'imageName', defaultValue: 'axonserver'),
//        string(name: 'serverName', defaultValue: 'axon-server'),
//        string(name: 'groupId', defaultValue: 'io.axoniq.axonserver'),
//        string(name: 'artifactId', defaultValue: 'axonserver'),
//        string(name: 'projectVersion', defaultValue: '4.0-M3-SNAPSHOT')

                    def canaryTests = build job: 'axon-server-canary/master', propagate: false, wait: false,
                        parameters: [
                            string(name: 'namespace', value: props ['project.artifactId'] + '-canary'),
                            string(name: 'imageName', value: 'axonserver'),
                            string(name: 'serverName', value: 'axon-server'),
                            string(name: 'groupId', value: props ['project.groupId']),
                            string(name: 'artifactId', value: props ['project.artifactId']),
                            string(name: 'projectVersion', value: props ['project.version'])
                        ]
                    if (canaryTests.result == "FAILURE") {
                        slackSend(message: "Build of Axon Server ${pomVersion} FAILED Canary Testing!")
                    }
                }
            }
        }
    }
