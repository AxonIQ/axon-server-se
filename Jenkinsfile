properties([
    [$class: 'BuildDiscarderProperty', strategy: [$class: 'LogRotator', daysToKeepStr: '2', numToKeepStr: '2']],
    parameters([
        string(name: 'namespace', defaultValue: 'devops')
    ])
])

def label = "worker-${UUID.randomUUID().toString()}"

podTemplate(label: label,
    containers: [
        containerTemplate(name: 'maven', image: 'eu.gcr.io/axoniq-devops/maven:3.5.4-jdk-8',
           command: 'cat', ttyEnabled: true,
            envVars: [
                envVar(key: 'MAVEN_OPTS', value: '-Xmx3200m -Djavax.net.ssl.trustStore=/docker-java-home/lib/security/cacerts -Djavax.net.ssl.trustStorePassword=changeit'),
                envVar(key: 'MVN_BLD', value: '-B -s /maven_settings/settings.xml')
            ]),
        containerTemplate(name: 'docker', image: 'docker',
            command: 'cat', ttyEnabled: true)
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

            stage ('Project setup') {
                container("maven") {
                    sh """
                        sh ./getLastFromNexus.sh io.axoniq.axoniq-templater axoniq-templater
                        cat /maven_settings/*xml >./settings.xml
                        export AXONIQ_BRANCH=${gitBranch}
                        export AXONIQ_NS=${params.namespace}
                        ./axoniq-templater -s ./settings.xml -P docker -pom pom.xml -mod axonserver -env AXONIQ -envDot -q -dump >jenkins-build.properties
                    """
                }
            }

            def props = readProperties file: 'jenkins-build.properties'
            def gcloudRegistry = props ['gcloud.registry']
            def gcloudProjectName = props ['gcloud.project.name']

            stage ('Maven build') {
                container("maven") {
                    sh "mvn \${MVN_BLD} -Dmaven.test.failure.ignore -Ddockerfile.push.skip -Ddockerfile.build.skip -Pdocker clean package"
                    junit '**/target/surefire-reports/TEST-*.xml'
                    sh "mvn \${MVN_BLD} -DskipTests deploy"
                }
            }

            stage('Docker build') {
                container('docker') {
                    sh """
                        cat /dockercfg/system-account.json | docker login -u _json_key --password-stdin https://eu.gcr.io
                        docker push ${gcloudRegistry}/${gcloudProjectName}/axonserver:${pomVersion}
                        docker push ${gcloudRegistry}/${gcloudProjectName}/axonserver-enterprise:${pomVersion}
                    """
                }
            }

            stage ('Run SonarQube') {
                when(gitBranch == 'master') {
                    container("maven") {
                        sh "mvn \${MVN_BLD} -DskipTests -Psonar sonar:sonar"
                    }
                }
            }

            stage('Trigger followup') {

// Axon Server - Build Docker Images
//        string(name: 'namespace', defaultValue: 'devops'),
//        string(name: 'groupId', defaultValue: 'io.axoniq.axonserver'),
//        string(name: 'artifactId', defaultValue: 'axonserver'),
//        string(name: 'projectVersion', defaultValue: '4.0-M3-SNAPSHOT')
                build job: 'axon-server-dockerimages/master', propagate: false, wait: true,
                    parameters: [
                        string(name: 'namespace', value: params.namespace),
                        string(name: 'groupId', value: props ['project.groupId']),
                        string(name: 'artifactId', value: props ['project.artifactId']),
                        string(name: 'projectVersion', value: props ['project.version'])
                    ]

// Axon Server - Canary tests
//        string(name: 'namespace', defaultValue: 'axon-server-canary'),
//        string(name: 'imageName', defaultValue: 'axonserver'),
//        string(name: 'serverName', defaultValue: 'axon-server'),
//        string(name: 'groupId', defaultValue: 'io.axoniq.axonserver'),
//        string(name: 'artifactId', defaultValue: 'axonserver'),
//        string(name: 'projectVersion', defaultValue: '4.0-M3-SNAPSHOT')
                build job: 'axon-server-canary/master', propagate: false, wait: false,
                    parameters: [
                        string(name: 'namespace', value: props ['project.artifactId'] + '-canary'),
                        string(name: 'imageName', defaultValue: 'axonserver'),
                        string(name: 'serverName', defaultValue: 'axon-server'),
                        string(name: 'groupId', value: props ['project.groupId']),
                        string(name: 'artifactId', value: props ['project.artifactId']),
                        string(name: 'projectVersion', value: props ['project.version'])
                    ]
            }
        }
    }
