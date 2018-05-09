#!/usr/bin/env groovy

pipeline {
    agent any

    environment {
        APPLICATION_NAME = 'kafka-adminrest'
        FASIT_ENV = 'q4'
        ZONE = 'fss'
        NAMESPACE = 'default'
    }

    stages {
        stage('prepare') {
            steps {
                ciSkip 'check'
            }
        }
        stage('initialize') {
            steps {
                script {
                    sh './gradlew clean'
                    applicationVersionGradle = sh(script: './gradlew -q printVersion', returnStdout: true).trim()
                    env.COMMIT_HASH = gitVars 'commitHash'
                    env.COMMIT_HASH_SHORT = gitVars 'commitHashShort'
                    env.APPLICATION_VERSION = "${applicationVersionGradle}"
                    if (applicationVersionGradle.endsWith('-SNAPSHOT')) {
                        env.APPLICATION_VERSION = "${applicationVersionGradle}.${env.BUILD_ID}-${env.COMMIT_HASH_SHORT}"
                    }
                    changeLog = utils.gitVars(env.APPLICATION_NAME).changeLog
                    githubStatus 'pending'
                    slackStatus status: 'started', changeLog: "${changeLog.toString()}"
                }
            }
        }
        stage('build') {
            steps {
                script {
                    sh './gradlew build -x test'
                }
            }
        }
        stage('run tests (unit & intergration)') {
            steps {
                script {
                    sh './gradlew test'
                }
                slackStatus status: 'passed'
            }
        }
        stage('generate executable FAT jar') {
            steps {
                script {
                    sh './gradlew shadowJar'
                }
            }
        }
        stage('push docker image') {
            steps {
                dockerUtils 'createPushImage'
            }
        }
        stage('validate & upload nais.yaml to nexus m2internal') {
            steps {
                nais 'validate'
                nais 'upload'
            }
        }
        stage('deploy to nais') {
            steps {
                script {
                    def jiraIssueId = nais 'jiraDeploy'
                    slackStatus status: 'deploying', jiraIssueId: "${jiraIssueId}"
                    try {
                        timeout(time: 1, unit: 'HOURS') {
                            input id: "deploy", message: "Waiting for remote Jenkins server to deploy the application..."
                        }
                    } catch (Exception exception) {
                        currentBuild.description = "Deploy failed, see " + currentBuild.description
                        throw exception
                    }
                }
            }
        }
    }
    post {
        always {
            ciSkip 'postProcess'
            dockerUtils 'pruneBuilds'
            script {
                if (currentBuild.result == 'ABORTED') {
                    slackStatus status: 'aborted'
                }
            }
        }
        success {
            junit '**/build/test-results/junit-platform/*.xml'
            archive '**/build/libs/*'
            archive '**/build/install/*'
            deleteDir()
            githubStatus 'success'
            slackStatus status: 'success'
        }
        failure {
            githubStatus 'failure'
            slackStatus status: 'failure'
            deleteDir()
        }
    }
}
