#!/usr/bin/env groovy

pipeline {
    agent any

    environment {
        APPLICATION_NAME = 'kafka-adminrest'
        FASIT_ENV = 't4'
        ZONE = 'fss'
        NAMESPACE = 'default'
        COMMIT_HASH_SHORT = gitVars 'commitHashShort'
        COMMIT_HASH = gitVars 'commitHash'
    }

    stages {
        stage('initialize') {
            steps {
                ciSkip 'check'
                sh './gradlew clean'
                script {
                    applicationVersionGradle = sh(script: './gradlew -q printVersion', returnStdout: true).trim()
                    env.APPLICATION_VERSION = "${applicationVersionGradle}"
                    if (applicationVersionGradle.endsWith('-SNAPSHOT')) {
                        env.APPLICATION_VERSION = "${applicationVersionGradle}.${env.BUILD_ID}-${env.COMMIT_HASH_SHORT}"
                    }
                    changeLog = utils.gitVars(env.APPLICATION_NAME).changeLog.toString()
                    githubStatus 'pending'
                    slackStatus status: 'started', changeLog: "${changeLog}"
                }
            }
        }
        stage('build') {
            steps {
                sh './gradlew build -x test'
            }
        }
        stage('run tests (unit & intergration)') {
            steps {
                sh './gradlew test'
                slackStatus status: 'passed'
            }
        }
        stage('generate executable FAT jar') {
            steps {
                sh './gradlew shadowJar'
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

        stage('deploy to test') {
            environment { FASIT_ENV = 't4' }
            steps {
                deployApplication()
            }
        }

        stage('deploy to preprod') {
            environment { FASIT_ENV = 'q4' }
            steps {
                deployApplication()
            }
        }

        /*stage('deploy to nais') {
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
        }*/


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
            //junit '**/build/test-results/junit-platform/*.xml'
            archive '**/build/libs/*'
            //archive '**/build/install/*'
            githubStatus 'success'
            slackStatus status: 'success'
            deleteDir()
        }
        failure {
            githubStatus 'failure'
            slackStatus status: 'failure'
            deleteDir()
        }
    }
}


void deployApplication() {
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