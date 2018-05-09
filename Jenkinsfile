#!/usr/bin/env groovy

pipeline {
    agent any

    environment {
        APPLICATION_NAME = 'kafka.adminclient'
        GIT_PROJECT = 'INT'
        FASIT_ENV = 'q4'
        ZONE = 'fss'
        NAMESPACE = 'default'
    }

    stages {
        stage('initialize') {
            steps {
                script {
                    sh './gradlew clean'
                    applicationVersionGradle = sh(script: './gradlew -q printVersion', returnStdout: true).trim()
                    gitVars = utils.gitVars(env.APPLICATION_NAME)
                    applicationVersion = "${applicationVersionGradle}.${env.BUILD_ID}-${gitVars.commitHashShort}"
                    applicationFullName = "${env.APPLICATION_NAME}:${applicationVersion}"
                    utils.slackBuildStarted(env.APPLICATION_NAME, gitVars.changeLog.toString())
                    utils.githubCommitStatus(env.APPLICATION_NAME, gitVars.commitHash, "pending", "Build started")
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
                    utils.slackBuildPassed(env.APPLICATION_NAME)
                }
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
                script {
                    utils.dockerCreatePushImage(applicationFullName, gitVars.commitHash)
                }
            }
        }
        stage('validate & upload nais.yaml to nexus m2internal') {
            steps {
                script {
                    deploy.naisUploadYaml(env.APPLICATION_NAME, applicationVersion)
                }
            }
        }
        stage('deploy to nais') {
            steps {
                script {
                    response = deploy.naisDeployJira(env.APPLICATION_NAME, applicationVersion, env.FASIT_ENV, env.NAMESPACE, env.ZONE)
                    def jiraIssueId = readJSON([text: response.content])["key"].toString()
                    currentBuild.description = "Waiting for <a href=\"https://jira.adeo.no/browse/$jiraIssueId\">$jiraIssueId</a>"
                    utils.slackBuildDeploying(env.APPLICATION_NAME, jiraIssueId)

                    try {
                        timeout(time: 1, unit: 'HOURS') {
                            input id: "deploy", message: "Waiting for remote Jenkins server to deploy the application..."
                        }
                        currentBuild.description = ""
                    } catch (Exception exception) {
                        currentBuild.description = "Deploy failed, see <a href=\"https://jira.adeo.no/browse/$jiraIssueId\">$jiraIssueId</a>"
                        throw exception
                    }
                }
            }
        }
    }
    post {
        always {
            junit '**/build/test-results/junit-platform/*.xml'
            archive '**/build/libs/*'
            archive '**/build/install/*'
            deleteDir()
            script {
                utils.dockerPruneBuilds()
                if (currentBuild.result == 'ABORTED') {
                    utils.slackBuildAborted(env.APPLICATION_NAME)
                }
            }
        }
        success {
            script {
                utils.slackBuildSuccess(env.APPLICATION_NAME)
                utils.githubCommitStatus(env.APPLICATION_NAME, gitVars.commitHash, "success", "Build success")
            }
        }
        failure {
            script {
                utils.slackBuildFailed(env.APPLICATION_NAME)
                utils.githubCommitStatus(env.APPLICATION_NAME, gitVars.commitHash, "failure", "Build failed")
            }
        }
    }
}
