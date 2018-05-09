#!/usr/bin/env groovy

pipeline {
    agent { label 'erbeskær1' }

    environment {
        APPLICATION_NAME = 'kafka.admin.client'
        FASIT_ENV = 'q4'
    }

    stages {
        stage('prepare') { steps { ciSkip action: 'check' } }
        stage('initialize') {
            steps {
                script {
                    gitVars = utils.gitVars(env.APPLICATION_NAME)
                    utils.slackBuildStarted(env.APPLICATION_NAME, gitVars.changeLog.toString())
                    utils.githubCommitStatus(env.APPLICATION_NAME, gitVars.commitHash, "pending", "Build started")
                }
            }
        }
        stage('build') {
            steps {
                script {
                    sh './gradlew clean'
                    applicationVersionGradle = sh(script: './gradlew -q printVersion', returnStdout: true).trim()
                    gitCommitHashShort = sh(script: 'git rev-parse --short HEAD', returnStdout: true).trim()
                    applicationVersion = "${applicationVersionGradle}"
                    //applicationVersion = "${applicationVersionGradle}.${env.BUILD_ID}-${gitCommitHashShort}"
                    sh './gradlew build -x test'
                }
            }
        }
        stage('run tests (unit & intergration)') {
            steps {
                script {
                    sh './gradlew test'
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
        stage('deploy') {
            parallel {
                stage('deploy bankkontonummer') {
                    steps {
                        script {
                            def application = [
                                    name       : 'kafka2jms-bankkontonr',
                                    credential : 'srvkafka2jms-bankkontonr',
                                    properties : 'bankkontonr_kafka_event',
                                    channel    : 'kafka2jms-bankkontonr_channel',
                                    outputQueue: 'eia_queue_mottak_online_inbound'
                            ]

                            applicationFullname = "${application.name}:${applicationVersion}"
                            utils.dockerCreatePushImage(applicationFullname, gitCommitHashShort)
                            utils.kafka2jmsBuildGeneric(application, applicationVersion, env.FASIT_ENV)
                        }
                    }
                }
                stage('deploy oppfolgingsplan') {
                    steps {
                        script {
                            def application = [
                                    name       : 'kafka2jms-oppfolgingsplan',
                                    credential : 'srvkafka2jms-oppfolgingsplan',
                                    properties : 'oppfolgingsplan_kafka_event',
                                    channel    : 'kafka2jms-oppfolgingsplan_channel',
                                    outputQueue: 'eia_queue_mottak_inbound'
                            ]
                            applicationFullname = "${application.name}:${applicationVersion}"
                            utils.dockerCreatePushImage(applicationFullname, gitCommitHashShort)
                            utils.kafka2jmsBuildGeneric(application, applicationVersion, env.FASIT_ENV)
                        }
                    }
                }
                stage('deploy maalekort') {
                    steps {
                        script {
                            def application = [
                                    name       : 'kafka2jms-maalekort',
                                    credential : 'srvkafka2jms-maalekort',
                                    properties : 'maalekort_kafka_event',
                                    channel    : 'kafka2jms-maalekort_channel',
                                    outputQueue: 'mottak_queue_maalekort_meldinger'
                            ]
                            applicationFullname = "${application.name}:${applicationVersion}"
                            utils.dockerCreatePushImage(applicationFullname, gitCommitHashShort)
                            utils.kafka2jmsBuildGeneric(application, applicationVersion, env.FASIT_ENV)
                        }
                    }
                }
                stage('deploy barnehageliste') {
                    steps {
                        script {
                            def application = [
                                    name       : 'kafka2jms-barnehageliste',
                                    credential : 'srvkafka2jms-barnehageliste',
                                    properties : 'barnehageliste_kafka_event',
                                    channel    : 'kafka2jms-barnehageliste_channel',
                                    outputQueue: 'BARNEHAGELISTER_FRA_ALTINN'
                            ]
                            applicationFullname = "${application.name}:${applicationVersion}"
                            utils.dockerCreatePushImage(applicationFullname, gitCommitHashShort)
                            utils.kafka2jmsBuildGeneric(application, applicationVersion, env.FASIT_ENV)
                        }
                    }
                }
                stage('deploy inntektsskjema') {
                    steps {
                        script {
                            def application = [
                                    name       : 'kafka2jms-inntektsskjema',
                                    credential : 'srvkafka2jms-inntektsskjema',
                                    properties : 'inntektsskjema_kafka_event',
                                    channel    : 'kafka2jms-inntektsskjema_channel',
                                    outputQueue: 'dokmot_ALTINNMOTTAK'
                            ]
                            applicationFullname = "${application.name}:${applicationVersion}"
                            utils.dockerCreatePushImage(applicationFullname, gitCommitHashShort)
                            utils.kafka2jmsBuildGeneric(application, applicationVersion, env.FASIT_ENV)
                        }
                    }
                }
            }
        }
    }
    post {
        always {
            ciSkip action: 'postProcess'
            deleteDir()
            script {
                utils.dockerPruneBuilds()
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