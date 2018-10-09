#!/usr/bin/env groovy

pipeline {
    agent any

    environment {
        APPLICATION_NAME = 'kafka-adminrest'
        ZONE = 'fss'
        DOCKER_SLUG = 'integrasjon'
    }

    stages {
        stage('initialize') {
            steps {
                init action: 'gradle'
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
        stage('generate distribution files') {
            steps {
                sh './gradlew installDist'
            }
        }
        stage('push docker image') {
            steps {
                dockerUtils action: 'createPushImage'
            }
        }
        stage('validate & upload nais.yaml to nexus m2internal') {
            steps {
                nais action: 'validate'
                nais action: 'upload'
            }
        }
        stage('deploy') {
            parallel {
                stage('deploy to test') {
                    steps {
                        deployApp action: 'jiraPreprod', environment: 't4', namespace: 't4'
                    }
                }

                stage('deploy to preprod') {
                    steps {
                        deployApp action: 'jiraPreprod', environment: 'q4', namespace: 'q4'
                    }
                }

                stage('deploy to production') {
                    when { environment name: 'DEPLOY_TO', value: 'production' }
                    steps {
                        deployApp action: 'jiraProd'
                        githubStatus action: 'tagRelease'
                    }
                }
            }
        }
    }
    post {
        always {
            postProcess action: 'always'
            junit '**/build/test-results/test/*.xml'
            archiveArtifacts artifacts: '**/build/libs/*', allowEmptyArchive: true
        }
        success {
            postProcess action: 'success'
        }
        failure {
            postProcess action: 'failure'
        }
    }
}
