#!/usr/bin/env groovy

pipeline {
    agent any

    environment {
        APPLICATION_NAME = 'kafka-adminrest'
        APPLICATION_SERVICE = 'CMDB-274891'
        APPLICATION_COMPONENT = 'CMDB-247049'
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
        stage('generate executable FAT jar') {
            steps {
                sh './gradlew shadowJar'
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
                        deploy action: 'jiraPreprod', environment: 't4', namespace: 't4'
                    }
                }

                stage('deploy to preprod') {
                    steps {
                        deploy action: 'jiraPreprod', environment: 'q4', namespace: 'q4'
                    }
                }

                stage('deploy to production') {
                    when { environment name: 'DEPLOY_TO', value: 'production' }
                    steps {
                        deploy action: 'jiraProd'
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
