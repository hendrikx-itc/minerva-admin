pipeline {
    agent {
        node {
            label 'docker'
        }
    }
    stages {
        stage ('build-minerva-service') {
            agent {
                dockerfile {
                    filename 'packaging/Dockerfile'
                }
            }
            steps {
                sh "CARGO_HOME=${WORKSPACE} cargo deb -p minerva-service"

                stash name: 'deb', includes: 'target/debian/*.deb'
            }
        }
        stage('publish-minerva-service') {
            steps {
                unstash name: 'deb'
                script {
                    publishPackages 'target/debian', 'kpn/focal/stable', 'focal'
                }
            }
        }
        stage ('build-minerva-admin') {
            agent {
                dockerfile {
                    filename 'packaging/Dockerfile'
                }
            }
            steps {
                sh "CARGO_HOME=${WORKSPACE} cargo deb -p minerva-admin"

                stash name: 'deb', includes: 'target/debian/*.deb'
            }
        }
        stage('publish-minerva-admin') {
            steps {
                unstash name: 'deb'
                script {
                    publishPackages 'target/debian', 'kpn/focal/stable', 'focal'
                }
            }
        }
    }
}

