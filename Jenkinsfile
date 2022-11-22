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
                dir('dispatcher') {
                    sh "CARGO_HOME=${WORKSPACE} cargo deb"
                }
                stash name: 'deb', includes: 'target/debian/*.deb'
            }
        }
        stage('publish-minerva-service') {
            steps {
                unstash name: 'deb'
                script {
                    publishPackages 'target/debian', 'kpn/bionic/stable', 'bionic'
                }
            }
        }
    }
}

