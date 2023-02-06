pipeline {
    agent {
        node {
            label 'docker'
        }
    }
    stages {
        stage ('clean') {
            steps {
                script {
                    sh 'rm -rf target/debian'
                }
            }
        }
        stage ('build-minerva-service') {
            agent {
                dockerfile {
                    filename 'packaging/Dockerfile'
                }
            }
            steps {
                sh "CARGO_HOME=${WORKSPACE} cargo deb -p minerva-service --target=x86_64-unknown-linux-musl"

                stash name: 'deb', includes: 'target/x86_64-unknown-linux-musl/debian/*.deb'
            }
        }
        stage ('build-minerva-admin') {
            agent {
                dockerfile {
                    filename 'packaging/Dockerfile'
                }
            }
            steps {
                sh "CARGO_HOME=${WORKSPACE} cargo deb -p minerva-admin --target=x86_64-unknown-linux-musl"

                stash name: 'deb', includes: 'target/x86_64-unknown-linux-musl/debian/*.deb'
            }
        }
        stage('publish-packages') {
            steps {
                unstash name: 'deb'
                script {
                    publishPackages 'target/x86_64-unknown-linux-musl/debian', 'minerva/any/stable'
                }
            }
        }
    }
}

