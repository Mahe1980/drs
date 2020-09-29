def DockerImagePython = 'neuron-python36:v1.0.2'
def PytestJUnitFilePath = 'tests/target/junit/coverage.xml'
def PytestCovFilePath = 'tests/target/pytest-cov/coverage.xml'
def mavenSettingsFileId = 'ra-mvn-settings'
def maven_dockerImage = 'red_agent_base_os_no_agent:v.0.1.0'
def nexus_host = 'http://nexus.neuron.shared-services.vf.local/nexus/repository/pypi-proxy/'
def proxy_url = 'outbound-proxy.neuron.shared-services.vf.local:3128'
def classifier = (env.BRANCH_NAME == 'develop') ? "" : "-SNAPSHOT"
def changelist = (env.BRANCH_NAME == 'develop') ? "${BUILD_NUMBER}" : "${CHANGE_ID}"


pipeline {
  agent {
    kubernetes {
      yaml """
        apiVersion: v1
        kind: Pod
        spec:
          containers:
          - name: python3-container
            image: eu.gcr.io/${SS_PROJECT}/${DockerImagePython}
            resources:
              limits:
                cpu: "2"
                memory: "16Gi"
              requests:
                cpu: "0.5"
                memory: "2Gi"
            command:
            - cat
            tty: true
          - name: mvn-container
            image: eu.gcr.io/${SS_PROJECT}/${maven_dockerImage}
            resources:
              limits:
                cpu: "2"
                memory: "5Gi"
              requests:
                cpu: "2"
                memory: "5Gi"
            command:
            - cat
            tty: true
        """
        }
  }


    stages {

        stage('Clone repository') {
            steps {
                checkout scm
            }
        }

        stage('Install Python packages') {
            steps {
                container("python3-container") {
                   sh script: """
                        pip install --proxy ${proxy_url} --upgrade protobuf
                        pip install --proxy ${proxy_url} -r requirements.txt
                   """
                }
            }
        }

        stage('Pycodestyle check') {
            steps {
                container("python3-container") {
                    sh 'pycodestyle *'
                }
            }
        }

        stage('Maven package') {
            steps {
                container("mvn-container") {
                    configFileProvider([configFile(fileId: mavenSettingsFileId, variable: 'MAVEN_SETTINGS_XML')]) {
                        script {
                            sh "mvn -s ${MAVEN_SETTINGS_XML} package -DskipTests -B -Dchangelist=${changelist} -Dsha1=${classifier}"
                        }
                    }
                }
            }
        }

        stage('Pytest') {
            steps {
                container("python3-container") {
                    sh script: """
                        export PYTHONPATH=%PYTHONPATH%
                        python -m pytest tests \
                            --junitxml=$PytestJUnitFilePath \
                            --durations 10 \
                            --cov . \
                            --cov-config .coveragerc \
                            --cov-report xml:$PytestCovFilePath

                        # Convert PyTest xUnit test reports to valid xUnit format
                        # https://issues.jenkins-ci.org/browse/JENKINS-51920
                        xsltproc pytest-xunit.xsl $PytestJUnitFilePath > ${PytestJUnitFilePath}.tmp
                        rm -f $PytestJUnitFilePath
                        mv ${PytestJUnitFilePath}.tmp $PytestJUnitFilePath
                    """
                }
            }
        }

        stage('Maven deploy') {
            steps {
                container("mvn-container") {
                    configFileProvider([configFile(fileId: mavenSettingsFileId, variable: 'MAVEN_SETTINGS_XML')]) {
                        script {
                            sh "mvn deploy -s ${MAVEN_SETTINGS_XML} -DskipTests -Dchangelist=${changelist} -Dsha1=${classifier} -B -U"

                        }
                    }
                }
            }
        }
    }

    post {
        always {
            xunit thresholds: [failed(failureNewThreshold: '0', failureThreshold: '0', unstableNewThreshold: '0', unstableThreshold: '0')], tools: [JUnit(
                    deleteOutputFiles: true,
                    failIfNotNew: false,
                    pattern: '**/' + PytestJUnitFilePath,
                    skipNoTestFiles: false,
                    stopProcessingIfError: false)]

            cobertura autoUpdateHealth: false,
                    autoUpdateStability: false,
                    classCoverageTargets: '55, 0, 55',
                    coberturaReportFile: '**/' + PytestCovFilePath,
                    conditionalCoverageTargets: '80, 0, 80',
                    lineCoverageTargets: '90, 0, 90',
                    maxNumberOfBuilds: 0,
                    methodCoverageTargets: '80, 0, 80',
                    onlyStable: false,
                    failUnhealthy: true,
                    failUnstable: true,
                    sourceEncoding: 'ASCII',
                    zoomCoverageChart: false
        }
    }
}