pipeline {
    agent any

    /**
     * Build file for EliasDB
     *
     * Each build happens with 2 commits. The first commit is the actual 
     * feature or fix commit. The commit message should follow conventional 
     * commit messages (https://www.conventionalcommits.org/en/v1.0.0-beta.4/).
     * In a second commit a program called standard version 
     * (https://github.com/conventional-changelog/standard-version) calculates
     * a new product version. The versioning will be according to the rules
     * of “Semantic Versioning” (https://semver.org/).
     *
     * Building is done using goreleaser (https://goreleaser.com/) for different
     * platforms.
     *
     * Testing produces code coverage badges which can be embedded on other
     * pages.
     *
     * Everything runs in docker containers to ensure isolation of the build
     * system and to allow painless upgrades.
     */

    stages {
        stage('Commit Analysis') {
            steps {

                // Read the commit message into a variable
                //
                script {
                  commit_msg = sh(returnStdout: true, script: 'git log -1')
                }
            }
        }
        stage('Prepare Release Build') {
            
            // Check for a release build (a commit by standard-version)
            //
            when { expression { return commit_msg =~ /chore\(release\)\:/ } }
            steps {

                // Find out the tagged version
                //
                script {
                  version = sh(returnStdout: true, script: 'git log -1 | grep chore | tr -d "\\n" | sed "s/.*chore(release): \\([0-9\\.]*\\)/\\1/"')
                }

                echo "Building version: ${version} ..."
            }
        }
        stage('Build') {
            when { expression { return commit_msg =~ /chore\(release\)\:/ } }
            steps {
                
                // Fetch all git tags and run goreleaser
                //
                checkout scm
                sshagent (credentials: ['Gogs']) {
                    sh 'git fetch --tags'
                }

                sh 'mkdir -p .cache'
                sh 'docker run --rm --user $(id -u):$(id -g) -v $PWD/.cache:/.cache -v $PWD:/go/code -w /go/code goreleaser/goreleaser --snapshot --skip-publish --rm-dist'
            }
        }
        stage('Test') {

            // The tests are run in both stages - no release commit is made if the tests fail.
            // The output is the coverage data and the badge.
            //
            steps {
                echo 'Running tests ...'

                sh """echo '<svg width="88" height="20" xmlns="http://www.w3.org/2000/svg"><g shape-rendering="crispEdges"><path fill="#555" d="M0 0h41v20H0z"/><path fill="#fc1" d="M41 0h40v20H41z"/></g><g fill="#fff" text-anchor="middle" font-family="DejaVu Sans,Verdana,Geneva,sans-serif" font-size="11"><text x="20.5" y="14">tests</text><text x="60" y="14">fail</text></g></svg>' > test_result.svg"""

                sh 'docker run --rm -e GOPATH=/tmp -v $PWD:/go golang go test -p 1 --coverprofile=coverage.out ./...'
                sh 'docker run --rm -e GOPATH=/tmp -v $PWD:/go golang go tool cover --html=coverage.out -o coverage.html'

                echo 'Determine overall coverage and writing badge'
                script {
                  coverage = sh(returnStdout: true, script: 'docker run --rm -e GOPATH=/tmp -v $PWD:/go golang go tool cover -func=coverage.out | tee coverage.txt | tail -1 | grep -o "[0-9]*.[0-9]*%$" | tr -d "\\n"')
                  
                  echo "Overall coverage is: ${coverage}"
                  
                  if (coverage.equals("100.0%")) {
                    sh """echo '<svg width="110" height="20" xmlns="http://www.w3.org/2000/svg"><g shape-rendering="crispEdges"><path fill="#555" d="M0 0h61v20H0z"/><path fill="#4c1" d="M61 0h50v20H61z"/></g><g fill="#fff" text-anchor="middle" font-family="DejaVu Sans,Verdana,Geneva,sans-serif" font-size="11"><text x="30.5" y="14">coverage</text><text x="85" y="14">$coverage</text></g></svg>' > test_result.svg"""
                  } else {
                    sh """echo '<svg width="110" height="20" xmlns="http://www.w3.org/2000/svg"><g shape-rendering="crispEdges"><path fill="#555" d="M0 0h61v20H0z"/><path fill="#fc1" d="M61 0h50v20H61z"/></g><g fill="#fff" text-anchor="middle" font-family="DejaVu Sans,Verdana,Geneva,sans-serif" font-size="11"><text x="30.5" y="14">coverage</text><text x="85" y="14">$coverage</text></g></svg>' > test_result.svg"""
                  }
                }
            }
        }
        stage('Create Release Build Commit') {
            
            // Check for a non-release build to avoid a commit loop
            //
            when { not { expression { return commit_msg =~ /chore\(release\)\:/ } } }
            steps {

                // Before running standard-version it is important to fetch
                // the existing tags so next version can be calculated
                //
                echo 'Running standard version ...'
                sshagent (credentials: ['Gogs']) {
                    sh 'git fetch --tags'
                }
                sh 'docker run --rm -v $PWD:/app standard-version'
  
                // The new version is inserted into the code
                //
                script {
                  new_version = sh(returnStdout: true, script: 'git tag | tail -1 | tr -d "\\n"')
                }
                echo "Inserting version $new_version into the code"
                sh "find . -name '*.go' -exec sed -i -e 's/ProductVersion\\ =\\ \\\".*\\\"/ProductVersion = \\\"${new_version.substring(1)}\\\"/g' {} \\;"

                // The commit is amended to include the code change
                //
                echo "Tagging the build and push the changes into the origin repository"
                sshagent (credentials: ['Gogs']) {
                    sh 'git config user.name "Matthias Ladkau"'
                    sh 'git config user.email "webmaster@devt.de"'

                    sh 'git commit -a --amend --no-edit'
                    sh "git tag --force $new_version"

                    sh 'git push --tags origin master'
                }
            }
        }
        stage('Upload Release Build Commit') {
            when { expression { return commit_msg =~ /chore\(release\)\:/ } }
            steps {
                echo "Uploading release build ..."

                // After a successful build the resulting artifacts are 
                // uploaded for publication
                //
                sshagent (credentials: ['Gogs']) {
                  
                  // Clear distribution folder
                  sh 'ssh -o StrictHostKeyChecking=no -p 7000 krotik@devt.de rm -fR pub/eliasdb'
                  sh 'ssh -o StrictHostKeyChecking=no -p 7000 krotik@devt.de mkdir -p pub/eliasdb'
                  
                  // Copy distribution packages in place
                  sh 'scp -P 7000 -o StrictHostKeyChecking=no dist/*.tar.gz krotik@devt.de:~/pub/eliasdb'
                  sh 'scp -P 7000 -o StrictHostKeyChecking=no dist/checksums.txt krotik@devt.de:~/pub/eliasdb'

                  // Copy coverage in place
                  sh 'scp -P 7000 -o StrictHostKeyChecking=no coverage.* krotik@devt.de:~/pub/eliasdb'

                  // Copy test result in place
                  sh 'scp -P 7000 -o StrictHostKeyChecking=no test_result.svg krotik@devt.de:~/pub/eliasdb'
                }
            }
        }
    }
}
