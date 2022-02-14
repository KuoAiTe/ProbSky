# ProbSky
This is an implementation of “ProbSky: Efficient Computation of Probabilistic Skyline Queries over Distributed Data.”
# Dataset
#### TripAdvisor Dataset
Please find the dataset at https://github.com/Diego999/HotelRec/blob/master/README.md.
#### Synthetic dataset (It may take 5-10 minutes)
    # Create a synthetic dataset (correlated, independent, anti-correlated).
    python3 data/generator/synthetic.py

# Citation
Please cite our paper if you find it helpful, thanks!
```
Ai-Te Kuo, Haiquan Chen, Liang Tang, Wei-Shinn Ku, and Xiao Qin, “ProbSky: Efficient Computation of Probabilistic Skyline Queries over Distributed Data,” IEEE Transactions on Knowledge and Data Engineering (TKDE), in press, 2022.
```



# How to run (macOS)

### Install sbt
    curl -s "https://get.sdkman.io" | bash
    source "$HOME/.sdkman/bin/sdkman-init.sh"
    sdk install sbt

# Run ProbSky using sbt
    sbt
    run
    Enter 2, 1 is for spark-submit only.
    # Change src/main/scala/ExperimentApp.scala for parameters

# Run ProbSky using spark-submit

### Step 1: Install Brew
    ruby -e "$(curl -fsSL https://raw.githubusercontent.com/Homebrew/install/master/install)"
    git -C "/usr/local/Homebrew/Library/Taps/homebrew/homebrew-core" fetch --unshallow
    brew update && brew upgrade
    brew install --cask homebrew/cask-versions/adoptopenjdk8

### Step 2: Install  Spark
    brew install apache-spark

### Step 3: Install
    brew install scala

### Step 4: Install python3 package
    python3 -m pip install pyyaml

### Step 5: Run in local mode
    python3 run.py -l

# Deploy on a AWS cluster
### Step 1: Install package dependencies
    sudo yum install python3 -y
    sudo python3 -m pip install pyyaml
    sudo yum -y install tmux
    echo 'set -g mode-mouse on' >> ~/.tmux.conf
    sudo yum install git-core -y
### Step 2: Install sbt package on AWS machines (optional)
    curl https://bintray.com/sbt/rpm/rpm | sudo tee /etc/yum.repos.d/bintray-sbt-rpm.repo
    sudo yum install sbt -y
### Step 3: Clone ProbSky
    git clone https://github.com/KuoAiTe/ProbSky
### Step 4: Configure
    # Configure Lines 413, 421, 424-428
    vim +411 config/configs.yaml
### Step 5: Run
    tmux new-session -d -s probsky 'python3 run.py'
    # Attach to ProbSky session
    tmux -t probsky
    # Deattach the session
    Press ctrl + b and press d


# Launching Apache Spark clusters on AWS machines
Get the latest release of Flintrock by using pip (See https://github.com/nchammas/flintrock)

    pip3 install flintrock

Creating a new AWS User for Flintrock

chmod 400 YOUR_PEM_FILE
flintrock configure
### Launch command
    flintrock launch test-cluster \
        --num-slaves 8 \
        --spark-version 3.0.1 \
        --ec2-key-name ProbSky \
        --ec2-identity-file YOUR_PEM_FILE \
        --ec2-ami ami-007a607c4abd192db \
        --ec2-user ec2-user

## Login command
    flintrock login test-cluster --ec2-identity-file YOUR_PEM_FILE
    export AWS_ACCESS_KEY_ID=YOUR_ACCESS_KEY_ID
    export AWS_SECRET_ACCESS_KEY=YOUR_SECRET_ACCESS_KEY

## Add or remove slaves
    flintrock add-slaves test-cluster --num-slaves 1
    flintrock remove-slaves test-cluster --num-slaves 1

## Check your Spark master address on an AWS virtual machine
    vim /spark/logs/s*

## Destroy the cluster
    flintrock destroy test-cluster

Should you have any question, please contact me at robinsa87@gmail.com.
