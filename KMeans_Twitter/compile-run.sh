#!/bin/bash

THIS_DIR=$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)

main() {
    # check argument
    [[ -n $1 ]] || {
        print_help
        exit 1
    }

    # check sbt
    command -v sbt >/dev/null 2>&1 || {
        echo "sbt command not found. Please make sure you have correctly configured your $HOME/.bash_profile"
        exit 1
    }

    pushd "$THIS_DIR" || exit 1

    # task 1
    if [[ $(basename "$1") == K_means_clustering.scala ]]; then
        # prepare files
        check_file "$1" && echo "code file check ✓"
        check_file "$2"
        checksum_file "$2" "9c2ec3194495349ed87ace97a430b491" && echo "data file check ✓"

        mkdir -p task1/src/main/scala
        cp "$1" task1/src/main/scala
        echo "$CONFIG_TASK1" >task1/build.sbt

        hdfs dfs -mkdir -p assign2/task1

        # compile & run things
        echo "start compiling..."
        pushd task1 || exit 1
        set -eo pipefail
        sbt package

        echo "start running..."
        spark-submit --class Assignment2 --master yarn --deploy-mode client --driver-memory 4g --num-executors=4 --executor-cores=4 --executor-memory 2g target/scala-2.12/task1_2.12-0.1.jar "$SHARED_TASK1_FILE_PATH" | tee "task1_$(date +%F_%T)".log

        set +eo pipefail
        popd || exit 1

    # task 2
    elif [[ $(basename "$1") == Main.scala ]]; then
        # prepare files
        check_file "$1" && echo "code file check ✓"
        check_file "$2"
        checksum_file "$2" "df952449d6f9b5f9fdfe3fc53ddef7ca" && echo "data file check ✓"
        check_file "$3" && echo "stopwords file check ✓"

        mkdir -p task2/src/main/scala
        cp "$1" task2/src/main/scala
        echo "$CONFIG_TASK2" >task2/build.sbt

        hdfs dfs -mkdir -p assign2/task2
        hdfs dfs -put "$3" assign2/task2

        # compile & run things
        echo "start compiling..."
        pushd task2 || exit 1
        set -eo pipefail
        sbt package

        echo "start running..."
        spark-submit --class Main --master yarn --deploy-mode client --driver-memory 4g --num-executors=4 --executor-cores=4 --executor-memory 2g target/scala-2.12/task2_2.12-0.1.jar "$SHARED_TASK2_FILE_PATH" "assign2/task2/$(basename "$3")" | tee "task2_$(date +%F_%T)".log

        set +eo pipefail
        popd || exit 1

    # error
    else
        echo 'Argument file must be "K_means_clustering.scala" or "Main.scala"' >&2
        print_help
        exit 1
    fi

    popd || exit 1
}

check_file() {
    [[ -f $1 ]] || {
        echo "File $1 not found" >&2
        print_help
        exit 1
    }
}

checksum_file(){
    [[ $(md5sum "$1" | cut -d" " -f1) == "$2" ]] || {
        echo "It seems the content of the data file $1 has been changed" >&2
        echo "Please use the original version of the data file. Do not change its content."
        exit 1
    }
}

print_help() {
    echo "Usage"
    echo "├─ For task 1: $0 K_means_clustering.scala <data-file>"
    echo "└─ For task 2: $0 Main.scala <data-file> <stopwords-file>"
}

CONFIG_TASK1='
name := "task1"
version := "0.1"
scalaVersion := "2.12.10"
libraryDependencies ++= Seq(
        "org.apache.spark" %% "spark-sql" % "3.0.0",
)
'

CONFIG_TASK2='
name := "task2"
version := "0.1"
scalaVersion := "2.12.10"
libraryDependencies ++= Seq(
        "org.apache.spark" %% "spark-sql" % "3.0.0",
        "org.apache.spark" %% "spark-mllib" % "3.0.0"
)
'

SHARED_HDFS_DATA_DIR=/shared
SHARED_TASK1_FILE_PATH="$SHARED_HDFS_DATA_DIR/qa.csv"
SHARED_TASK2_FILE_PATH="$SHARED_HDFS_DATA_DIR/twitter.csv"

main "$@"
