#!/bin/bash

# Sync changes to/from stack
unstable_stack="ubuntu@192.168.0.1"
lib_path="/mnt/deploy/polaris-storage/.venv-storage/lib/python2.7/site-packages"
backup="backup"
billiard="billiard"
app="celery/app"
concurrency="celery/concurrency"
processes="celery/concurrency/processes"
worker="celery/worker"

logged_c="celery"
logged_b="billiard"

backup()
{
    if ! [ -d "backup" ]
    then
        set -x o
        echo "create backup dir"
        mkdir -p "backup/billiard" 
        mkdir -p "backup/celery/app"
        mkdir -p "backup/celery/concurrency/processes/"
        mkdir -p "backup/celery/worker"

        scp $unstable_stack":"$lib_path"/"$billiard"/pool.py" $backup"/"$billiard"/"
        scp $unstable_stack":"$lib_path"/"$app"/amqp.py" $backup"/"$app"/"
        scp $unstable_stack":"$lib_path"/"$app"/base.py" $backup"/"$app"/"
        scp $unstable_stack":"$lib_path"/"$app"/task.py" $backup"/"$app"/"
        scp $unstable_stack":"$lib_path"/"$processes"/__init__.py" $backup"/"$processes"/"
        scp $unstable_stack":"$lib_path"/"$concurrency"/base.py" $backup"/"$concurrency"/"
        scp $unstable_stack":"$lib_path"/"$worker"/consumer.py" $backup"/"$worker"/"
        scp $unstable_stack":"$lib_path"/"$worker"/strategy.py" $backup"/"$worker"/"
        scp $unstable_stack":"$lib_path"/"$worker"/job.py" $backup"/"$worker"/"

        echo "Done!"
    else
        echo "already backuped!"
    fi
}

upload()
{
    set -x o
    echo "upload logged source files"
    scp $logged_b"/pool.py" $unstable_stack":"$lib_path"/"$billiard
    scp $logged_c"/app/amqp.py" $unstable_stack":"$lib_path"/"$app"/"
    scp $logged_c"/app/base.py" $unstable_stack":"$lib_path"/"$app"/"
    scp $logged_c"/app/task.py" $unstable_stack":"$lib_path"/"$app"/"
    scp $logged_c"/worker/consumer.py" $unstable_stack":"$lib_path"/"$worker"/"
    scp $logged_c"/worker/strategy.py" $unstable_stack":"$lib_path"/"$worker"/"
    scp $logged_c"/worker/job.py" $unstable_stack":"$lib_path"/"$worker"/"
    scp $logged_c"/concurrency/base.py" $unstable_stack":"$lib_path"/"$concurrency"/"
    scp $logged_c"/concurrency/processes/__init__.py" $unstable_stack":"$lib_path"/"$processes"/"
    echo "Done!"
}

restore()
{
    echo "restore backup files"
    scp $backup"/"$billiard"/pool.py" $unstable_stack":"$lib_path"/"$billiard
    scp $backup"/app/amqp.py" $unstable_stack":"$lib_path"/"$app"/"
    scp $backup"/app/base.py" $unstable_stack":"$lib_path"/"$app"/"
    scp $backup"/app/task.py" $unstable_stack":"$lib_path"/"$app"/"
    scp $backup"/worker/consumer.py" $unstable_stack":"$lib_path"/"$worker"/"
    scp $backup"/worker/strategy.py" $unstable_stack":"$lib_path"/"$worker"/"
    scp $backup"/concurrency/base.py" $unstable_stack":"$lib_path"/"$concurrency"/"
    scp $backup"/concurrency/processes/__init__.py" $unstable_stack":"$lib_path"/"$processes"/"
    echo "Done!"
}

download()
{
    if ! [ -d "task_log" ]
    then
        mkdir "task_log"
    fi
    if [ -f "task_log/"$1 ]
    then
        mv "task_log/"$1 "task_log/"$1.`md5sum task.log`
    fi
    echo "Download $1..."
    scp $unstable_stack:"/var/log/celery/$1" "task_log/"
    echo "Done!"
}

if [[ $1 == "download" ]]
then
    $1 $2
else
    $1
fi
