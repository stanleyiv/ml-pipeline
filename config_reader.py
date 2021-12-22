import yaml
import os
import time
import json
import subprocess
from enum import Enum

from utils import getFirstOrDefault, open_thread_with_callback

pipeline_config = [ 
    {
        'jobs': [
            {
                '_id': 'ingest_data',
                'title': 'Ingest Data Into Pipeline',
                'command': 'python ./jobs/ingest_data.py',
                'params': {
                    'type': 'DB_MONGO',
                    'DB_MONGO_HOST': 'example.host.com',
                    'DB_MONGO_PORT': '27017',
                    'DB_MONGO_USERNAME': 'root',
                    'DB_MONGO_PASSWORD': 'root'
                }
            },
            {
                '_id': 'preprocess_data',
                'title': 'Perform Data Preprocessing',
                'command': 'python ./jobs/preprocess_data.py'
            },
            {
                '_id': 'train_model',
                'title': 'Perform Model Training',
                'command': 'python ./jobs/train.py',
                'params': {
                    'lossFunc': '',
                    'epochs': '5',
                    'learning_rate': '.001'
                }
            },
            {
                '_id': 'eval_model',
                'title': 'Perform Model Evaluation',
                'command': 'python ./jobs/evaluate.py'
            },
            {
                '_id': 'deploy_model',
                'title': 'Perform Model Deployment',
                'command': 'python ./jobs/deploy.py'
            }
        ]
    },
    {
        'workflow': {
            'jobs': [
                {
                    '_step': 'ingest_data',
                    'type': 'scheduled',
                    'interval': 60
                },
                {
                    '_step': 'preprocess_data',
                    'requires': 'ingest_data'
                },
                {
                    '_step': 'train_model',
                    'requires': 'preprocess_data'
                },                
                {
                    '_step': 'eval_model',
                    'requires': 'train_model'
                },
                {
                    '_step': 'deploy_model',
                    'requires': 'eval_model'
                }
            ]
        }
    }
]



def writeConfig():
    with open(".config.yaml", 'w') as yamlfile:
        data = yaml.dump(pipeline_config, yamlfile)

class TaskStatus(Enum):
    Ready = 1
    Running = 2
    Completed = 3

class Task:
    def __init__(self, id, title, params, command, requires):
        self.status = TaskStatus.Ready
        self.id = id
        self.title = title
        self.params = params
        self.command = command
        self.requires = requires

def main():
    available_tasks = []

    def on_exit(task):
        task.status = TaskStatus.Completed

    with open(".config.yaml", "r") as yamlfile:
        data = yaml.load(yamlfile, Loader=yaml.FullLoader)

    jobs = data[0]['jobs']
    workflow = data[1]['workflow']

    for job in workflow['jobs']:
        wf_step = job['_step']
        wf_requires = job['requires'] if 'requires' in job else None
        job_setup = getFirstOrDefault(jobs, '_id', wf_step)
        if not job_setup:
            continue
        job_id = job_setup['_id']
        job_title = job_setup['title'] if 'title' in job_setup else None
        job_params = job_setup['params'] if 'params' in job_setup else None
        job_command = job_setup['command'] if 'command' in job_setup else None

        available_tasks.append(Task(id=job_id, title=job_title, params=job_params, command=job_command, requires=wf_requires))


    for task in available_tasks:
        if task.status != TaskStatus.Ready:
            continue

        if not task.requires:
            open_thread_with_callback(on_exit, on_exit_args=task, popen_args=[task.command])
            task.status = TaskStatus.Running

if __name__ == '__main__':
    main()