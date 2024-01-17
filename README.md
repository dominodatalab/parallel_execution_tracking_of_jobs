# Demo for using Ray as substitute for Cloud Lambda processing based systems

Domino's On-Demand Ray can be used as substitute for cloud based "Lamba processing" like systems . Combined with Domino's "Experiment Manager", Apache Ray provides a powerful abstraction to run multiple jobs in parallel while tracking these multiple runs a part of a single experiment run.

## Setup
Add your model JAR file to the following location
```
domino_project_name=os.environ['DOMINO_PROJECT_NAME']
root_folder = f'/domino/datasets/local/{domino_project_name}/jar/model.jar'
```
The input files will be placed in the folder
```
/mnt/input/
          /high_r0.yaml
          /large_experiment_1.yaml
          /low_r0.yaml
```
When the worker process is called train_function the yaml is sent to it as a parameter (dict) along with the path to the executable jar. The worker writes the output locally to its /tmp/ folder and writes the output as artifacts to the experiment run. Except for the executable jar no other data location is shared between the workspace and the ray workers. Method to distribute the JAR to ray workers also exist. We chose this for simplicity

## Best Practices for Ray Worker Configuration
Say you want to process a 1000 files (1000 trials). Starting 1000 worker Ray cluster on a small HW Tier is not the best strategy for the following reason - You might need a 1000 nodes. K8s clusters are complex to manage operationally for such a large number of nodes. Typically 100-200 is a reasonable number. More than that, needs finer tuning of your K8s cluster for stability

Instead use a larger HW Tier say 64 cores. And choose 16 workers each using up all of the HW Tier (Ask our PS team how). Inside each worker process 64 files in 64 threads (processes) and return 64 results from each worker. You will achieve a high degree of parallelism (1024) with just 16 nodes added to your Ray cluster.

If you want to use GPU a similar strategy applies. NVIDIA now supports the MIG architecture which allows for GPU sharing. You could divide a single GPU into 56 logical GPU's. This allows for parallelism of upto 56. Ask our PS Team for more details
