# Cloud Service Failure Prediction on Google’s Borg Cluster Traces Using Traditional Machine Learning

<!-- The project currently has a folder dedicated to data preprocessing, and another one to machine learning (where only data balancing has been performed so far). --->

This application represents the work of my master's thesis, and it is a research focused on analysing the workload of an industrial set of clusters, provided as traces in the Google’s Borg cluster workload traces.
The aim was to to develop highly accurate predictive models for both job and task failures, goal which was achieved. A job classifier having a performance of 83.97% accuracy (Gradient Boosting) and a task classifier of 98.79% accuracy performance (Decision Tree) were obtained.

A description of the data from the cluster traces can be found [here](https://drive.google.com/file/d/10r6cnJ5cJ89fPWCgj7j4LtLBqYN9RiI9/view), together with information on how to retrieve this data.

A research paper based on this thesis was presented at "2023 25th International Symposium on Symbolic and Numeric Algorithms for Scientific Computing (SYNASC)", and was afterwards [published](https://doi.org/10.1109/SYNASC61333.2023.00029) in IEEE Xplore.
