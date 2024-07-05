# Cloud Service Failure Prediction on Google’s Borg Cluster Traces Using Traditional Machine Learning

This application represents the work of my master's thesis, and it is a research focused on analyzing the workload of an industrial set of clusters, provided as traces in Google’s Borg cluster workload traces (performed on the latest and most complete version: 2019 version).
The aim was to develop highly accurate predictive models for both job and task failures, a goal which was achieved. A job classifier having a performance of 83.97% accuracy (Gradient Boosting) and a task classifier of 98.79% accuracy performance (Decision Tree) were obtained.

A description of the data from the cluster traces used in the research can be found [here](https://drive.google.com/file/d/10r6cnJ5cJ89fPWCgj7j4LtLBqYN9RiI9/view), together with information on how to retrieve this data.

A research paper based on this thesis was made by myself and by my supervising teacher Adrian Spătaru, which was presented at "2023 25th International Symposium on Symbolic and Numeric Algorithms for Scientific Computing (SYNASC)", and was afterward [published](https://doi.org/10.1109/SYNASC61333.2023.00029) in IEEE Xplore.

## Methodology

### Preprocessing

The research started with an analysis to decide what features should be considered when building the ML models. Therefore, a preprocessing step was performed, starting by selecting only the jobs and tasks (without the alloc set and instances), and by removing the following: entries having transient states (e.g. Queued, Evicted), irrelevant fields, entries containing missing information, entries that started or ended outside the trace period, and task entries having a job parent that started before the trace period.

Other data reduction steps were afterward performed.

- The job parent-child dependency information (that formed a Directed Acyclic Graph) was taken into consideration by removing the jobs that terminated in FAILURE because their parent terminated unsuccessfully. That is, all job children that finished after the end of their parents, and parents that ended in failure, should be filtered out.
  
- A class balancing step was performed on the training data. For jobs, having a small difference between the number of successes and failures, an oversampling method was applied: synthetic minority sampling technique (SMOTE). For the tasks the difference between successes and failures was much greater, therefore random undersampling was used to reduce the majority class, getting to a closer ratio of almost 1:1.

After performing all these steps, the newly obtained datasets were divided into two parts and saved as Parquet files, in order to prepare them for the ML part: a 75% dataset for the training part, and a 25% dataset for the testing part.

### Model Generation and Evaluation

For the model generation phase, four supervised machine learning classification algorithms were implemented: Decision Tree, Random Forest, Gradient Boosting, and Logistic Regression.

Data that has already been preprocessed and split into train and test samples is read from disk. Each algorithm is applied on the same training sample, and evaluated using the same test sample. The measurements of interest are accuracy, recall by label, F1-score, AUC-ROC, and training time.

### Experimental Results

For jobs, the best overall performance was for **Gradient Boosting:** *Acc: 83.97%, Recall: 76.45%, F1: 84.04%, AUC: 85.33%, Tr. Time: 17 sec.*

For tasks, the best overall performance was for **Decision Tree:** *Acc: 98.79%, Recall: 98.80%, F1: 98.85%, AUC: 98.72%, Tr. Time: 1 min 24 sec.*

An additional step of cross-validation was performed on the task's Decision Tree classifier. The purpose of this step was to check if the task prediction model was in a case of overfitting, considering the general result of approximately 98% accuracy. The validation used three folds, and the outcome was impressively good, obtaining a prediction accuracy of 99.32%, confirming there was no overfitting of the data.

Finally, a feature importance analysis was performed, with the following results: for jobs failure prediction, memory request was the most relevant feature for almost all classifiers, except for the LR, where CPU request was more important. For task failure prediction, memory request was the most relevant feature for all of the four classifiers.
