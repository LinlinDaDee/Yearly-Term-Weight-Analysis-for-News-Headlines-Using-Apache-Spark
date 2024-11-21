# Yearly-Term-Weight-Analysis-for-News-Headlines-Using-Apache-Spark
This project computes yearly average term weights in a dataset of Australian news headlines using TF-IDF, identifying the most significant terms over time. It demonstrates efficient big data processing techniques with Apache Spark, including implementations using both RDD and DataFrame APIs.
## Features
- Calculates term weights using TF-IDF.
- Identifies top-k terms by yearly average weight while filtering out highly frequent terms.
- Implements solutions using both RDD and DataFrame APIs in Apache Spark.
- Processes large-scale datasets efficiently without relying on external libraries like NumPy or Pandas.
## Background
Detecting popular and trending topics from news articles is essential for monitoring public opinion and understanding societal trends. This project analyzes textual data from a dataset of Australian news headlines using Apache Spark, focusing on computing yearly average term weights and identifying the most important terms.
## Steps
1. Preprocessing:
   - Parse the dataset to extract terms and group them by year.
   - Remove terms that appear in the top-n most frequent global terms.
2. Term Weight Calculation:
    - Term Frequency (TF) : measures how often a term appears in a given year.
      TF(t, y) = \log_{10}(\text{frequency of } t \text{ in } y)
    - Inverse Document Frequency (IDF) : accounts for how unique or significant a term is by considering how many headlines contain the term in that year.
      IDF(t, y) = \log_{10}\left(\frac{\text{total headlines in } y}{\text{headlines in } y \text{ containing } t}\right)
    - Term Weight: combines these two metrics to reflect the importance of a term in a specific year.
      Weight(t, y) = TF(t, y) \times IDF(t, y)
3. Yearly Average Calculation:
   - Calculate the yearly average weight for each term by dividing the total weight of the term by the number of years it appears.
4. Sorting and Filtering:
   - Sort terms by their average weights in descending order.
   - Resolve ties by sorting alphabetically.
   - Output the top-k terms.
5. 	Run the Project
   - Parameters
     - Input File Path: The path to the input dataset.
     - Output Folder Path: The folder where the result file will be saved.
     - n: The number of top frequent terms to filter out.
     - k: The number of terms to output in the result.
   - Example Command
     - RDD Implementation:
       ```bash
       spark-submit rdd.py "file:///path/to/testcase.txt" "file:///path/to/output" <n> <k>
       ```
     - DataFrame Implementation
       ```bash
       spark-submit df.py "file:///path/to/testcase.txt" "file:///path/to/output" <n> <k>
       ```
